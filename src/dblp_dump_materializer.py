"""Materialize DBLP TOC JSON files from the local DBLP XML dump.

The live DBLP TOC API is useful for small updates, but it is rate-limited and
can become unstable for very large venues.  This module builds the same JSON
shape expected by the consolidator from a local ``dblp.xml`` or
``dblp.xml.gz`` snapshot, so corpus refreshes can run offline and deterministically.
"""

from __future__ import annotations

import csv
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .bibtex_dump import _EntitySubstitutingStream, _load_entity_table, _open_dump
from .models import Configuration, DownloadLogEntry, DownloadStatus
from .venue_config import VenueStrategyRegistry

_DBLP_PREFIX = "https://dblp.org/"
_CONF_PAGE_RE = re.compile(r"^db/conf/(?P<stream>[^/]+)/(?P<slug>[^/]+)\.html$")
_YEAR_RE = re.compile(r"(?P<year>\d{4}(?:-\d+)?)")


@dataclass
class DumpTarget:
    event: str
    year: int
    file_path: Path
    page_paths: set[str]
    crossrefs: set[str]
    hits: list[dict] = field(default_factory=list)


class DblpDumpMaterializer:
    """Generate per-event/year DBLP JSON files from a local XML dump."""

    def __init__(
        self,
        config: Configuration,
        json_dir: Path,
        log_dir: Path,
        dump_path: Path,
    ) -> None:
        self.config = config
        self.json_dir = Path(json_dir)
        self.log_dir = Path(log_dir)
        self.dump_path = Path(dump_path)
        self.venue_registry = VenueStrategyRegistry()

    def materialize(
        self,
        events: list[str] | None = None,
        years: list[int] | None = None,
        overwrite: bool = False,
    ) -> list[DownloadLogEntry]:
        """Materialize JSON files for the selected events and years."""
        if not self.dump_path.exists():
            raise FileNotFoundError(f"DBLP dump not found at {self.dump_path}")

        self.json_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        selected_events = events or list(self.config.events)
        selected_years = years or self.config.effective_years()
        targets, initial_log = self._build_targets(selected_events, selected_years, overwrite)
        if targets:
            self._scan_dump(targets)

        log_entries = [*initial_log]
        timestamp = datetime.now()
        for target in targets:
            log_entries.append(self._write_target(target, timestamp))
        self._save_log(log_entries)
        return log_entries

    def _build_targets(
        self,
        events: list[str],
        years: list[int],
        overwrite: bool,
    ) -> tuple[list[DumpTarget], list[DownloadLogEntry]]:
        targets: list[DumpTarget] = []
        log_entries: list[DownloadLogEntry] = []
        timestamp = datetime.now()

        for event in events:
            for year in years:
                file_path = self.json_dir / f"data_{event}{year}.json"
                if file_path.exists() and not overwrite and _json_has_hits(file_path):
                    log_entries.append(
                        DownloadLogEntry(
                            event=event,
                            year=year,
                            file_name=str(file_path),
                            url="local-dump",
                            status=DownloadStatus.VALID,
                            message="Valid file exists",
                            timestamp=timestamp,
                        )
                    )
                    continue

                page_paths = self._page_paths_for(event, year)
                if not page_paths:
                    log_entries.append(
                        DownloadLogEntry(
                            event=event,
                            year=year,
                            file_name=str(file_path),
                            url="local-dump",
                            status=DownloadStatus.SKIPPED,
                            message="No DBLP paths configured",
                            timestamp=timestamp,
                        )
                    )
                    continue

                targets.append(
                    DumpTarget(
                        event=event,
                        year=year,
                        file_path=file_path,
                        page_paths=page_paths,
                        crossrefs={_crossref_from_page_path(path) for path in page_paths},
                    )
                )
        return targets, log_entries

    def _page_paths_for(self, event: str, year: int) -> set[str]:
        urls = self.venue_registry.get_strategy(event).get_urls(event, year, self.config)
        if event == "esorics":
            urls.extend(
                f"https://dblp.org/db/conf/esorics/esorics{year}-{part}.html"
                for part in range(1, 5)
            )
        elif event == "aisec":
            urls.append(f"https://dblp.org/db/conf/ccs/aisec{year}.html")
        elif event == "hotmobile":
            urls.append(f"https://dblp.org/db/conf/wmcsa/hotmobile{year}.html")
        elif event == "kdd":
            urls.extend(
                f"https://dblp.org/db/conf/kdd/kdd{year}-{part}.html" for part in range(1, 5)
            )
        elif event in {"acl", "emnlp", "naacl"}:
            urls.extend(
                f"https://dblp.org/db/conf/{event}/{event}{year}-{part}.html"
                for part in range(1, 8)
            )
            urls.extend(
                f"https://dblp.org/db/conf/{event}/{event}{year}{suffix}.html"
                for suffix in ("f", "s", "d", "i")
            )
        return {
            url.removeprefix(_DBLP_PREFIX)
            for url in urls
            if url.startswith(_DBLP_PREFIX) and url.endswith(".html")
        }

    def _scan_dump(self, targets: list[DumpTarget]) -> None:
        page_index: dict[str, list[DumpTarget]] = {}
        crossref_index: dict[str, list[DumpTarget]] = {}
        for target in targets:
            for path in target.page_paths:
                page_index.setdefault(path, []).append(target)
            for crossref in target.crossrefs:
                crossref_index.setdefault(crossref, []).append(target)

        dtd_path = self.dump_path.parent / "dblp.dtd"
        entity_table = _load_entity_table(dtd_path)
        parser = ET.XMLPullParser(events=("end",))
        with _open_dump(self.dump_path) as raw:
            stream = _EntitySubstitutingStream(raw, entity_table)
            while True:
                chunk = stream.read(1 << 16)
                if not chunk:
                    parser.close()
                    break
                parser.feed(chunk)
                for _, elem in parser.read_events():
                    if elem.tag not in {"article", "inproceedings", "proceedings"}:
                        continue
                    matched_targets = self._targets_for_entry(elem, page_index, crossref_index)
                    if matched_targets:
                        hit = _entry_to_hit(elem)
                        for target in matched_targets:
                            target.hits.append(hit)
                    elem.clear()

    @staticmethod
    def _targets_for_entry(
        elem: ET.Element,
        page_index: dict[str, list[DumpTarget]],
        crossref_index: dict[str, list[DumpTarget]],
    ) -> list[DumpTarget]:
        matched: dict[tuple[str, int], DumpTarget] = {}
        for child in elem:
            if child.tag == "url" and child.text:
                page_path = child.text.split("#", 1)[0]
                for target in page_index.get(page_path, []):
                    matched[(target.event, target.year)] = target
            elif child.tag == "crossref" and child.text:
                for target in crossref_index.get(child.text, []):
                    matched[(target.event, target.year)] = target
        return list(matched.values())

    def _write_target(self, target: DumpTarget, timestamp: datetime) -> DownloadLogEntry:
        target.hits.sort(key=lambda hit: str(hit.get("info", {}).get("key", "")))
        payload = {
            "result": {
                "hits": {
                    "@total": str(len(target.hits)),
                    "@computed": str(len(target.hits)),
                    "@sent": str(len(target.hits)),
                    "@first": "0",
                    "hit": target.hits,
                }
            }
        }
        if not target.hits:
            return DownloadLogEntry(
                event=target.event,
                year=target.year,
                file_name=str(target.file_path),
                url="local-dump",
                status=DownloadStatus.FAILED,
                message="No DBLP records found in local dump",
                timestamp=timestamp,
            )

        target.file_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return DownloadLogEntry(
            event=target.event,
            year=target.year,
            file_name=str(target.file_path),
            url="local-dump",
            http_code=200,
            status=DownloadStatus.DOWNLOADED,
            message=f"Materialized {len(target.hits)} DBLP records from local dump",
            timestamp=timestamp,
        )

    def _save_log(self, entries: list[DownloadLogEntry]) -> None:
        log_file = self.log_dir / "download_log.csv"
        fieldnames = ["Event", "Year", "File", "URL", "HTTP_Code", "Status", "Message", "Timestamp"]
        with log_file.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for entry in entries:
                writer.writerow(
                    {
                        "Event": entry.event,
                        "Year": entry.year,
                        "File": entry.file_name,
                        "URL": entry.url,
                        "HTTP_Code": entry.http_code or "",
                        "Status": entry.status.value,
                        "Message": entry.message or "",
                        "Timestamp": entry.timestamp.isoformat(),
                    }
                )


def _json_has_hits(file_path: Path) -> bool:
    data = json.loads(file_path.read_text(encoding="utf-8"))
    hits = data.get("result", {}).get("hits", {}).get("hit", [])
    return bool(hits)


def _crossref_from_page_path(page_path: str) -> str:
    match = _CONF_PAGE_RE.match(page_path)
    if not match:
        return ""
    year_match = _YEAR_RE.search(match.group("slug"))
    if not year_match:
        return ""
    return f"conf/{match.group('stream')}/{year_match.group('year')}"


def _entry_to_hit(elem: ET.Element) -> dict:
    key = elem.get("key") or ""
    info: dict[str, object] = {
        "authors": {"author": []},
        "type": _api_type_for_tag(elem.tag),
        "key": key,
        "url": f"https://dblp.org/rec/{key}" if key else None,
    }
    ee_values: list[str] = []

    for child in elem:
        text = _element_text(child)
        if not text:
            continue
        if child.tag == "author" or child.tag == "editor":
            author = {"text": text}
            if child.get("pid"):
                author["@pid"] = child.get("pid")
            info["authors"]["author"].append(author)
        elif child.tag == "booktitle" or child.tag == "journal":
            info["venue"] = text
        elif child.tag == "ee":
            ee_values.append(text)
        elif child.tag == "url":
            info["toc_url"] = text
        elif child.tag in {"title", "pages", "year", "doi"}:
            info[child.tag] = text

    if ee_values:
        info["ee"] = _preferred_ee(ee_values)
    if "doi" not in info and info.get("ee", "").startswith("https://doi.org/"):
        info["doi"] = str(info["ee"]).removeprefix("https://doi.org/")
    info["access"] = "closed"

    if isinstance(info["authors"]["author"], list) and len(info["authors"]["author"]) == 1:
        info["authors"]["author"] = info["authors"]["author"][0]

    return {
        "@score": "1",
        "@id": key,
        "info": {key_: value for key_, value in info.items() if value not in (None, [], {})},
        "url": "URL#3995011" if not key else f"URL#{key}",
    }


def _preferred_ee(ee_values: list[str]) -> str:
    for value in ee_values:
        if "doi.org/" in value:
            return value
    return ee_values[0]


def _element_text(element) -> str:
    """Full text of an element, including the text after any child tag.

    DBLP marks up titles with ``<sup>`` and ``<sub>`` for names such as
    ``D<sup>3</sup>FL``. ``element.text`` stops at the first child, so reading it
    directly truncated those titles to their first character. ``itertext``
    walks the whole subtree; superscripts are inlined with no surrounding space,
    which is how DBLP renders them (``D3FL``).
    """
    if element is None:
        return ""
    if len(element) == 0:
        return (element.text or "").strip()
    parts: list[str] = [element.text or ""]
    for child in element:
        inner = "".join(child.itertext())
        if child.tag in {"sup", "sub", "inf"}:
            # Attach the superscript to the token it modifies, but keep the
            # tail's own spacing: DBLP already separates "SPHINCS+ Signature"
            # from "D3FL:" correctly, and stripping the tail merged real words.
            parts[-1] = parts[-1].rstrip()
            parts.append(inner.strip())
            parts.append(child.tail or "")
        else:
            parts.append(inner)
            parts.append(child.tail or "")
    return re.sub(r"\s+", " ", "".join(parts)).strip()


def _api_type_for_tag(tag: str) -> str:
    if tag == "proceedings":
        return "Editorship"
    if tag == "inproceedings":
        return "Conference and Workshop Papers"
    return "article"
