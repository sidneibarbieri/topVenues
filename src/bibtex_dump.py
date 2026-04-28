"""Build BibTeX entries from the DBLP XML dump.

The DBLP project publishes the full bibliography as a single, gzipped XML
file at https://dblp.org/xml/dblp.xml.gz, refreshed daily. Combined with the
DTD that defines entity expansions (``&Auml;`` and friends) we get a
self-contained source for every BibTeX entry DBLP serves through its API —
without ever touching the rate-limited per-record endpoint.

Pipeline::

    download_dump(target_dir)            # ~600 MB .gz + ~14 KB .dtd
    parse_dump_for_keys(xml_path, keys)  # streaming, returns {key: bibtex}

The streaming parser keeps memory bounded (~150 MB peak) by clearing
elements after processing and indexing only the proceedings records needed
to resolve ``<crossref>`` references on inproceedings entries.
"""

from __future__ import annotations

import gzip
import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

DBLP_DUMP_URL = "https://dblp.org/xml/dblp.xml.gz"
DBLP_DTD_URL = "https://dblp.org/xml/dblp.dtd"

# Entry tags DBLP uses. ``proceedings`` are indexed only as crossref targets.
INPROCEEDINGS_TAGS = {
    "article",
    "inproceedings",
    "incollection",
    "book",
    "phdthesis",
    "mastersthesis",
}
PROCEEDINGS_TAG = "proceedings"
ALL_TAGS = INPROCEEDINGS_TAGS | {PROCEEDINGS_TAG}

# Fields inherited from a parent ``proceedings`` entry when an inproceedings
# uses ``<crossref>``. DBLP's API does the same when called with ``?param=1``.
INHERITED_FIELDS = (
    "editor", "booktitle", "publisher", "series", "volume", "address", "isbn",
)

# BibTeX field order matches the DBLP API output for visual diff parity.
FIELD_ORDER = (
    "author", "editor", "title", "booktitle", "journal",
    "volume", "number", "pages", "year",
    "publisher", "series", "address", "isbn",
    "url", "doi",
    "biburl", "bibsource",
)

_FIELD_PADDING = 13   # ``author       = `` style alignment
_AUTHOR_INDENT = " " * 18  # column where each ``and``-joined name starts


def download_dump(target_dir: Path, force: bool = False) -> tuple[Path, Path]:
    """Download ``dblp.xml.gz`` and ``dblp.dtd`` into ``target_dir``.

    Returns the local paths to (xml, dtd). Skips files that already exist
    unless ``force`` is set.
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    xml_path = target_dir / "dblp.xml.gz"
    dtd_path = target_dir / "dblp.dtd"

    if not force and xml_path.exists() and dtd_path.exists():
        return xml_path, dtd_path

    with httpx.Client(timeout=httpx.Timeout(None), follow_redirects=True) as client:
        if force or not dtd_path.exists():
            logger.info("Fetching %s", DBLP_DTD_URL)
            dtd_path.write_bytes(client.get(DBLP_DTD_URL).raise_for_status().content)

        if force or not xml_path.exists():
            logger.info("Fetching %s", DBLP_DUMP_URL)
            with client.stream("GET", DBLP_DUMP_URL) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", 0))
                report_every = max(8 << 20, total // 50)  # at most ~50 prints
                next_threshold = report_every
                with xml_path.open("wb") as fh:
                    received = 0
                    for chunk in resp.iter_bytes(chunk_size=1 << 16):
                        fh.write(chunk)
                        received += len(chunk)
                        if received >= next_threshold or received == total:
                            pct = (received / total * 100) if total else 0
                            print(f"  download… {received >> 20:>5} / "
                                  f"{(total or received) >> 20} MiB ({pct:5.1f}%)",
                                  flush=True)
                            next_threshold += report_every

    return xml_path, dtd_path


def parse_dump_for_keys(
    xml_path: Path,
    target_keys: Iterable[str],
    dtd_path: Path | None = None,
    progress_every: int = 200_000,
) -> dict[str, str]:
    """Stream-parse the DBLP XML, returning ``{key: bibtex}`` for every match.

    Works directly on the gzipped dump. The DBLP DTD declares hundreds of
    named entities (``&uuml;``, ``&Auml;`` …) that Python's stdlib parser
    refuses to load externally; the helper expands them on the fly to
    numeric character references the parser already understands.
    """
    xml_path = Path(xml_path)
    dtd_path = Path(dtd_path) if dtd_path else xml_path.parent / "dblp.dtd"
    targets = set(target_keys)
    if not targets:
        return {}

    entity_table = _load_entity_table(dtd_path)
    logger.info("Streaming %s with %d named entities", xml_path, len(entity_table))

    proceedings_index: dict[str, dict[str, object]] = {}
    matched: dict[str, tuple[str, dict[str, object]]] = {}
    seen = 0

    parser = ET.XMLPullParser(events=("end",))
    with _open_dump(xml_path) as raw:
        substituting = _EntitySubstitutingStream(raw, entity_table)
        while True:
            chunk = substituting.read(1 << 16)
            if not chunk:
                parser.close()
                break
            parser.feed(chunk)
            for _, elem in parser.read_events():
                tag = elem.tag
                if tag not in ALL_TAGS:
                    # Leave nested children (``<author>``, ``<title>``…) alone:
                    # clearing them here would wipe their text *before* the
                    # parent entry's end event arrives, leaving us with empty
                    # extractions. They are released when the parent clears.
                    continue
                seen += 1
                key = elem.get("key")
                if key:
                    if tag == PROCEEDINGS_TAG:
                        proceedings_index[key] = _extract_fields(elem)
                    elif key in targets:
                        matched[key] = (tag, _extract_fields(elem))
                if progress_every and seen % progress_every == 0:
                    print(f"  parse… {seen:>10,} entries seen, "
                          f"{len(matched):>5,}/{len(targets):,} matched", flush=True)
                elem.clear()

    return _resolve_crossrefs_and_format(matched, proceedings_index)


def _open_dump(xml_path: Path):
    if xml_path.suffix == ".gz":
        return gzip.open(xml_path, "rb")
    return xml_path.open("rb")


_DTD_ENTITY_RE = re.compile(rb'<!ENTITY\s+(\w+)\s+"([^"]*)"\s*>')


def _load_entity_table(dtd_path: Path) -> dict[bytes, bytes]:
    """Parse ``<!ENTITY name "value">`` declarations from the DBLP DTD."""
    if not dtd_path.exists():
        raise FileNotFoundError(f"DBLP DTD not found at {dtd_path}")
    return {
        match.group(1): match.group(2)
        for match in _DTD_ENTITY_RE.finditer(dtd_path.read_bytes())
    }


class _EntitySubstitutingStream:
    """File-like wrapper that rewrites ``&named;`` entities into ``&#NNN;``.

    Python's stdlib parser handles numeric character references natively
    but refuses to follow an external DTD without security trade-offs, so
    we materialise the substitution on the byte stream just before it
    reaches the parser. A tail buffer guarantees an entity straddling a
    chunk boundary is never split.
    """

    _CARRY = 64
    _PATTERN = re.compile(rb"&(\w+);")

    def __init__(self, source, table: dict[bytes, bytes]) -> None:
        self._source = source
        self._table = table
        self._buf = b""
        self._eof = False

    def read(self, size: int = -1) -> bytes:
        target = size if size > 0 else 1 << 16
        while not self._eof and len(self._buf) < target + self._CARRY:
            chunk = self._source.read(1 << 16)
            if not chunk:
                self._eof = True
                break
            self._buf += chunk

        cut = len(self._buf) if self._eof else len(self._buf) - self._CARRY
        if not self._eof:
            # Never cut inside a ``&NAME;`` reference: if the last ``&`` before
            # the cut isn't followed by ``;`` within the cut window, move the
            # cut to just before that ``&`` so the entity stays whole.
            last_amp = self._buf.rfind(b"&", 0, cut)
            if last_amp >= 0 and self._buf.find(b";", last_amp, cut) == -1:
                cut = last_amp

        head = self._PATTERN.sub(self._substitute, self._buf[:cut])
        self._buf = self._buf[cut:]

        if size > 0 and len(head) > size:
            self._buf = head[size:] + self._buf
            head = head[:size]
        return head

    def _substitute(self, match: re.Match) -> bytes:
        name = match.group(1)
        if name in (b"amp", b"lt", b"gt", b"quot", b"apos"):
            return match.group(0)
        return self._table.get(name, match.group(0))


# ── Internals ──────────────────────────────────────────────────────────────


def _extract_fields(elem: ET.Element) -> dict[str, object]:
    """Pull BibTeX-relevant fields out of a single XML entry."""
    fields: dict[str, object] = {}
    authors: list[str] = []
    editors: list[str] = []
    eee: list[str] = []
    crossrefs: list[str] = []

    for child in elem:
        text = (child.text or "").strip()
        if not text:
            continue
        if child.tag == "author":
            authors.append(text)
        elif child.tag == "editor":
            editors.append(text)
        elif child.tag == "ee":
            eee.append(text)
        elif child.tag == "crossref":
            crossrefs.append(text)
        else:
            fields[child.tag] = text

    if authors:
        fields["author"] = authors
    if editors:
        fields["editor"] = editors
    if crossrefs:
        fields["__crossref"] = crossrefs[0]

    if eee:
        for url in eee:
            if "doi.org/" in url:
                fields["doi"] = url.split("doi.org/", 1)[1]
                fields["url"] = url
                break
        if "url" not in fields:
            fields["url"] = eee[0]

    return fields


def _resolve_crossrefs_and_format(
    matched: dict[str, tuple[str, dict[str, object]]],
    proceedings_index: dict[str, dict[str, object]],
) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, (tag, fields) in matched.items():
        crossref_key = fields.pop("__crossref", None)
        if crossref_key and crossref_key in proceedings_index:
            parent = proceedings_index[crossref_key]
            for fname in INHERITED_FIELDS:
                if fname not in fields and fname in parent:
                    fields[fname] = parent[fname]
        out[key] = format_bibtex(tag, key, fields)
    return out


def format_bibtex(tag: str, key: str, fields: dict[str, object]) -> str:
    """Render a single entry in DBLP's house style."""
    fields = dict(fields)
    fields.setdefault("biburl", f"https://dblp.org/rec/{key}.bib")
    fields.setdefault("bibsource", "dblp computer science bibliography, https://dblp.org")

    if "pages" in fields:
        fields["pages"] = re.sub(r"(\d)-(\d)", r"\1--\2", str(fields["pages"]))

    lines: list[str] = [f"@{tag}{{DBLP:{key},"]
    body: list[str] = []
    for fname in FIELD_ORDER:
        if fname not in fields:
            continue
        value = fields[fname]
        if fname in ("author", "editor") and isinstance(value, list):
            value = (" and\n" + _AUTHOR_INDENT).join(value)
        body.append(f"  {fname:<{_FIELD_PADDING}}= {{{value}}},")

    if body:
        body[-1] = body[-1].rstrip(",")
    lines.extend(body)
    lines.append("}")
    return "\n".join(lines)
