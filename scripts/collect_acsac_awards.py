#!/usr/bin/env python3
"""Collect ACSAC paper-award metadata from the official archive page."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx
from bs4 import BeautifulSoup, Tag

ARCHIVE_URL = "https://www.acsac.org/archive/"
AWARD_PATTERNS = (
    "Distinguished Paper",
    "Distinguished Paper with Artifacts",
    "Outstanding Paper",
    "Outstanding Paper and Student Paper",
    "Outstanding Student Paper",
)


@dataclass(frozen=True)
class AwardRecord:
    venue: str
    year: int
    award: str
    title: str
    url: str | None
    source_url: str


def _year_from_heading(heading: Tag) -> int | None:
    text = heading.get_text(" ", strip=True)
    match = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    return int(match.group(1)) if match else None


def _parse_award_text(text: str) -> tuple[str, str] | None:
    cleaned = " ".join(text.split())
    if "–" in cleaned:
        award, title = cleaned.split("–", 1)
    elif "-" in cleaned:
        award, title = cleaned.split("-", 1)
    else:
        return None

    award = award.strip()
    if not any(award.startswith(pattern) for pattern in AWARD_PATTERNS):
        return None

    title = title.strip().strip('" ')
    if not title:
        return None
    return award, title


def collect_awards(html: str) -> list[AwardRecord]:
    soup = BeautifulSoup(html, "html.parser")
    records: list[AwardRecord] = []

    for heading in soup.find_all("h3"):
        year = _year_from_heading(heading)
        if year is None:
            continue

        sibling = heading.find_next_sibling()
        while sibling is not None and not (isinstance(sibling, Tag) and sibling.name == "h3"):
            if isinstance(sibling, Tag):
                for item in sibling.find_all("li"):
                    parsed = _parse_award_text(item.get_text(" ", strip=True))
                    if parsed is None:
                        continue
                    award, title = parsed
                    link = item.find("a", href=True)
                    records.append(
                        AwardRecord(
                            venue="ACSAC",
                            year=year,
                            award=award,
                            title=title,
                            url=link["href"] if link else None,
                            source_url=ARCHIVE_URL,
                        )
                    )
            sibling = sibling.find_next_sibling()

    if not records:
        raise RuntimeError("No ACSAC paper awards found in the official archive HTML.")
    return records


def write_outputs(records: list[AwardRecord], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "acsac_paper_awards.json"
    tsv_path = output_dir / "acsac_paper_awards.tsv"

    rows = [asdict(record) for record in records]
    json_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with tsv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    response = httpx.get(ARCHIVE_URL, timeout=60.0, follow_redirects=True)
    response.raise_for_status()
    records = collect_awards(response.text)
    output_dir = Path(__file__).resolve().parents[1] / "data" / "awards"
    write_outputs(records, output_dir)
    print(f"Wrote {len(records)} ACSAC paper-award records to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
