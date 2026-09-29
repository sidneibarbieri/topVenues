#!/usr/bin/env python3
"""Turn dated award captures into award tables, and check them against their sources.

Some official award pages cannot be parsed by one rule: every ACM CCS year uses a
different layout, and USENIX serves its listing behind a browser challenge. A
capture under ``data/awards/captures/`` records what those pages said, the date,
and how it was read; each record keeps the official page it came from.

``import`` writes ``data/awards/<capture>_paper_awards.json`` (and a ``.tsv``
mirror) for every capture, leaving out any award another table already holds
(same venue, year and title), so no paper is labeled twice.

``verify`` fetches every source page of the captures that a script can reach
and fails if a recorded title is not on its page.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, ConfigDict

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.awards import (  # noqa: E402
    AwardRecord,
    award_tables,
    load_award_table,
    normalize_title,
)

AWARDS_DIR = REPO_ROOT / "data" / "awards"
CAPTURES_DIR = AWARDS_DIR / "captures"
FIELDS = ("venue", "year", "award", "title", "url", "source_url")


class CapturedAward(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    year: int
    award: str
    title: str
    url: str
    source_url: str
    published_title: str | None = None

    def to_record(self) -> AwardRecord:
        """The award under its proceedings title, which is what the corpus holds."""
        return AwardRecord(
            venue=self.venue,
            year=self.year,
            award=self.award,
            title=self.published_title or self.title,
            url=self.url,
            source_url=self.source_url,
        )


class AwardCapture(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: str
    captured_on: str
    scripted_verification: bool
    method: str
    records: tuple[CapturedAward, ...]


def load_capture(path: Path) -> AwardCapture:
    return AwardCapture.model_validate_json(path.read_text(encoding="utf-8"))


def output_table(capture_path: Path, awards_dir: Path = AWARDS_DIR) -> Path:
    return awards_dir / f"{capture_path.stem}_paper_awards.json"


def award_key(record: AwardRecord) -> tuple[str, int, str]:
    return (record.venue, record.year, normalize_title(record.title))


def new_records(capture: AwardCapture, held: list[AwardRecord]) -> list[AwardRecord]:
    held_keys = {award_key(record) for record in held}
    records = [award.to_record() for award in capture.records]
    return [record for record in records if award_key(record) not in held_keys]


def records_held_elsewhere(table: Path, awards_dir: Path = AWARDS_DIR) -> list[AwardRecord]:
    return [
        record
        for path in award_tables(awards_dir)
        if path != table
        for record in load_award_table(path)
    ]


def write_table(records: list[AwardRecord], table: Path) -> None:
    rows = [asdict(record) for record in records]
    table.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with table.with_suffix(".tsv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def import_captures() -> None:
    for capture_path in sorted(CAPTURES_DIR.glob("*.json")):
        table = output_table(capture_path)
        records = new_records(load_capture(capture_path), records_held_elsewhere(table))
        write_table(records, table)
        print(f"{capture_path.name}: {len(records)} records -> {table.name}")


def page_text(html: str) -> str:
    return normalize_title(BeautifulSoup(html, "html.parser").get_text(" "))


def titles_missing_from_pages(capture: AwardCapture, pages: dict[str, str]) -> list[CapturedAward]:
    return [
        award
        for award in capture.records
        if normalize_title(award.title) not in pages[award.source_url]
    ]


def verify_captures() -> None:
    missing: list[CapturedAward] = []
    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        for capture_path in sorted(CAPTURES_DIR.glob("*.json")):
            capture = load_capture(capture_path)
            if not capture.scripted_verification:
                print(f"{capture_path.name}: not reachable by a script, skipped")
                continue
            sources = sorted({award.source_url for award in capture.records})
            pages = {url: page_text(client.get(url).raise_for_status().text) for url in sources}
            absent = titles_missing_from_pages(capture, pages)
            print(f"{capture_path.name}: {len(capture.records) - len(absent)}/"
                  f"{len(capture.records)} titles found on {len(sources)} pages")  # fmt: skip
            missing.extend(absent)
    if missing:
        listing = "\n".join(f"  [{a.venue} {a.year}] {a.title} ({a.source_url})" for a in missing)
        raise SystemExit(f"titles not found on their source page:\n{listing}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("command", choices=("import", "verify"))
    arguments = parser.parse_args()
    if arguments.command == "import":
        import_captures()
    else:
        verify_captures()


if __name__ == "__main__":
    main()
