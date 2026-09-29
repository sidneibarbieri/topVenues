#!/usr/bin/env python3
"""Turn the USENIX best-papers capture into an award table.

USENIX serves its best-papers listing behind a browser challenge, so the
listing cannot be re-fetched by a script. The capture under
``data/awards/captures/`` records what the official pages said, with the date
and the method; this importer is the reproducible step from that capture to
``data/awards/usenix_2019_2026_paper_awards.json``.

An award another table already holds (same venue, year and title) is left out,
so no paper is labeled twice.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.awards import AwardRecord, load_award_records, normalize_title  # noqa: E402

AWARDS_DIR = REPO_ROOT / "data" / "awards"
CAPTURE = AWARDS_DIR / "captures" / "usenix_best_papers_2019_2026.json"
OUTPUT_JSON = AWARDS_DIR / "usenix_2019_2026_paper_awards.json"
OUTPUT_TSV = AWARDS_DIR / "usenix_2019_2026_paper_awards.tsv"
LISTING_URL = (
    "https://www.usenix.org/conferences/best-papers?taxonomy_vocabulary_1_tid={year}&title_1="
)
CAPTURED_EVENT_TO_VENUE = {"USENIX Security": "USENIX Security", "WOOT": "USENIX WOOT"}
FIELDS = ("venue", "year", "award", "title", "url", "source_url")


@dataclass(frozen=True)
class CapturedAward:
    event: str
    year: int
    title: str
    url: str
    award: str


def load_capture(path: Path = CAPTURE) -> list[CapturedAward]:
    capture = json.loads(path.read_text(encoding="utf-8"))
    return [CapturedAward(*row) for row in capture["records"]]


def to_record(captured: CapturedAward) -> AwardRecord:
    return AwardRecord(
        venue=CAPTURED_EVENT_TO_VENUE[captured.event],
        year=captured.year,
        award=captured.award,
        title=captured.title,
        url=captured.url,
        source_url=LISTING_URL.format(year=captured.year),
    )


def award_key(record: AwardRecord) -> tuple[str, int, str]:
    return (record.venue, record.year, normalize_title(record.title))


def new_records(captured: list[CapturedAward], existing: list[AwardRecord]) -> list[AwardRecord]:
    held = {award_key(record) for record in existing}
    records = [to_record(award) for award in captured]
    return [record for record in records if award_key(record) not in held]


def write_records(records: list[AwardRecord]) -> None:
    rows = [asdict(record) for record in records]
    OUTPUT_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with OUTPUT_TSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    other_tables = [
        record
        for record in load_award_records(AWARDS_DIR)
        if not record.source_url.startswith(LISTING_URL.split("?")[0])
    ]
    records = new_records(load_capture(), other_tables)
    write_records(records)
    print(f"Wrote {len(records)} USENIX paper-award records to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
