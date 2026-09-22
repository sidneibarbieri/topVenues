#!/usr/bin/env python3
"""Import curated top-4 paper-award records into the topVenues awards dataset.

Source: a manifest of official award-page scrapes for IEEE S&P, ACM CCS, NDSS,
and USENIX Security, vendored under ``data/awards/sources/``. Each record keeps
the official award-page URL (``source_url``), so labels stay source-backed. The
output matches the ACSAC award-table schema (``data/awards/*_paper_awards.json``
plus a ``.tsv`` mirror), so all venues share one award format.
"""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.awards import AwardRecord  # noqa: E402  (path set above)

MANIFEST = REPO_ROOT / "data" / "awards" / "sources" / "top4_awards_manifest.json"
OUTPUT_JSON = REPO_ROOT / "data" / "awards" / "top4_paper_awards.json"
OUTPUT_TSV = REPO_ROOT / "data" / "awards" / "top4_paper_awards.tsv"
FIELDS = ("venue", "year", "award", "title", "url", "source_url")


def _to_record(row: dict) -> AwardRecord:
    return AwardRecord(
        venue=row["conference"],
        year=int(row["year"]),
        award=row["award"],
        title=row["title"].strip(),
        url=(row.get("resolved_url") or row.get("official_url") or None),
        source_url=row["official_url"],
    )


def main() -> None:
    rows = json.loads(MANIFEST.read_text(encoding="utf-8"))
    records = [_to_record(row) for row in rows]

    OUTPUT_JSON.write_text(
        json.dumps([asdict(record) for record in records], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with OUTPUT_TSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(FIELDS)
        for record in records:
            writer.writerow([getattr(record, field) or "" for field in FIELDS])

    print(f"wrote {len(records)} top-4 award records -> {OUTPUT_JSON.name}, {OUTPUT_TSV.name}")


if __name__ == "__main__":
    main()
