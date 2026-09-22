#!/usr/bin/env python3
"""Report how many curated award papers appear in the topVenues corpus.

Loads every ``data/awards/*_paper_awards.json`` table, joins it to one verified
corpus profile by normalized title within venue, prints per-venue coverage, and
writes matched pairs into that profile's disposable analysis workspace.
Unmatched awards are reported honestly: the selected profile may not contain
that paper for that venue/year.
"""

from __future__ import annotations

import argparse
import collections
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.awards import load_award_records, match_awards_to_corpus  # noqa: E402
from src.profiles import (  # noqa: E402
    PROFILE_IDS,
    PROJECT_ROOT,
    select_profile_id,
    verified_profile_snapshot,
)

AWARDS_DIR = REPO_ROOT / "data" / "awards"
MATCH_FIELDS = ("venue", "year", "award", "title", "paper_id", "source_url")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        choices=PROFILE_IDS,
        default=select_profile_id(),
        help="immutable corpus profile (default: TOPVENUES_PROFILE or security-20)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="TSV destination (default: selected profile's analysis workspace)",
    )
    args = parser.parse_args()

    records = load_award_records(AWARDS_DIR)
    if not records:
        raise SystemExit(f"no award tables found under {AWARDS_DIR}")

    with verified_profile_snapshot(args.profile, PROJECT_ROOT) as verified:
        matched, unmatched = match_awards_to_corpus(records, verified.database_path)
        output = args.output or (
            verified.profile.workspace_data_dir.parent / "analysis" / "award_corpus_matches.tsv"
        )

    by_venue: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0])
    for match in matched:
        by_venue[match.award.venue][0] += 1
    for record in unmatched:
        by_venue[record.venue][1] += 1

    print(
        f"profile: {args.profile} | award records: {len(records)} | "
        f"matched in corpus: {len(matched)} | unmatched: {len(unmatched)}"
    )
    print(f"{'venue':<18}{'matched':>9}{'unmatched':>11}")
    for venue in sorted(by_venue):
        hit, miss = by_venue[venue]
        print(f"{venue:<18}{hit:>9}{miss:>11}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(MATCH_FIELDS)
        for match in matched:
            award = match.award
            writer.writerow(
                [
                    award.venue,
                    award.year,
                    award.award,
                    award.title,
                    match.paper_id,
                    award.source_url,
                ]
            )
    print(f"wrote {len(matched)} matches -> {output}")

    if unmatched:
        print("\nunmatched (not in corpus by title+venue):")
        for record in unmatched:
            print(f"  [{record.venue} {record.year}] {record.title}")


if __name__ == "__main__":
    main()
