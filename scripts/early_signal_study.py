"""End-to-end measurement: are top-4 cybersecurity papers preprinted on arXiv?

Pipeline:
  1. Harvest cs.CR preprints from arXiv (or load a cached snapshot).
  2. Load top-4 papers from the local SQLite corpus.
  3. For each top-4 paper, find arXiv preprints by the same authors with
     a similar title.
  4. Compute match rate, lag distribution, and per-venue breakdowns.

The harvest step is bandwidth-light (a few tens of MB of metadata) but
takes minutes because arXiv asks for a three-second pause between
queries. Re-running this script reuses a cached snapshot.

Usage::

    python scripts/early_signal_study.py --harvest    # one-time fetch
    python scripts/early_signal_study.py              # analyse from cache
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.arxiv_fetcher import ARXIV_ACKNOWLEDGMENT, harvest, load_jsonl, save_jsonl
from src.author_matcher import (
    DEFAULT_TITLE_THRESHOLD,
    Match,
    build_author_index,
    find_matches,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

TOP4 = ("USENIX Security", "ACM CCS", "IEEE S&P", "NDSS")
STUDY_YEARS = (2024, 2025)
SNAPSHOT_PATH = Path("data/arxiv/cs_cr_2022_2026.jsonl")


def harvest_snapshot(force: bool) -> Path:
    if SNAPSHOT_PATH.exists() and not force:
        logger.info("snapshot already present at %s", SNAPSHOT_PATH)
        return SNAPSHOT_PATH

    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    written = save_jsonl(harvest(since_year=2022, until_year=2026), SNAPSHOT_PATH)
    logger.info("harvested %d preprints in %.1f min", written, (time.time() - start) / 60)
    return SNAPSHOT_PATH


def load_top4_cohort(db_path: Path, years: tuple[int, ...]) -> list[dict]:
    placeholders = ",".join(["?"] * len(TOP4))
    year_placeholders = ",".join(["?"] * len(years))
    query = (
        f"SELECT paper_id, title, authors, year, event FROM papers "
        f"WHERE event IN ({placeholders}) AND year IN ({year_placeholders}) "
        f"AND title IS NOT NULL AND authors IS NOT NULL"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(query, TOP4 + years).fetchall()]


def parse_authors(authors_text: str) -> list[str]:
    return [a.strip() for a in authors_text.split(",") if a.strip()]


def report(matches: list[Match], total_papers: int, matched_paper_ids: set[str]) -> None:
    print()
    print("═" * 70)
    print(f" Early-signal study — top-4 cybersecurity papers in {STUDY_YEARS}")
    print("═" * 70)
    print()

    n_matched = len(matched_paper_ids)
    rate = n_matched / total_papers if total_papers else 0.0
    print(f"  Top-4 papers analysed     : {total_papers:>5}")
    print(f"  Papers with arXiv preprint: {n_matched:>5}  ({rate*100:.1f}%)")
    print()

    if not matches:
        print("  No matches found.")
        return

    lags = sorted({m.paper_id: m.lag_days for m in matches}.values())  # one per paper
    print("  Preprint-to-publication lag (days):")
    print(f"    p25    p50    p75    p90    max")
    p = statistics.quantiles(lags, n=100)
    print(f"    {p[24]:>5}  {p[49]:>5}  {p[74]:>5}  {p[89]:>5}  {max(lags):>5}")
    print()

    print("  By venue:")
    by_venue: dict[str, dict[str, int]] = {}
    for m in matches:
        v = by_venue.setdefault(m.paper_venue, {"matches": 0})
        v["matches"] += 1
    venue_totals = _venue_totals()
    print(f"    {'venue':<22} {'matched':>8} {'total':>6} {'rate':>6}")
    for venue in TOP4:
        matched_in_venue = len({m.paper_id for m in matches if m.paper_venue == venue})
        total_in_venue = venue_totals.get(venue, 0)
        venue_rate = matched_in_venue / total_in_venue if total_in_venue else 0.0
        print(f"    {venue:<22} {matched_in_venue:>8} {total_in_venue:>6} {venue_rate*100:>5.1f}%")
    print()

    print("  Top-similarity examples:")
    seen: set[str] = set()
    examples = sorted(matches, key=lambda m: m.title_similarity, reverse=True)
    for m in examples:
        if m.paper_id in seen:
            continue
        seen.add(m.paper_id)
        if len(seen) > 5:
            break
        print(f"    [{m.title_similarity:.2f}] {m.paper_venue} {m.paper_year}")
        print(f"           paper : {m.paper_title[:80]}")
        print(f"           arXiv : {m.arxiv_title[:80]} (lag {m.lag_days}d)")

    print()
    print(f"  {ARXIV_ACKNOWLEDGMENT}")


def _venue_totals() -> dict[str, int]:
    with sqlite3.connect("data/dataset/papers.db") as conn:
        return dict(conn.execute(
            f"SELECT event, COUNT(*) FROM papers "
            f"WHERE event IN ({','.join(['?']*len(TOP4))}) "
            f"AND year IN ({','.join(['?']*len(STUDY_YEARS))}) "
            f"GROUP BY event",
            TOP4 + STUDY_YEARS,
        ).fetchall())


def export_matches(matches: list[Match], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        for match in matches:
            payload = {
                "paper_id": match.paper_id,
                "arxiv_id": match.arxiv_id,
                "paper_title": match.paper_title,
                "arxiv_title": match.arxiv_title,
                "title_similarity": match.title_similarity,
                "shared_authors": list(match.shared_authors),
                "lag_days": match.lag_days,
                "paper_year": match.paper_year,
                "paper_venue": match.paper_venue,
                "arxiv_submitted": match.arxiv_submitted,
            }
            fh.write(json.dumps(payload) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harvest", action="store_true",
                        help="force a fresh arXiv harvest before analysis")
    parser.add_argument("--title-threshold", type=float, default=DEFAULT_TITLE_THRESHOLD,
                        help="Jaccard threshold for title match (default 0.55)")
    args = parser.parse_args()

    snapshot = harvest_snapshot(force=args.harvest)
    logger.info("loading preprints from %s", snapshot)
    preprints = load_jsonl(snapshot)
    logger.info("loaded %d preprints", len(preprints))

    author_index = build_author_index(preprints)
    logger.info("indexed %d unique author keys", len(author_index))

    cohort = load_top4_cohort(Path("data/dataset/papers.db"), STUDY_YEARS)
    logger.info("analysing %d top-4 papers from %s", len(cohort), STUDY_YEARS)

    all_matches: list[Match] = []
    matched_paper_ids: set[str] = set()
    for paper in cohort:
        matches = find_matches(
            paper_id=paper["paper_id"],
            paper_title=paper["title"],
            paper_authors=parse_authors(paper["authors"]),
            paper_year=paper["year"],
            paper_venue=paper["event"],
            author_index=author_index,
            title_threshold=args.title_threshold,
        )
        if matches:
            all_matches.extend(matches)
            matched_paper_ids.add(paper["paper_id"])

    export_matches(all_matches, Path("data/arxiv/matches.jsonl"))
    report(all_matches, len(cohort), matched_paper_ids)
    return 0


if __name__ == "__main__":
    sys.exit(main())
