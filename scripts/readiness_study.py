"""Measure the scientific-readiness triage filter on the local corpus.

Question: does authorship by a researcher with a *prior* top-4 publication
predict that an arXiv cs.CR preprint will itself become a top-4 paper?

The experiment reuses the cached arXiv snapshot produced by
``early_signal_study.py`` and the local SQLite corpus; it runs fully
offline and in a few seconds.

Usage::

    python scripts/readiness_study.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.arxiv_fetcher import ARXIV_ACKNOWLEDGMENT, load_jsonl
from src.readiness import OutcomeIndex, ReadinessResult, analyze, build_prior_author_set

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

TOP4 = ("USENIX Security", "ACM CCS", "IEEE S&P", "NDSS")
SNAPSHOT_PATH = Path("data/arxiv/cs_cr_2022_2026.jsonl")
DB_PATH = Path("data/dataset/papers.db")

# Each row: (preprint cohort year, prior-record window, outcome window).
COHORTS = (
    (2023, (2019, 2022), (2023, 2026)),
    (2022, (2019, 2021), (2022, 2026)),
)
THRESHOLDS = (0.5, 0.6, 0.7)


def _top4_authors(conn: sqlite3.Connection, lo: int, hi: int) -> list[list[str]]:
    rows = conn.execute(
        f"SELECT authors FROM papers WHERE event IN ({','.join('?' * len(TOP4))}) "
        f"AND year BETWEEN ? AND ? AND authors IS NOT NULL",
        (*TOP4, lo, hi),
    ).fetchall()
    return [[a.strip() for a in r[0].split(",") if a.strip()] for r in rows]


def _top4_titles(conn: sqlite3.Connection, lo: int, hi: int) -> list[str]:
    rows = conn.execute(
        f"SELECT title FROM papers WHERE event IN ({','.join('?' * len(TOP4))}) "
        f"AND year BETWEEN ? AND ? AND title IS NOT NULL",
        (*TOP4, lo, hi),
    ).fetchall()
    return [r[0] for r in rows]


def _print_result(year: int, result: ReadinessResult) -> None:
    print(
        f"    {year}  thr={result.threshold:<4} "
        f"precision {result.precision * 100:5.2f}%  "
        f"base {result.base_rate * 100:4.2f}%  "
        f"lift {result.lift:5.1f}x  "
        f"recall {result.recall * 100:3.0f}%  "
        f"vol-cut {result.volume_reduction * 100:3.0f}%"
    )


def main() -> int:
    preprints = load_jsonl(SNAPSHOT_PATH)
    logger.info("loaded %d preprints", len(preprints))

    print()
    print("═" * 70)
    print(" Scientific-readiness filter — prior-top-4 authorship as an")
    print(" early-signal predictor for arXiv cs.CR preprints")
    print("═" * 70)
    print()
    print("  P(preprint becomes a top-4 paper) by author track record:")
    print()

    with sqlite3.connect(DB_PATH) as conn:
        for cohort_year, (plo, phi), (olo, ohi) in COHORTS:
            prior = build_prior_author_set(_top4_authors(conn, plo, phi))
            outcome = OutcomeIndex(_top4_titles(conn, olo, ohi))
            cohort = [
                (p.title, p.authors)
                for p in preprints
                if p.submitted_at.startswith(str(cohort_year))
            ]
            logger.info(
                "cohort %d: %d preprints, %d prior-top-4 authors (%d-%d)",
                cohort_year, len(cohort), len(prior), plo, phi,
            )
            for threshold in THRESHOLDS:
                _print_result(cohort_year, analyze(cohort, prior, outcome, threshold))
            print()

    print(f"  {ARXIV_ACKNOWLEDGMENT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
