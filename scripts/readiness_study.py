"""Measure the scientific-readiness triage filter on the local corpus.

Question: does authorship by a researcher with a prior in-scope publication
predict that an arXiv cs.CR preprint will itself enter the configured scope?

The experiment reuses the cached arXiv snapshot produced by
``early_signal_study.py`` and the local SQLite corpus; it runs fully
offline and in a few seconds.

Usage::

    python scripts/readiness_study.py
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.arxiv_fetcher import ARXIV_ACKNOWLEDGMENT, load_jsonl
from src.config import load_configuration
from src.readiness import OutcomeIndex, ReadinessResult, analyze, build_prior_author_set

DB_PATH = Path("data/dataset/papers.db")


def _scope_authors(
    conn: sqlite3.Connection,
    events: list[str],
    lo: int,
    hi: int,
) -> list[list[str]]:
    rows = conn.execute(
        f"SELECT authors FROM papers WHERE event IN ({','.join('?' * len(events))}) "
        f"AND year BETWEEN ? AND ? AND authors IS NOT NULL",
        (*events, lo, hi),
    ).fetchall()
    return [[a.strip() for a in r[0].split(",") if a.strip()] for r in rows]


def _scope_titles(conn: sqlite3.Connection, events: list[str], lo: int, hi: int) -> list[str]:
    rows = conn.execute(
        f"SELECT title FROM papers WHERE event IN ({','.join('?' * len(events))}) "
        f"AND year BETWEEN ? AND ? AND title IS NOT NULL",
        (*events, lo, hi),
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
    scope = load_configuration().study_scope
    preprints = load_jsonl(Path(scope.preprint_snapshot))

    print()
    print("═" * 70)
    print(" Scientific-readiness filter — prior-scope authorship as an")
    print(" early-signal predictor for arXiv cs.CR preprints")
    print("═" * 70)
    print()
    print("  P(preprint joins the publication scope) by author track record:")
    print()

    with sqlite3.connect(DB_PATH) as conn:
        for cohort_year in sorted(scope.prior_windows, reverse=True):
            plo, phi = scope.prior_windows[cohort_year]
            olo, ohi = scope.outcome_windows[cohort_year]
            prior = build_prior_author_set(_scope_authors(conn, scope.core_events, plo, phi))
            outcome = OutcomeIndex(_scope_titles(conn, scope.core_events, olo, ohi))
            cohort = [
                (p.title, p.authors)
                for p in preprints
                if p.submitted_at.startswith(str(cohort_year))
            ]
            for threshold in scope.title_thresholds:
                _print_result(cohort_year, analyze(cohort, prior, outcome, threshold))
            print()

    print(f"  {ARXIV_ACKNOWLEDGMENT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
