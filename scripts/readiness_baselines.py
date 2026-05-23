"""Baselines and operating points for the scientific-readiness filter.

This experiment answers the two questions a careful reviewer asks about the
readiness result:

  1. Is prior top-tier authorship a real signal, or just a proxy for
     publication volume or community membership? We compare it against a
     prolific-author baseline, an any-security-venue baseline, and a random
     security-author control.
  2. Which author carries the signal? We re-run the same filter restricted to
     the first author and to the senior (last) author, exposing a tunable
     precision-recall trade-off.

It reads the configured publication scope and the versioned snapshot, runs
offline in a few seconds, and reuses :func:`src.readiness.analyze` for every
row by feeding it the cohort projected onto the relevant author position.
"""

from __future__ import annotations

import random
import sqlite3
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.arxiv_fetcher import ARXIV_ACKNOWLEDGMENT, load_jsonl
from src.config import load_configuration
from src.readiness import (
    OutcomeIndex,
    ReadinessResult,
    analyze,
    build_prior_author_set,
    strict_author_key,
)

DB_PATH = Path("data/dataset/papers.db")
THRESHOLD = 0.6
PROLIFIC_MIN_PAPERS = 3
RANDOM_SEED = 42

Cohort = list[tuple[str, tuple[str, ...]]]


def _author_lists(conn: sqlite3.Connection, events: tuple[str, ...] | None,
                  lo: int, hi: int) -> list[list[str]]:
    if events is None:
        rows = conn.execute(
            "SELECT authors FROM papers WHERE year BETWEEN ? AND ? AND authors IS NOT NULL",
            (lo, hi),
        ).fetchall()
    else:
        placeholders = ",".join("?" * len(events))
        rows = conn.execute(
            f"SELECT authors FROM papers WHERE event IN ({placeholders}) "
            f"AND year BETWEEN ? AND ? AND authors IS NOT NULL",
            (*events, lo, hi),
        ).fetchall()
    return [[a.strip() for a in r[0].split(",") if a.strip()] for r in rows]


def _scope_titles(conn: sqlite3.Connection, events: tuple[str, ...], lo: int, hi: int) -> list[str]:
    placeholders = ",".join("?" * len(events))
    rows = conn.execute(
        f"SELECT title FROM papers WHERE event IN ({placeholders}) "
        f"AND year BETWEEN ? AND ? AND title IS NOT NULL",
        (*events, lo, hi),
    ).fetchall()
    return [r[0] for r in rows]


def _prolific_authors(author_lists: list[list[str]], minimum: int) -> frozenset[str]:
    counts: Counter[str] = Counter()
    for authors in author_lists:
        for key in {strict_author_key(a) for a in authors if strict_author_key(a)}:
            counts[key] += 1
    return frozenset(key for key, count in counts.items() if count >= minimum)


def _project(cohort: Cohort, position: int) -> Cohort:
    """Keep only the author at ``position`` so ``analyze`` tests that slot."""
    return [(title, (authors[position],)) for title, authors in cohort if authors]


def _row(label: str, result: ReadinessResult) -> str:
    lift = f"{result.lift:.1f}x" if result.base_rate else "inf"
    return (
        f"  {label:<26}{result.precision * 100:>7.1f}%{result.recall * 100:>8.0f}%"
        f"{lift:>7}{result.volume_reduction * 100:>9.0f}%"
    )


def main() -> int:
    scope = load_configuration().study_scope
    core_events = tuple(scope.core_events)
    cohort_year = max(scope.prior_windows)
    prior_lo, prior_hi = scope.prior_windows[cohort_year]
    outcome_lo, outcome_hi = scope.outcome_windows[cohort_year]

    conn = sqlite3.connect(DB_PATH)
    top4 = build_prior_author_set(_author_lists(conn, core_events, prior_lo, prior_hi))
    any_corpus = build_prior_author_set(_author_lists(conn, None, prior_lo, prior_hi))
    prolific = _prolific_authors(_author_lists(conn, None, prior_lo, prior_hi), PROLIFIC_MIN_PAPERS)
    random.seed(RANDOM_SEED)
    random_authors = frozenset(random.sample(sorted(any_corpus), len(top4)))

    outcome = OutcomeIndex(_scope_titles(conn, core_events, outcome_lo, outcome_hi))
    cohort: Cohort = [
        (p.title, p.authors)
        for p in load_jsonl(Path(scope.preprint_snapshot))
        if p.submitted_at.startswith(str(cohort_year))
    ]

    print()
    print(f"  Scientific-readiness baselines ({cohort_year} cs.CR preprints, "
          f"Jaccard {THRESHOLD})")
    print(f"  {'filter':<26}{'precision':>7}{'recall':>8}{'lift':>7}{'vol-cut':>9}")
    print("  prestige vs. trivial signals")
    print(_row("prior top-4 (any author)", analyze(cohort, top4, outcome, THRESHOLD)))
    print(_row("any security-venue author", analyze(cohort, any_corpus, outcome, THRESHOLD)))
    print(_row(f"prolific (>= {PROLIFIC_MIN_PAPERS} papers)", analyze(cohort, prolific, outcome, THRESHOLD)))
    print(_row("random security authors", analyze(cohort, random_authors, outcome, THRESHOLD)))
    print("  operating points by author position (prior top-4)")
    print(_row("first author", analyze(_project(cohort, 0), top4, outcome, THRESHOLD)))
    print(_row("senior (last) author", analyze(_project(cohort, -1), top4, outcome, THRESHOLD)))

    print(f"\n  {ARXIV_ACKNOWLEDGMENT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
