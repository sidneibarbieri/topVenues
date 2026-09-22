"""Top-4 concentration must rank by share and refuse thin evidence."""

from __future__ import annotations

import pytest

from src.analytics import (
    CONCENTRATION_MINIMUM_PAPERS,
    TOP4_CONCENTRATION_METRIC,
    _author_sort_key,
    top4_concentration,
)


def _entry(author: str, papers: int, top4: int) -> dict:
    return {"author": author, "papers": papers, "top4": top4, "score": 0.0}


def test_share_is_top4_over_papers():
    assert top4_concentration(_entry("a", 40, 30)) == pytest.approx(0.75)


def test_an_author_with_no_papers_scores_zero_instead_of_dividing():
    assert top4_concentration(_entry("a", 0, 0)) == 0.0


def test_concentration_outranks_volume():
    """A 38/38 author must beat a 95/78 one, which paper count cannot express."""
    focused = _entry("focused", 38, 38)
    prolific = _entry("prolific", 95, 78)
    ordered = sorted(
        [prolific, focused],
        key=lambda entry: _author_sort_key(entry, TOP4_CONCENTRATION_METRIC),
    )
    assert ordered[0]["author"] == "focused"


def test_ties_on_share_break_towards_the_larger_body_of_work():
    small = _entry("small", 12, 12)
    large = _entry("large", 38, 38)
    ordered = sorted(
        [small, large],
        key=lambda entry: _author_sort_key(entry, TOP4_CONCENTRATION_METRIC),
    )
    assert ordered[0]["author"] == "large"


def test_the_minimum_is_high_enough_to_exclude_single_paper_authors():
    """One top-4 paper scores 100%; without a floor that noise wins the ranking."""
    assert CONCENTRATION_MINIMUM_PAPERS >= 5


def _corpus(tmp_path):
    """One author with a mixed record, one who never reaches a top-4 venue."""
    import sqlite3

    db = tmp_path / "papers.db"
    connection = sqlite3.connect(db)
    connection.execute(
        "create table papers (paper_id text, title text, abstract text, "
        "authors text, event text, year integer)"
    )
    rows = [(f"p{i}", f"t{i}", "", "Mixed Author", "ACM CCS", 2024) for i in range(9)]
    rows += [(f"q{i}", f"u{i}", "", "Mixed Author", "ESORICS", 2024) for i in range(3)]
    rows += [(f"r{i}", f"v{i}", "", "Regional Author", "ESORICS", 2024) for i in range(12)]
    connection.executemany("insert into papers values (?,?,?,?,?,?)", rows)
    connection.commit()
    connection.close()
    return db


def test_a_top4_only_scope_does_not_force_every_author_to_100_percent(tmp_path):
    """The scope selects who appears; the ratio still spans the whole record.

    Counting inside the filter made the denominator equal the numerator, so
    every author scored 100% and the ranking silently became a paper count.
    """
    from src.analytics import reference_authors

    ranked = reference_authors(
        _corpus(tmp_path),
        allowed_tiers=frozenset({"top-4"}),
        ranking_metric=TOP4_CONCENTRATION_METRIC,
    )

    assert [entry["author"] for entry in ranked] == ["Mixed Author"]
    assert ranked[0]["papers"] == 12, "the 3 ESORICS papers belong in the denominator"
    assert ranked[0]["top4_share"] == pytest.approx(0.75)


def test_a_scope_without_top4_still_measures_concentration(tmp_path):
    """Excluding top-4 from the scope must not zero out the numerator."""
    from src.analytics import reference_authors

    ranked = reference_authors(
        _corpus(tmp_path),
        allowed_tiers=frozenset({"top-tier"}),
        ranking_metric=TOP4_CONCENTRATION_METRIC,
    )

    shares = {entry["author"]: entry["top4_share"] for entry in ranked}
    assert shares["Mixed Author"] == pytest.approx(0.75)
    assert shares["Regional Author"] == 0.0


def test_other_metrics_still_count_only_papers_inside_the_scope(tmp_path):
    """Only concentration widens the count; paper rankings must not change."""
    from src.analytics import PAPER_COUNT_METRIC, reference_authors

    ranked = reference_authors(
        _corpus(tmp_path),
        allowed_tiers=frozenset({"top-4"}),
        ranking_metric=PAPER_COUNT_METRIC,
    )

    assert [(entry["author"], entry["papers"]) for entry in ranked] == [("Mixed Author", 9)]
