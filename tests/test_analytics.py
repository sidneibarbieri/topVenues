"""Tests for area-level analytics."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.analytics import _split_authors, area_year_counts, authors_at_position, top_authors
from src.models import Paper


def _corpus(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    connection.execute("create table papers (paper_id text, authors text, event text, year int)")
    connection.executemany(
        "insert into papers values (?, ?, ?, ?)",
        [
            ("p1", "Alice, Bob", "ACM CCS", 2024),
            ("p2", "Alice and Carol", "IEEE S&P", 2025),
            ("p3", "Dan", "HotNets", 2025),
        ],
    )
    connection.commit()
    connection.close()


def test_area_year_counts_groups_by_area_and_year(tmp_path: Path) -> None:
    db = tmp_path / "papers.db"
    _corpus(db)
    counts = area_year_counts(db)
    assert counts["security"] == {2024: 1, 2025: 1}
    assert counts["networks"] == {2025: 1}


def test_top_authors_overall_and_by_area(tmp_path: Path) -> None:
    db = tmp_path / "papers.db"
    _corpus(db)
    assert top_authors(db)[0] == ("Alice", 2)  # Alice appears in two security papers
    assert ("Dan", 1) in top_authors(db, area="networks")
    assert all(name != "Dan" for name, _ in top_authors(db, area="security"))


def test_dblp_identity_suffix_is_preserved() -> None:
    assert _split_authors("Wei Wang 0001, Wei Wang 0002") == ["Wei Wang 0001", "Wei Wang 0002"]


def test_author_position_views_preserve_dblp_identity_suffixes() -> None:
    authors = "Alice 0001, Bob 0002, Carol 0003"
    assert authors_at_position(authors, "any") == ["Alice 0001", "Bob 0002", "Carol 0003"]
    assert authors_at_position(authors, "first") == ["Alice 0001"]
    assert authors_at_position(authors, "last") == ["Carol 0003"]


def test_reference_authors_supports_first_and_last_author_views(tmp_path: Path) -> None:
    from src.analytics import reference_authors
    from src.database import DatabaseManager

    manager = DatabaseManager(tmp_path / "papers.db")
    manager.upsert_papers(
        [
            Paper(
                paper_id="conf/ccs/One24",
                title="One",
                year=2024,
                event="ACM CCS",
                authors="Alice, Bob",
            ),
            Paper(
                paper_id="conf/ndss/Two25",
                title="Two",
                year=2025,
                event="NDSS",
                authors="Carol, Bob",
            ),
        ]
    )

    assert reference_authors(manager.db_path, position="first")[0]["author"] == "Alice"
    assert reference_authors(manager.db_path, position="last")[0]["author"] == "Bob"


def test_reference_authors_can_restrict_to_security_big_four(tmp_path: Path) -> None:
    from src.analytics import reference_authors
    from src.database import DatabaseManager
    from src.tiers import TOP4

    manager = DatabaseManager(tmp_path / "papers.db")
    manager.upsert_papers(
        [
            Paper(paper_id="1", title="Top", year=2024, event="ACM CCS", authors="Alice"),
            Paper(paper_id="2", title="Strong", year=2024, event="IEEE CNS", authors="Bob"),
        ]
    )

    authors = reference_authors(manager.db_path, allowed_tiers=frozenset({TOP4}))
    assert [entry["author"] for entry in authors] == ["Alice"]


def test_reference_authors_defaults_to_raw_count_and_exposes_weighted_view(
    tmp_path: Path,
) -> None:
    from src.analytics import TIER_WEIGHTED_METRIC, reference_authors
    from src.database import DatabaseManager

    manager = DatabaseManager(tmp_path / "papers.db")
    manager.upsert_papers(
        [
            Paper(paper_id="1", title="One", year=2024, event="IEEE CNS", authors="Alice"),
            Paper(paper_id="2", title="Two", year=2025, event="IEEE CNS", authors="Alice"),
            Paper(paper_id="3", title="Three", year=2025, event="ACM CCS", authors="Bob"),
        ]
    )

    assert reference_authors(manager.db_path)[0]["author"] == "Alice"
    assert reference_authors(manager.db_path)[0]["recent_papers"] == 2
    assert (
        reference_authors(manager.db_path, ranking_metric=TIER_WEIGHTED_METRIC)[0]["author"]
        == "Bob"
    )


def test_reference_authors_rejects_unknown_metric(tmp_path: Path) -> None:
    from src.analytics import reference_authors
    from src.database import DatabaseManager

    manager = DatabaseManager(tmp_path / "papers.db")
    with pytest.raises(ValueError, match="unknown author ranking metric"):
        reference_authors(manager.db_path, ranking_metric="impact")


class TestTopicTrend:
    @pytest.fixture
    def trend_db(self, tmp_path):
        from src.database import DatabaseManager
        from src.models import Paper

        manager = DatabaseManager(tmp_path / "papers.db")
        manager.upsert_papers(
            [
                Paper(paper_id="1", title="LLM agents", year=2024, event="ACM CCS"),
                Paper(
                    paper_id="2",
                    title="Fuzzing loops",
                    year=2024,
                    event="ACM CCS",
                    abstract="uses an llm oracle",
                ),
                Paper(paper_id="3", title="Routing", year=2024, event="ACM SIGCOMM"),
                Paper(paper_id="4", title="LLM watermarking", year=2025, event="NDSS"),
                Paper(paper_id="5", title="Congestion", year=2025, event="ACM SIGCOMM"),
            ]
        )
        return manager.db_path

    def test_counts_and_share_by_year(self, trend_db):
        from src.analytics import topic_trend

        trend = topic_trend(trend_db, "LLM")

        assert trend["total"] == 3
        assert trend["by_year"] == [
            {"year": 2024, "papers": 2, "share_pct": 66.67},
            {"year": 2025, "papers": 1, "share_pct": 50.0},
        ]

    def test_match_is_case_insensitive_over_abstract(self, trend_db):
        from src.analytics import topic_trend

        trend = topic_trend(trend_db, "llm")
        assert trend["total"] == 3

    def test_area_filter_scopes_both_numerator_and_denominator(self, trend_db):
        from src.analytics import topic_trend

        trend = topic_trend(trend_db, "LLM", area="security")

        assert trend["by_year"] == [
            {"year": 2024, "papers": 2, "share_pct": 100.0},
            {"year": 2025, "papers": 1, "share_pct": 100.0},
        ]

    def test_tier_filter_scopes_both_numerator_and_denominator(self, trend_db):
        from src.analytics import topic_trend
        from src.tiers import TOP_TIER

        trend = topic_trend(trend_db, "LLM", allowed_tiers=frozenset({TOP_TIER}))

        assert trend["total"] == 0
        assert trend["by_year"] == [
            {"year": 2024, "papers": 0, "share_pct": 0.0},
            {"year": 2025, "papers": 0, "share_pct": 0.0},
        ]

    def test_year_start_cuts_earlier_years(self, trend_db):
        from src.analytics import topic_trend

        trend = topic_trend(trend_db, "LLM", year_start=2025)
        assert [row["year"] for row in trend["by_year"]] == [2025]

    def test_top_venues_ordered_by_count(self, trend_db):
        from src.analytics import topic_trend

        trend = topic_trend(trend_db, "LLM")
        assert trend["top_venues"][0] == ("ACM CCS", 2)
