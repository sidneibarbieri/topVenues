"""Tests for the BM25 full-text index and ranked search."""

import pytest

from src.database import DatabaseManager
from src.models import Paper


@pytest.fixture
def db(tmp_path):
    return DatabaseManager(tmp_path / "papers.db")


def _paper(paper_id: str, **overrides) -> Paper:
    base = {
        "paper_id": paper_id,
        "title": f"Title {paper_id}",
        "year": 2024,
        "event": "ACM CCS",
    }
    base.update(overrides)
    return Paper(**base)


def _seed_corpus(db: DatabaseManager) -> None:
    db.upsert_papers(
        [
            _paper("1", title="Fuzzing the Kernel", abstract="We fuzz kernels."),
            _paper(
                "2",
                title="A Survey of Intrusion Detection",
                abstract="Fuzzing is mentioned once in passing.",
            ),
            _paper(
                "3",
                title="Unrelated Networking Paper",
                abstract="Congestion control.",
                event="ACM SIGCOMM",
            ),
            _paper(
                "4",
                title="Fuzzing Fuzzing Fuzzing",
                year=2020,
                abstract="All about fuzzing.",
                authors="Alice Fuzzer",
            ),
        ]
    )


class TestBuildFtsIndex:
    def test_index_absent_by_default(self, db):
        assert not db.has_fts_index()

    def test_build_creates_index(self, db):
        _seed_corpus(db)
        db.build_fts_index()
        assert db.has_fts_index()

    def test_build_is_idempotent(self, db):
        _seed_corpus(db)
        db.build_fts_index()
        db.build_fts_index()
        assert len(db.search_ranked("fuzzing", limit=None)) == 3

    def test_upserts_after_build_are_searchable(self, db):
        _seed_corpus(db)
        db.build_fts_index()
        db.upsert_paper(_paper("5", title="Late Fuzzing Arrival"))
        titles = {row["title"] for row in db.search_ranked("fuzzing", limit=None)}
        assert "Late Fuzzing Arrival" in titles

    def test_update_replaces_indexed_text(self, db):
        _seed_corpus(db)
        db.build_fts_index()
        db.upsert_paper(_paper("1", title="Renamed Symbolic Execution Paper"))
        titles = {row["title"] for row in db.search_ranked("fuzzing", limit=None)}
        assert "Fuzzing the Kernel" not in titles


class TestSearchRanked:
    def test_builds_index_on_first_use(self, db):
        _seed_corpus(db)
        assert not db.has_fts_index()
        results = db.search_ranked("fuzzing")
        assert db.has_fts_index()
        assert len(results) == 3

    def test_title_hits_outrank_abstract_hits(self, db):
        _seed_corpus(db)
        results = db.search_ranked("fuzzing")
        assert results[0]["title"] == "Fuzzing Fuzzing Fuzzing"
        assert results[-1]["title"] == "A Survey of Intrusion Detection"

    def test_rank_is_best_first(self, db):
        _seed_corpus(db)
        ranks = [row["rank"] for row in db.search_ranked("fuzzing")]
        assert ranks == sorted(ranks)

    def test_event_and_year_filters(self, db):
        _seed_corpus(db)
        assert db.search_ranked("fuzzing", event="ACM SIGCOMM", year=2020) == []
        results = db.search_ranked("fuzzing", year=2020)
        assert [row["paper_id"] for row in results] == ["4"]
        results = db.search_ranked("fuzzing", event="ACM CCS", year=2020)
        assert [row["paper_id"] for row in results] == ["4"]

    def test_limit(self, db):
        _seed_corpus(db)
        assert len(db.search_ranked("fuzzing", limit=1)) == 1

    def test_multi_token_query_is_conjunctive(self, db):
        _seed_corpus(db)
        results = db.search_ranked("fuzzing kernel")
        assert [row["paper_id"] for row in results] == ["1"]

    def test_special_characters_do_not_break_match_syntax(self, db):
        _seed_corpus(db)
        assert db.search_ranked('"unbalanced OR (') == []
        assert db.search_ranked("*") == []

    def test_prefix_operator_is_preserved(self, db):
        _seed_corpus(db)
        titles = {row["title"] for row in db.search_ranked("fuzz*")}
        assert "Fuzzing the Kernel" in titles

    def test_author_hits_match(self, db):
        _seed_corpus(db)
        results = db.search_ranked("Alice")
        assert [row["paper_id"] for row in results] == ["4"]
