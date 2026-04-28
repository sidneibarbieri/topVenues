"""Tests for DatabaseManager."""

import sqlite3

import pandas as pd
import pytest

from src.database import DatabaseManager
from src.models import Paper


@pytest.fixture
def db(tmp_path):
    return DatabaseManager(tmp_path / "papers.db")


def _seed(db: DatabaseManager, papers: list[Paper]) -> None:
    db.upsert_papers(papers)


def _paper(paper_id: str, **overrides) -> Paper:
    base = {
        "paper_id": paper_id,
        "title": f"Title {paper_id}",
        "year": 2024,
        "event": "ACM CCS",
    }
    base.update(overrides)
    return Paper(**base)


def _csv_with_abstracts(path, rows: list[tuple[str, str | None]]) -> None:
    """Write a legacy-shaped CSV with ID and Abstract columns at minimum."""
    df = pd.DataFrame(
        [{"ID": pid, "Abstract": abs_text, "Title": "x", "Year": 2024} for pid, abs_text in rows]
    )
    df.to_csv(path, index=False, encoding="utf-8")


class TestImportAbstractsFromCsv:
    def test_fills_empty_abstracts(self, db, tmp_path):
        _seed(db, [_paper("1"), _paper("2")])
        csv = tmp_path / "old.csv"
        _csv_with_abstracts(csv, [("1", "A" * 200), ("2", "B" * 200)])

        result = db.import_abstracts_from_csv(csv)

        assert result.scanned == 2
        assert result.matched == 2
        assert result.updated == 2
        assert result.skipped_existing == 0
        assert result.missing_in_db == 0

        with sqlite3.connect(db.db_path) as conn:
            rows = conn.execute("SELECT paper_id, abstract FROM papers ORDER BY paper_id").fetchall()
        assert rows == [("1", "A" * 200), ("2", "B" * 200)]

    def test_never_overwrites_existing_abstract(self, db, tmp_path):
        _seed(db, [_paper("1", abstract="ORIGINAL " * 20)])
        csv = tmp_path / "old.csv"
        _csv_with_abstracts(csv, [("1", "REPLACEMENT " * 20)])

        result = db.import_abstracts_from_csv(csv)

        assert result.scanned == 1
        assert result.matched == 1
        assert result.updated == 0
        assert result.skipped_existing == 1
        assert result.missing_in_db == 0

        with sqlite3.connect(db.db_path) as conn:
            (current,) = conn.execute("SELECT abstract FROM papers WHERE paper_id = '1'").fetchone()
        assert current.startswith("ORIGINAL")

    def test_ignores_short_abstracts(self, db, tmp_path):
        _seed(db, [_paper("1")])
        csv = tmp_path / "old.csv"
        _csv_with_abstracts(csv, [("1", "too short")])

        result = db.import_abstracts_from_csv(csv)

        assert result.scanned == 0
        assert result.updated == 0

    def test_counts_missing_in_db(self, db, tmp_path):
        _seed(db, [_paper("1")])
        csv = tmp_path / "old.csv"
        _csv_with_abstracts(csv, [("1", "X" * 200), ("999", "Y" * 200)])

        result = db.import_abstracts_from_csv(csv)

        assert result.scanned == 2
        assert result.matched == 1
        assert result.updated == 1
        assert result.missing_in_db == 1

    def test_idempotent_second_run_is_noop(self, db, tmp_path):
        _seed(db, [_paper("1")])
        csv = tmp_path / "old.csv"
        _csv_with_abstracts(csv, [("1", "Z" * 200)])

        first = db.import_abstracts_from_csv(csv)
        second = db.import_abstracts_from_csv(csv)

        assert first.updated == 1
        assert second.updated == 0
        assert second.skipped_existing == 1

    def test_missing_file_raises(self, db, tmp_path):
        with pytest.raises(FileNotFoundError):
            db.import_abstracts_from_csv(tmp_path / "does_not_exist.csv")
