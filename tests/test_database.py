"""Tests for DatabaseManager."""

import gzip
import sqlite3

import pandas as pd
import pytest

from src.database import (
    DatabaseManager,
    bootstrap_from_gzipped_snapshot,
)
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


class TestBootstrapFromGzippedSnapshot:
    """The DB materialises itself transparently from a .gz snapshot."""

    def _seeded_db_bytes(self, tmp_path, paper_id: str = "42") -> bytes:
        source = tmp_path / f"_seed_{paper_id}.db"
        source_db = DatabaseManager(source)
        source_db.upsert_paper(_paper(paper_id, title=f"Seeded {paper_id}"))
        data = source.read_bytes()
        source.unlink()
        # Clean up the sidecar that DatabaseManager.__init__ may have written
        (source.parent / f"{source.name}.sync-id").unlink(missing_ok=True)
        return data

    def test_decompresses_when_db_missing(self, tmp_path):
        db_path = tmp_path / "papers.db"
        gz_path = tmp_path / "papers.db.gz"
        gz_path.write_bytes(gzip.compress(self._seeded_db_bytes(tmp_path)))
        bootstrap_from_gzipped_snapshot(db_path)
        assert db_path.exists()
        # A sync-id marker is now present so the next launch knows we're synced
        assert (tmp_path / "papers.db.sync-id").exists()
        rows = DatabaseManager(db_path).get_all_papers()
        assert any(r["paper_id"] == "42" for r in rows)

    def test_noop_when_no_snapshot(self, tmp_path):
        bootstrap_from_gzipped_snapshot(tmp_path / "papers.db")  # must not raise
        assert not (tmp_path / "papers.db").exists()

    def test_adopts_baseline_on_first_run_without_marker(self, tmp_path):
        """Existing setups with no marker get a quiet baseline write."""
        db_path = tmp_path / "papers.db"
        DatabaseManager(db_path)  # creates an empty DB locally
        gz_path = tmp_path / "papers.db.gz"
        gz_path.write_bytes(gzip.compress(self._seeded_db_bytes(tmp_path, "99")))
        bootstrap_from_gzipped_snapshot(db_path)
        # Marker now exists; local DB was NOT clobbered.
        assert (tmp_path / "papers.db.sync-id").exists()
        rows = DatabaseManager(db_path).get_all_papers()
        assert not any(r["paper_id"] == "99" for r in rows)

    def test_auto_refresh_when_upstream_changes_and_db_untouched(self, tmp_path):
        """Pure reader scenario — snapshot bumped, local DB pristine."""
        db_path = tmp_path / "papers.db"
        gz_path = tmp_path / "papers.db.gz"

        # Initial bootstrap: marker pins gz=A, db=A
        gz_path.write_bytes(gzip.compress(self._seeded_db_bytes(tmp_path, "old")))
        bootstrap_from_gzipped_snapshot(db_path)

        # Upstream publishes a new snapshot with a different paper
        import os
        import time
        new_bytes = gzip.compress(self._seeded_db_bytes(tmp_path, "new"))
        gz_path.write_bytes(new_bytes)
        future = time.time() + 60
        os.utime(gz_path, (future, future))

        bootstrap_from_gzipped_snapshot(db_path)

        rows = DatabaseManager(db_path).get_all_papers()
        assert any(r["paper_id"] == "new" for r in rows), "Auto-refresh should adopt upstream"

    def test_protects_local_modifications_when_both_changed(self, tmp_path, caplog):
        """Maintainer scenario — both gz and db drifted; never auto-clobber."""
        import logging
        import os
        import time
        db_path = tmp_path / "papers.db"
        gz_path = tmp_path / "papers.db.gz"

        gz_path.write_bytes(gzip.compress(self._seeded_db_bytes(tmp_path, "base")))
        bootstrap_from_gzipped_snapshot(db_path)

        # User modifies their local DB (adds a paper, touches mtime)
        DatabaseManager(db_path).upsert_paper(_paper("locallymade", title="Local work"))

        # And upstream also publishes a new snapshot
        gz_path.write_bytes(gzip.compress(self._seeded_db_bytes(tmp_path, "upstream")))
        future = time.time() + 60
        os.utime(gz_path, (future, future))

        with caplog.at_level(logging.WARNING):
            bootstrap_from_gzipped_snapshot(db_path)

        # Local modification preserved
        rows = DatabaseManager(db_path).get_all_papers()
        assert any(r["paper_id"] == "locallymade" for r in rows)
        assert not any(r["paper_id"] == "upstream" for r in rows)
        # User was warned
        assert any("modifications" in r.message for r in caplog.records)
