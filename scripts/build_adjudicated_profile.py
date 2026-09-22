#!/usr/bin/env python3
"""Build the declared successor from a frozen source and adjudication log."""

from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profiles import verified_profile_snapshot  # noqa: E402
from src.sqlite_connection import managed_sqlite_connection  # noqa: E402

ADJUDICATION_PATH = ROOT / "data" / "adjudication" / "security-20-v3-identity.json"


def _write_gzip_snapshot(source: Path, target: Path) -> None:
    with (
        source.open("rb") as input_file,
        target.open("wb") as output_file,
        gzip.GzipFile(fileobj=output_file, mode="wb", compresslevel=9, mtime=0) as archive,
    ):
        shutil.copyfileobj(input_file, archive, length=1024 * 1024)


def _load_adjudication() -> dict:
    return json.loads(ADJUDICATION_PATH.read_text(encoding="utf-8"))


def _require_paper_ids(connection: sqlite3.Connection, paper_ids: set[str]) -> None:
    placeholders = ",".join("?" for _ in paper_ids)
    existing = {
        row[0]
        for row in connection.execute(
            f"SELECT paper_id FROM papers WHERE paper_id IN ({placeholders})", tuple(paper_ids)
        )
    }
    missing = paper_ids - existing
    if missing:
        raise RuntimeError(f"adjudication refers to missing paper IDs: {sorted(missing)}")


def main() -> None:
    adjudication = _load_adjudication()
    source_profile = adjudication["source_profile"]
    target_profile = adjudication["target_profile"]
    target_snapshot = ROOT / "data" / "profiles" / target_profile / "papers.db.gz"
    if target_snapshot.exists():
        raise FileExistsError(f"refusing to overwrite immutable snapshot: {target_snapshot}")
    target_snapshot.parent.mkdir(parents=True, exist_ok=False)

    merge_decisions = [
        decision for decision in adjudication["decisions"] if decision["decision"] == "merge_alias"
    ]
    declared_years = set(adjudication["strict_year_window"])

    with (
        verified_profile_snapshot(source_profile, ROOT) as source,
        tempfile.TemporaryDirectory(prefix="topvenues-adjudicate-") as directory,
    ):
        database_path = Path(directory) / "papers.db"
        shutil.copyfile(source.database_path, database_path)
        with managed_sqlite_connection(database_path) as connection:
            initial_count = connection.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
            adjudicated_ids = {
                paper_id
                for decision in merge_decisions
                for paper_id in [decision["canonical_paper_id"], *decision["removed_paper_ids"]]
            }
            _require_paper_ids(connection, adjudicated_ids)
            removed_aliases = [
                paper_id
                for decision in merge_decisions
                for paper_id in decision["removed_paper_ids"]
            ]
            connection.executemany(
                "DELETE FROM papers WHERE paper_id = ?",
                [(paper_id,) for paper_id in removed_aliases],
            )
            placeholders = ",".join("?" for _ in declared_years)
            outside_window = connection.execute(
                f"SELECT paper_id FROM papers WHERE year NOT IN ({placeholders}) OR year IS NULL",
                tuple(sorted(declared_years)),
            ).fetchall()
            connection.execute(
                f"DELETE FROM papers WHERE year NOT IN ({placeholders}) OR year IS NULL",
                tuple(sorted(declared_years)),
            )
            expected_count = initial_count - len(removed_aliases) - len(outside_window)
            actual_count = connection.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
            if actual_count != expected_count:
                raise RuntimeError(f"expected {expected_count} records, found {actual_count}")
        with managed_sqlite_connection(database_path) as connection:
            connection.execute("VACUUM")
        _write_gzip_snapshot(database_path, target_snapshot)

    print(
        f"Built {target_profile}: {actual_count:,} records; removed "
        f"{len(removed_aliases)} confirmed aliases and {len(outside_window)} out-of-window records."
    )


if __name__ == "__main__":
    main()
