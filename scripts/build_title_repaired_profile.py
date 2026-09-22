#!/usr/bin/env python3
"""Build the title-repaired successor from a frozen source and a repair log.

DBLP marks titles up with inline elements, and reading only the first text node
stored a prefix: "D3FL: Label-Free ..." was kept as "D". Ten records in
security-20-v3 are affected, and in any title-ordered view they read as
duplicates of one another.

The repair is declared, not inferred. Every corrected title is listed in
data/adjudication/security-20-v4-titles.json with the stored value, the
corrected value and the DBLP record it came from, so the successor can be
rebuilt and checked without trusting this script.

Titles are the only field touched: no record is added, removed, merged or
re-scoped, so every count in the source profile is preserved by construction.
"""

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

REPAIR_LOG = ROOT / "data" / "adjudication" / "security-20-v4-titles.json"
SOURCE_PROFILE = "security-20-v3"
SUCCESSOR_PROFILE = "security-20-v4"


def _write_gzip_snapshot(source: Path, target: Path) -> None:
    """Write a byte-stable gzip so the digest depends only on the content."""
    with (
        source.open("rb") as input_file,
        target.open("wb") as output_file,
        gzip.GzipFile(fileobj=output_file, mode="wb", compresslevel=9, mtime=0) as archive,
    ):
        shutil.copyfileobj(input_file, archive, length=1024 * 1024)


def _apply_repairs(connection: sqlite3.Connection, repairs: list[dict]) -> None:
    """Set each declared title, failing loudly if the stored value moved."""
    for repair in repairs:
        paper_id = repair["paper_id"]
        row = connection.execute(
            "SELECT title FROM papers WHERE paper_id = ?", (paper_id,)
        ).fetchone()
        if row is None:
            raise SystemExit(f"repair log names {paper_id}, absent from {SOURCE_PROFILE}")
        if row[0] != repair["stored_title"]:
            raise SystemExit(
                f"{paper_id}: source holds {row[0]!r}, log expected "
                f"{repair['stored_title']!r}; the log is stale"
            )
        connection.execute(
            "UPDATE papers SET title = ? WHERE paper_id = ?",
            (repair["corrected_title"], paper_id),
        )


def _snapshot_counts(connection: sqlite3.Connection) -> dict[str, int]:
    scalar = lambda sql: connection.execute(sql).fetchone()[0]  # noqa: E731
    has_abstract = "abstract IS NOT NULL AND TRIM(abstract) <> ''"
    has_bibtex = "bibtex IS NOT NULL AND TRIM(bibtex) <> ''"
    return {
        "papers": scalar("SELECT COUNT(*) FROM papers"),
        "abstracts": scalar(f"SELECT COUNT(*) FROM papers WHERE {has_abstract}"),
        "bibtex": scalar(f"SELECT COUNT(*) FROM papers WHERE {has_bibtex}"),
        "venues": scalar("SELECT COUNT(DISTINCT event) FROM papers"),
    }


def main() -> int:
    log = json.loads(REPAIR_LOG.read_text(encoding="utf-8"))
    repairs = log["repairs"]
    target_root = ROOT / "data" / "profiles" / SUCCESSOR_PROFILE
    target_root.mkdir(parents=True, exist_ok=True)

    with (
        verified_profile_snapshot(SOURCE_PROFILE) as verified,
        tempfile.TemporaryDirectory(prefix="topvenues-v4-") as workspace,
    ):
        working_copy = Path(workspace) / "papers.db"
        shutil.copyfile(verified.database_path, working_copy)
        with managed_sqlite_connection(working_copy) as connection:
            before = _snapshot_counts(connection)
            _apply_repairs(connection, repairs)
            connection.commit()
            after = _snapshot_counts(connection)
        if before != after:
            raise SystemExit(f"counts changed: {before} -> {after}")
        _write_gzip_snapshot(working_copy, target_root / "papers.db.gz")

    print(f"repaired {len(repairs)} titles; counts unchanged: {after}")
    print(f"snapshot written to {target_root / 'papers.db.gz'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
