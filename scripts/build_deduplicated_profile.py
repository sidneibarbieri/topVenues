#!/usr/bin/env python3
"""Build a successor snapshot by merging exact duplicate resource locators.

The source profile remains untouched.  The target is a separately named,
immutable profile whose manifest records the exact number of merged records.
"""

from __future__ import annotations

import gzip
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.deduplication import deduplicate_papers  # noqa: E402
from src.models import Paper  # noqa: E402
from src.profiles import verified_profile_snapshot  # noqa: E402
from src.sqlite_connection import managed_sqlite_connection  # noqa: E402

SOURCE_PROFILE = "security-20"
TARGET_PROFILE = "security-20-v2"


def _write_gzip_snapshot(source: Path, target: Path) -> None:
    with (
        source.open("rb") as input_file,
        target.open("wb") as output_file,
        gzip.GzipFile(fileobj=output_file, mode="wb", compresslevel=9, mtime=0) as archive,
    ):
        shutil.copyfileobj(input_file, archive, length=1024 * 1024)


def main() -> None:
    target_snapshot = ROOT / "data" / "profiles" / TARGET_PROFILE / "papers.db.gz"
    if target_snapshot.exists():
        raise FileExistsError(
            f"refusing to overwrite immutable profile {TARGET_PROFILE!r}: {target_snapshot}. "
            "Declare a new target profile and manifest for a successor release."
        )
    target_snapshot.parent.mkdir(parents=True, exist_ok=True)

    with (
        verified_profile_snapshot(SOURCE_PROFILE, ROOT) as source,
        tempfile.TemporaryDirectory(prefix="topvenues-deduplicate-") as directory,
    ):
        database_path = Path(directory) / "papers.db"
        shutil.copyfile(source.database_path, database_path)
        with managed_sqlite_connection(database_path) as connection:
            connection.row_factory = sqlite3.Row
            records = [Paper(**dict(row)) for row in connection.execute("SELECT * FROM papers")]
            merged, report = deduplicate_papers(records)
            retained_ids = {paper.paper_id for paper in merged}
            for paper in merged:
                connection.execute(
                    "UPDATE papers SET abstract = ?, bibtex = ? WHERE paper_id = ?",
                    (paper.abstract, paper.bibtex, paper.paper_id),
                )
            connection.executemany(
                "DELETE FROM papers WHERE paper_id = ?",
                [
                    (paper_id,)
                    for paper_id in {record.paper_id for record in records} - retained_ids
                ],
            )
        with managed_sqlite_connection(database_path) as connection:
            connection.execute("VACUUM")
        _write_gzip_snapshot(database_path, target_snapshot)

    print(
        f"Built {TARGET_PROFILE}: {report.input_records:,} input records, "
        f"{report.output_records:,} retained, {report.merged_records:,} merged "
        f"across {report.groups_merged:,} exact-resource groups."
    )


if __name__ == "__main__":
    main()
