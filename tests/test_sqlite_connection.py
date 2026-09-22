"""Cross-platform SQLite connection lifecycle tests."""

import sqlite3

import pytest

from src.sqlite_connection import managed_sqlite_connection


def test_connection_is_closed_after_success(tmp_path) -> None:
    database = tmp_path / "success.db"

    with managed_sqlite_connection(database) as connection:
        connection.execute("CREATE TABLE values_table (value INTEGER)")

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connection.execute("SELECT 1")
    database.unlink()


def test_connection_rolls_back_and_closes_after_failure(tmp_path) -> None:
    database = tmp_path / "failure.db"
    with managed_sqlite_connection(database) as connection:
        connection.execute("CREATE TABLE values_table (value INTEGER)")

    with (
        pytest.raises(RuntimeError, match="stop transaction"),
        managed_sqlite_connection(database) as connection,
    ):
        connection.execute("INSERT INTO values_table VALUES (1)")
        raise RuntimeError("stop transaction")

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connection.execute("SELECT 1")
    with managed_sqlite_connection(database) as verification_connection:
        count = verification_connection.execute("SELECT COUNT(*) FROM values_table").fetchone()[0]
    assert count == 0
    database.unlink()
