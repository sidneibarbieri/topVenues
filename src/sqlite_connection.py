"""Deterministic SQLite connection lifecycle."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


@contextmanager
def managed_sqlite_connection(
    database: str | Path,
    **connect_options: Any,
) -> Iterator[sqlite3.Connection]:
    """Open a transactional connection and always close its file handle."""
    connection = sqlite3.connect(database, **connect_options)
    try:
        with connection:
            yield connection
    finally:
        connection.close()
