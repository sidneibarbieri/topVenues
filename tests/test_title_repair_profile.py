"""The title-repaired successor must fix titles and change nothing else."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from src.profiles import verified_profile_snapshot

ROOT = Path(__file__).resolve().parent.parent
REPAIR_LOG = ROOT / "data" / "adjudication" / "security-20-v4-titles.json"


@pytest.fixture(scope="module")
def repair_log() -> dict:
    return json.loads(REPAIR_LOG.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def successor_rows() -> dict[str, str]:
    with verified_profile_snapshot("security-20-v4") as verified:
        connection = sqlite3.connect(verified.database_path)
        try:
            return {
                str(paper_id): title
                for paper_id, title in connection.execute("SELECT paper_id, title FROM papers")
            }
        finally:
            connection.close()


def test_every_declared_repair_is_applied(repair_log, successor_rows):
    for repair in repair_log["repairs"]:
        assert successor_rows[repair["paper_id"]] == repair["corrected_title"]


def test_no_repaired_title_remains_a_bare_prefix(repair_log, successor_rows):
    """A truncated title is a prefix of the real one; none may survive."""
    for repair in repair_log["repairs"]:
        assert successor_rows[repair["paper_id"]] != repair["stored_title"]


def test_record_count_is_unchanged(successor_rows):
    with verified_profile_snapshot("security-20-v3") as source:
        connection = sqlite3.connect(source.database_path)
        try:
            source_count = connection.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        finally:
            connection.close()
    assert len(successor_rows) == source_count


def test_documented_short_title_is_left_alone(repair_log, successor_rows):
    """ZeroAUDIT is short but correct; repairing it would introduce an error."""
    for entry in repair_log["not_repaired"]:
        assert successor_rows[entry["paper_id"]] == entry["title"]
