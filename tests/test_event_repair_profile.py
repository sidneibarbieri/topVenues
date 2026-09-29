"""The event-repaired successor must relabel the declared records and change nothing else."""

from __future__ import annotations

import sqlite3

import pytest

from scripts.build_event_repaired_profile import (
    REPAIR_LOG,
    SOURCE_PROFILE,
    TARGET_PROFILE,
    EventRepairLog,
    derive_repairs,
)
from src.profiles import verified_profile_snapshot
from tests.repository_only import skip_unless_repository

skip_unless_repository()


def _rows(profile_id: str) -> dict[str, tuple]:
    with verified_profile_snapshot(profile_id) as verified:
        connection = sqlite3.connect(verified.database_path)
        try:
            columns = [row[1] for row in connection.execute("PRAGMA table_info(papers)")]
            select = ", ".join(columns)
            return {
                str(row[columns.index("paper_id")]): dict(zip(columns, row, strict=True))
                for row in connection.execute(f"SELECT {select} FROM papers")
            }
        finally:
            connection.close()


@pytest.fixture(scope="module")
def repair_log() -> EventRepairLog:
    return EventRepairLog.model_validate_json(REPAIR_LOG.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def source_rows() -> dict[str, dict]:
    return _rows(SOURCE_PROFILE)


@pytest.fixture(scope="module")
def successor_rows() -> dict[str, dict]:
    return _rows(TARGET_PROFILE)


def test_the_log_names_only_the_colocated_woot_papers(repair_log):
    assert len(repair_log.repairs) == 49
    assert {repair.venue for repair in repair_log.repairs} == {"WOOT @ USENIX Security Symposium"}
    assert {(r.stored_event, r.corrected_event) for r in repair_log.repairs} == {
        ("USENIX Security", "USENIX WOOT")
    }


def test_only_the_declared_events_change(repair_log, source_rows, successor_rows):
    corrected = {repair.paper_id: repair.corrected_event for repair in repair_log.repairs}
    assert source_rows.keys() == successor_rows.keys()
    for paper_id, source in source_rows.items():
        expected = {**source, "event": corrected.get(paper_id, source["event"])}
        assert successor_rows[paper_id] == expected


def test_the_normalizer_agrees_with_every_successor_event():
    with verified_profile_snapshot(TARGET_PROFILE) as verified:
        connection = sqlite3.connect(verified.database_path)
        try:
            assert derive_repairs(connection) == ()
        finally:
            connection.close()
