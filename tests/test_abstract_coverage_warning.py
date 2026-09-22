"""An abstract query cannot speak for venues whose abstracts were never stored."""

import sqlite3

import pytest

from web.app import ABSTRACT_COVERAGE_FLOOR, _abstract_coverage_by_venue


@pytest.fixture
def snapshot(tmp_path):
    path = tmp_path / "papers.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE papers (event TEXT, abstract TEXT)")
        # Full coverage.
        conn.executemany("INSERT INTO papers VALUES (?, ?)", [("ACM CCS", "text")] * 10)
        # A venue indexed bibliographically but never enriched.
        conn.executemany("INSERT INTO papers VALUES (?, ?)", [("ESORICS", None)] * 9)
        conn.execute("INSERT INTO papers VALUES ('ESORICS', 'text')")
        # Empty string must count as missing, not as present.
        conn.execute("INSERT INTO papers VALUES ('RAID', '   ')")
        conn.execute("INSERT INTO papers VALUES ('RAID', 'text')")
    return str(path)


def test_coverage_counts_blank_abstracts_as_missing(snapshot):
    coverage = _abstract_coverage_by_venue.__wrapped__(snapshot)
    assert coverage["ACM CCS"] == (10, 10)
    assert coverage["ESORICS"] == (1, 10)
    assert coverage["RAID"] == (1, 2)


def test_only_under_covered_venues_are_flagged(snapshot):
    coverage = _abstract_coverage_by_venue.__wrapped__(snapshot)
    flagged = {
        venue for venue, (hit, total) in coverage.items() if hit / total < ABSTRACT_COVERAGE_FLOOR
    }
    assert flagged == {"ESORICS", "RAID"}
    assert "ACM CCS" not in flagged
