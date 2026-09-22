"""Authorship shifts must describe byline movement, not infer seniority."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.research_intelligence import authorship_shifts


@pytest.fixture()
def corpus(tmp_path: Path) -> Path:
    db_path = tmp_path / "papers.db"
    connection = sqlite3.connect(db_path)
    connection.execute(
        "CREATE TABLE papers (paper_id TEXT, authors TEXT, event TEXT, year INTEGER,"
        " title TEXT, abstract TEXT)"
    )
    rows = [
        # Moved from first author to last author: a new group becoming visible.
        ("1", "Ada Lovelace, Senior One", "ACM CCS", 2019),
        ("2", "Ada Lovelace, Senior One", "NDSS", 2020),
        ("3", "Junior A, Ada Lovelace", "ACM CCS", 2024),
        ("4", "Junior B, Ada Lovelace", "NDSS", 2025),
        ("5", "Junior C, Ada Lovelace", "IEEE S&P", 2026),
        # Always last author: established, must not be reported as a shift.
        ("6", "Junior D, Grace Hopper", "ACM CCS", 2019),
        ("7", "Junior E, Grace Hopper", "NDSS", 2020),
        ("8", "Junior F, Grace Hopper", "ACM CCS", 2024),
        ("9", "Junior G, Grace Hopper", "NDSS", 2025),
        ("10", "Junior H, Grace Hopper", "IEEE S&P", 2026),
    ]
    connection.executemany(
        "INSERT INTO papers VALUES (?, ?, ?, ?, '', '')",
        [(paper_id, authors, event, year) for paper_id, authors, event, year in rows],
    )
    connection.commit()
    connection.close()
    return db_path


def test_an_author_who_moved_to_the_last_position_is_reported(corpus):
    shifts = {shift.author: shift for shift in authorship_shifts(corpus)}
    assert "Ada Lovelace" in shifts
    reported = shifts["Ada Lovelace"]
    assert reported.early_first == 2
    assert reported.recent_last == 3


def test_an_author_who_always_led_is_not_reported_as_a_shift(corpus):
    """Otherwise the list fills with established names and answers nothing."""
    assert "Grace Hopper" not in {shift.author for shift in authorship_shifts(corpus)}


def test_the_windows_are_reported_so_the_reader_can_check_them(corpus):
    shift = next(s for s in authorship_shifts(corpus) if s.author == "Ada Lovelace")
    assert "–" in shift.early_window and "–" in shift.recent_window


def test_venues_name_where_the_author_now_leads(corpus):
    shift = next(s for s in authorship_shifts(corpus) if s.author == "Ada Lovelace")
    assert set(shift.venues) <= {"ACM CCS", "NDSS", "IEEE S&P"}
    assert shift.venues


def test_an_empty_corpus_returns_nothing_instead_of_failing(tmp_path):
    db_path = tmp_path / "empty.db"
    connection = sqlite3.connect(db_path)
    connection.execute(
        "CREATE TABLE papers (paper_id TEXT, authors TEXT, event TEXT, year INTEGER,"
        " title TEXT, abstract TEXT)"
    )
    connection.commit()
    connection.close()
    assert authorship_shifts(db_path) == []
