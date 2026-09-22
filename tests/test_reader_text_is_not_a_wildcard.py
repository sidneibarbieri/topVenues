"""Text a reader types is a literal, never a LIKE pattern.

`%` and `_` are SQL LIKE wildcards. Unescaped, a topic of `%` matched all
14,859 records and the trend chart reported that as the topic's volume and a
100% corpus share. A topic such as `use_after_free` matched anything shaped
like it. Both produce a number a reader would quote.
"""

from __future__ import annotations

import sqlite3

import pytest

from src.analytics import topic_trend
from src.sql_patterns import contains_pattern


@pytest.fixture
def corpus(tmp_path):
    db = tmp_path / "papers.db"
    connection = sqlite3.connect(db)
    connection.execute(
        "create table papers (paper_id text, title text, abstract text, "
        "authors text, event text, year integer)"
    )
    connection.executemany(
        "insert into papers values (?,?,?,?,?,?)",
        [
            ("p1", "Detecting use_after_free bugs", "", "A", "ACM CCS", 2024),
            ("p2", "Detecting useXafterYfree bugs", "", "A", "ACM CCS", 2024),
            ("p3", "Reaching 95% precision", "", "B", "NDSS", 2024),
            ("p4", "An unrelated paper", "", "C", "NDSS", 2024),
        ],
    )
    connection.commit()
    connection.close()
    return db


def test_a_bare_percent_does_not_match_the_whole_corpus(corpus):
    assert topic_trend(corpus, "%")["total"] == 1


def test_an_underscore_matches_only_a_real_underscore(corpus):
    assert topic_trend(corpus, "use_after_free")["total"] == 1


def test_the_pattern_escapes_wildcards_and_the_escape_character():
    assert contains_pattern("LLM") == "%LLM%"
    assert contains_pattern("%") == "%\\%%"
    assert contains_pattern("a_b") == "%a\\_b%"
    assert contains_pattern("a\\b") == "%a\\\\b%"


def test_search_filters_treat_wildcards_as_literals(corpus):
    from src.database import DatabaseManager

    database = DatabaseManager(corpus)
    assert len(database.search(title_contains="use_after_free")) == 1
    assert len(database.search(title_contains="%")) == 1
