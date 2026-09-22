"""Tests for award loading, title normalization, and corpus matching."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from src.awards import (
    CONFERENCE_TO_CORPUS_VENUES,
    AwardRecord,
    build_corpus_award_map,
    load_award_records,
    match_awards_to_corpus,
    normalize_title,
)


def _write_corpus(db_path, rows):
    connection = sqlite3.connect(db_path)
    connection.execute("create table papers (paper_id text, title text, venue text)")
    connection.executemany("insert into papers values (?, ?, ?)", rows)
    connection.commit()
    connection.close()


def test_normalize_title_folds_punctuation_case_and_accents() -> None:
    assert normalize_title("LeapFrog: The Rowhammer Attack.") == "leapfrog the rowhammer attack"
    assert normalize_title("Café  Façade—2") == "cafe facade 2"


def test_conference_mapping_covers_top4_and_acsac() -> None:
    for conference in ("IEEE S&P", "ACM CCS", "NDSS", "USENIX Security", "ACSAC"):
        assert conference in CONFERENCE_TO_CORPUS_VENUES


def test_load_award_records_reads_paper_award_tables(tmp_path: Path) -> None:
    table = tmp_path / "demo_paper_awards.json"
    table.write_text(
        json.dumps(
            [
                {
                    "venue": "NDSS",
                    "year": 2025,
                    "award": "Distinguished Paper Award",
                    "title": "A Title",
                    "url": None,
                    "source_url": "https://example/awards",
                }
            ]
        ),
        encoding="utf-8",
    )
    records = load_award_records(tmp_path)
    assert records == [
        AwardRecord(
            "NDSS", 2025, "Distinguished Paper Award", "A Title", None, "https://example/awards"
        )
    ]


def test_match_awards_to_corpus_matches_by_venue_and_title(tmp_path: Path) -> None:
    db_path = tmp_path / "papers.db"
    connection = sqlite3.connect(db_path)
    connection.execute("create table papers (paper_id text, title text, venue text)")
    connection.executemany(
        "insert into papers values (?, ?, ?)",
        [
            ("p1", "LeapFrog: The Rowhammer Attack.", "SP"),
            ("p2", "Some Unrelated Paper", "CCS"),
        ],
    )
    connection.commit()
    connection.close()

    awards = [
        AwardRecord(
            "IEEE S&P",
            2025,
            "Distinguished Paper Award",
            "LeapFrog: The Rowhammer Attack",
            None,
            "https://sp/awards",
        ),
        AwardRecord("NDSS", 2025, "Best Paper Award", "Not In Corpus", None, "https://ndss/awards"),
    ]
    matched, unmatched = match_awards_to_corpus(awards, db_path)

    assert [match.paper_id for match in matched] == ["p1"]
    assert [record.title for record in unmatched] == ["Not In Corpus"]


def test_build_corpus_award_map_labels_matched_papers(tmp_path: Path) -> None:
    db_path = tmp_path / "papers.db"
    _write_corpus(db_path, [("p1", "LeapFrog: The Rowhammer Attack.", "SP")])
    (tmp_path / "demo_paper_awards.json").write_text(
        json.dumps(
            [
                {
                    "venue": "IEEE S&P",
                    "year": 2025,
                    "award": "Distinguished Paper Award",
                    "title": "LeapFrog: The Rowhammer Attack",
                    "url": None,
                    "source_url": "https://sp/awards",
                }
            ]
        ),
        encoding="utf-8",
    )
    award_map = build_corpus_award_map(tmp_path, db_path)
    assert award_map == {"p1": ["Distinguished Paper Award (IEEE S&P 2025)"]}
