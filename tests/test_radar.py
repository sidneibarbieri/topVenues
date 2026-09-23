"""The preprint radar: the measured rule, run forward, outside the corpus."""

from __future__ import annotations

import sqlite3

from src.radar import (
    FlaggedPreprint,
    PriorRecord,
    build_prior_index,
    flag,
    radar_path,
    snapshot,
    strict_author_key,
)


class _Preprint:
    """The shape the arXiv parser returns, reduced to what the rule reads."""

    def __init__(self, arxiv_id, title, authors, published="2026-09-01T00:00:00Z"):
        self.arxiv_id = arxiv_id
        self.title = title
        self.authors = authors
        self.abstract = "An abstract."
        self.published = published
        self.url = f"https://arxiv.org/abs/{arxiv_id}"


def _corpus(tmp_path, rows):
    database = tmp_path / "papers.db"
    connection = sqlite3.connect(database)
    connection.execute("create table papers (authors text, event text, year integer)")
    connection.executemany("insert into papers values (?, ?, ?)", rows)
    connection.commit()
    connection.close()
    return database


def test_strict_author_key_keeps_the_whole_given_name() -> None:
    assert strict_author_key("Jörg Schwenk") == "jorg schwenk"
    assert strict_author_key("Thorsten Holz") == "thorsten holz"


def test_strict_author_key_drops_names_too_short_to_discriminate() -> None:
    """An initial plus a surname would pool hundreds of people into one key."""
    assert strict_author_key("Y. Wang") == ""
    assert strict_author_key("Madonna") == ""


def test_the_index_only_counts_the_tracked_tier(tmp_path) -> None:
    database = _corpus(
        tmp_path,
        [
            ("Ada Lovelace, Alan Turing", "ACM CCS", 2024),
            ("Grace Hopper", "ACM WiSec", 2024),
        ],
    )
    index = build_prior_index(database, first_year=2022, last_year=2025)
    assert set(index) == {"ada lovelace", "alan turing"}
    assert index["ada lovelace"].venues == ["ACM CCS"]


def test_the_index_respects_the_window(tmp_path) -> None:
    """The window closes before the preprints, so the rule cannot read the outcome."""
    database = _corpus(tmp_path, [("Ada Lovelace", "NDSS", 2019)])
    assert build_prior_index(database, first_year=2022, last_year=2025) == {}


def test_flag_keeps_only_preprints_with_a_tracked_author() -> None:
    index = {
        "ada lovelace": PriorRecord(author="Ada Lovelace", papers=3, venues=["NDSS"], years=[2024])
    }
    kept = flag(
        [
            _Preprint("2609.1", "A flagged preprint", ["Ada Lovelace", "Someone Else"]),
            _Preprint("2609.2", "An unflagged preprint", ["Someone Else"]),
        ],
        index,
    )
    assert [item.arxiv_id for item in kept] == ["2609.1"]
    assert kept[0].prior_authors[0].papers == 3


def test_flag_orders_newest_first() -> None:
    index = {
        "ada lovelace": PriorRecord(author="Ada Lovelace", papers=1, venues=["NDSS"], years=[2024])
    }
    kept = flag(
        [
            _Preprint("old", "Older", ["Ada Lovelace"], published="2026-09-01T00:00:00Z"),
            _Preprint("new", "Newer", ["Ada Lovelace"], published="2026-09-20T00:00:00Z"),
        ],
        index,
    )
    assert [item.arxiv_id for item in kept] == ["new", "old"]


def test_the_snapshot_records_what_it_was_computed_against() -> None:
    result = snapshot(
        category="cs.CR",
        submitted_since="2026-09-01",
        profile_id="security-20-v5",
        corpus_fingerprint="2487ea98",
        considered=10,
        flagged=[],
    )
    assert result.profile_id == "security-20-v5"
    assert result.corpus_fingerprint == "2487ea98"
    assert result.flagged_share == 0.0


def test_a_flagged_preprint_round_trips_through_json() -> None:
    original = FlaggedPreprint(
        arxiv_id="2609.1",
        title="A flagged preprint",
        authors=["Ada Lovelace"],
        abstract="An abstract.",
        submitted="2026-09-20T00:00:00Z",
        url="https://arxiv.org/abs/2609.1",
        prior_authors=[PriorRecord(author="Ada Lovelace", papers=3, venues=["NDSS"], years=[2024])],
    )
    assert FlaggedPreprint.model_validate_json(original.model_dump_json()) == original
    assert original.strongest.papers == 3


def test_the_radar_lives_outside_every_profile(tmp_path) -> None:
    """A preprint is not a corpus record, so it is not stored with one."""
    path = radar_path(tmp_path)
    assert path.parent == tmp_path / "data" / "radar"
    assert "profiles" not in path.parts and "workspaces" not in path.parts
