"""The USENIX capture must become award records without labeling a paper twice."""

from __future__ import annotations

from scripts.import_usenix_awards import CapturedAward, load_capture, new_records, to_record
from src.awards import CONFERENCE_TO_CORPUS_VENUES, AwardRecord

LISTING = "https://www.usenix.org/conferences/best-papers?taxonomy_vocabulary_1_tid=2020&title_1="


def _captured(event: str = "WOOT", title: str = "A Paper") -> CapturedAward:
    return CapturedAward(event, 2020, title, "https://www.usenix.org/x", "Best Paper Award")


def test_woot_awards_join_through_both_dblp_spellings() -> None:
    record = to_record(_captured())
    assert record.venue == "USENIX WOOT"
    assert record.source_url == LISTING
    assert "WOOT @ USENIX Security Symposium" in CONFERENCE_TO_CORPUS_VENUES[record.venue]


def test_an_award_another_table_holds_is_not_added_again() -> None:
    held = AwardRecord("USENIX Security", 2020, "Distinguished Paper Award", "a paper", None, "x")
    captured = [_captured("USENIX Security", "A Paper!"), _captured("USENIX Security", "Other")]
    assert [record.title for record in new_records(captured, [held])] == ["Other"]


def test_every_captured_row_names_its_award_and_a_known_event() -> None:
    captured = load_capture()
    assert len(captured) == 98
    assert {award.event for award in captured} == {"USENIX Security", "WOOT"}
    assert all(award.award.endswith("Award") for award in captured)
