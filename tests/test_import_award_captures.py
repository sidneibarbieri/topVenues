"""Award captures must become award tables that are traceable, checkable and never doubled."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.import_award_captures import (
    CAPTURES_DIR,
    AwardCapture,
    CapturedAward,
    load_capture,
    new_records,
    output_table,
    titles_missing_from_pages,
)
from src.awards import CONFERENCE_TO_CORPUS_VENUES, AwardRecord, normalize_title

PAGE = "https://example.org/awards"


def _award(title: str, published_title: str | None = None) -> CapturedAward:
    return CapturedAward(
        venue="ACM CCS",
        year=2022,
        award="Distinguished Paper Award",
        title=title,
        url=PAGE,
        source_url=PAGE,
        published_title=published_title,
    )


def _capture(*awards: CapturedAward) -> AwardCapture:
    return AwardCapture(
        source="test", captured_on="2026-09-29", scripted_verification=True, method="test",
        records=awards,
    )  # fmt: skip


def test_an_award_another_table_holds_is_not_added_again() -> None:
    held = AwardRecord("ACM CCS", 2022, "Distinguished Paper Award", "a paper", None, "x")
    records = new_records(_capture(_award("A Paper!"), _award("Other")), [held])
    assert [record.title for record in records] == ["Other"]


def test_the_join_uses_the_proceedings_title_and_the_check_uses_the_page_wording() -> None:
    award = _award("T CHECKER : Precise Analysis", published_title="TChecker: Precise Analysis")
    assert award.to_record().title == "TChecker: Precise Analysis"
    page = normalize_title("Distinguished Paper Award T CHECKER : Precise Analysis Authors")
    assert titles_missing_from_pages(_capture(award), {PAGE: page}) == []


def test_a_title_absent_from_its_page_is_reported() -> None:
    capture = _capture(_award("Present Paper"), _award("Invented Paper"))
    missing = titles_missing_from_pages(capture, {PAGE: normalize_title("Present Paper")})
    assert [award.title for award in missing] == ["Invented Paper"]


def test_each_capture_writes_its_own_table() -> None:
    table = output_table(Path("captures/ndss_2019_2024.json"), Path("awards"))
    assert table == Path("awards/ndss_2019_2024_paper_awards.json")


@pytest.mark.parametrize("path", sorted(CAPTURES_DIR.glob("*.json")), ids=lambda path: path.stem)
def test_every_captured_award_is_dated_named_and_joinable(path: Path) -> None:
    capture = load_capture(path)
    assert capture.records
    assert all(
        award.award.endswith(("Award", "Runner-Up", "Honorable Mention"))
        for award in capture.records
    )
    assert {award.venue for award in capture.records} <= CONFERENCE_TO_CORPUS_VENUES.keys()
    keys = [(a.venue, a.year, normalize_title(a.title)) for a in capture.records]
    assert len(keys) == len(set(keys))
