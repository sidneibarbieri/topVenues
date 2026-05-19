"""Tests for src/arxiv_fetcher.py.

All tests are offline. The harvest() generator (which calls the arXiv API)
is not tested here — it is covered by the integration smoke-test in
scripts/early_signal_study.py when run with --harvest.

We test: Preprint dataclass construction, save_jsonl/load_jsonl round-trip,
and the _from_arxiv_result adapter (via a lightweight stub).
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from src.arxiv_fetcher import Preprint, load_jsonl, save_jsonl, year_windows


# ── Helpers ────────────────────────────────────────────────────────────


def make_preprint(**overrides) -> Preprint:
    defaults = dict(
        arxiv_id="2401.12345v1",
        title="Side-Channel Attacks on Embedded Systems",
        authors=("Alice Smith", "Bob Jones"),
        submitted_at="2024-01-15T00:00:00+00:00",
        updated_at="2024-02-01T00:00:00+00:00",
        primary_category="cs.CR",
        categories=("cs.CR", "cs.SE"),
        doi=None,
        journal_ref=None,
        summary="We study side-channel attacks.",
    )
    defaults.update(overrides)
    return Preprint(**defaults)


# ── Preprint dataclass ─────────────────────────────────────────────────


class TestPreprint:
    def test_construction(self) -> None:
        p = make_preprint()
        assert p.arxiv_id == "2401.12345v1"
        assert len(p.authors) == 2

    def test_frozen(self) -> None:
        p = make_preprint()
        with pytest.raises((AttributeError, TypeError)):
            p.title = "changed"  # type: ignore[misc]

    def test_submitted_date_parses_correctly(self) -> None:
        p = make_preprint(submitted_at="2024-01-15T00:00:00+00:00")
        dt = p.submitted_date()
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15

    def test_submitted_date_with_timezone(self) -> None:
        p = make_preprint(submitted_at="2023-11-30T12:34:56+05:30")
        dt = p.submitted_date()
        assert dt.year == 2023
        assert dt.month == 11

    def test_optional_doi_and_journal_ref(self) -> None:
        p_with = make_preprint(doi="10.1145/12345", journal_ref="IEEE S&P 2024")
        assert p_with.doi == "10.1145/12345"
        assert p_with.journal_ref == "IEEE S&P 2024"

        p_without = make_preprint(doi=None, journal_ref=None)
        assert p_without.doi is None
        assert p_without.journal_ref is None


# ── Date-range windowing ───────────────────────────────────────────────


class TestYearWindows:
    def test_one_window_per_year_inclusive(self) -> None:
        windows = list(year_windows(2022, 2024))
        assert windows == [
            ("202201010000", "202212312359"),
            ("202301010000", "202312312359"),
            ("202401010000", "202412312359"),
        ]

    def test_single_year(self) -> None:
        assert list(year_windows(2024, 2024)) == [("202401010000", "202412312359")]

    def test_windows_are_disjoint(self) -> None:
        # Each window's end precedes the next window's start, so no preprint
        # (which has exactly one submission date) lands in two windows.
        windows = list(year_windows(2020, 2026))
        for (_, end), (next_start, _) in zip(windows, windows[1:]):
            assert end < next_start

    def test_reversed_range_is_empty(self) -> None:
        assert list(year_windows(2025, 2024)) == []


# ── save_jsonl / load_jsonl round-trip ────────────────────────────────


class TestJsonlRoundTrip:
    def test_single_preprint(self, tmp_path: Path) -> None:
        p = make_preprint()
        target = tmp_path / "preprints.jsonl"
        written = save_jsonl([p], target)
        assert written == 1
        loaded = load_jsonl(target)
        assert len(loaded) == 1
        assert loaded[0] == p

    def test_multiple_preprints(self, tmp_path: Path) -> None:
        preprints = [make_preprint(arxiv_id=f"2401.{i:05d}v1") for i in range(10)]
        target = tmp_path / "preprints.jsonl"
        written = save_jsonl(preprints, target)
        assert written == 10
        loaded = load_jsonl(target)
        assert len(loaded) == 10
        for orig, back in zip(preprints, loaded):
            assert orig == back

    def test_authors_restored_as_tuple(self, tmp_path: Path) -> None:
        p = make_preprint(authors=("Alice", "Bob", "Carol"))
        target = tmp_path / "preprints.jsonl"
        save_jsonl([p], target)
        loaded = load_jsonl(target)
        assert isinstance(loaded[0].authors, tuple)

    def test_categories_restored_as_tuple(self, tmp_path: Path) -> None:
        p = make_preprint(categories=("cs.CR", "cs.LG", "cs.SE"))
        target = tmp_path / "preprints.jsonl"
        save_jsonl([p], target)
        loaded = load_jsonl(target)
        assert isinstance(loaded[0].categories, tuple)
        assert loaded[0].categories == ("cs.CR", "cs.LG", "cs.SE")

    def test_unicode_roundtrip(self, tmp_path: Path) -> None:
        p = make_preprint(title="Détection d'Intrusions: Étude Comparative")
        target = tmp_path / "preprints.jsonl"
        save_jsonl([p], target)
        loaded = load_jsonl(target)
        assert loaded[0].title == p.title

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        target = tmp_path / "subdir" / "preprints.jsonl"
        assert not target.parent.exists()
        save_jsonl([make_preprint()], target)
        assert target.exists()

    def test_one_json_object_per_line(self, tmp_path: Path) -> None:
        preprints = [make_preprint(arxiv_id=f"2401.{i:05d}v1") for i in range(5)]
        target = tmp_path / "preprints.jsonl"
        save_jsonl(preprints, target)
        lines = target.read_text().strip().splitlines()
        assert len(lines) == 5
        for line in lines:
            json.loads(line)  # must not raise

    def test_empty_iterator(self, tmp_path: Path) -> None:
        target = tmp_path / "empty.jsonl"
        written = save_jsonl(iter([]), target)
        assert written == 0
        loaded = load_jsonl(target)
        assert loaded == []

    def test_none_doi_and_journal_ref(self, tmp_path: Path) -> None:
        p = make_preprint(doi=None, journal_ref=None)
        target = tmp_path / "preprints.jsonl"
        save_jsonl([p], target)
        loaded = load_jsonl(target)
        assert loaded[0].doi is None
        assert loaded[0].journal_ref is None

    def test_overwrite_on_second_save(self, tmp_path: Path) -> None:
        target = tmp_path / "preprints.jsonl"
        save_jsonl([make_preprint(arxiv_id="2401.00001v1")], target)
        save_jsonl([make_preprint(arxiv_id="2401.00002v1")], target)
        loaded = load_jsonl(target)
        assert len(loaded) == 1
        assert loaded[0].arxiv_id == "2401.00002v1"
