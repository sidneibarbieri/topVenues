"""Tests for src/readiness.py — the scientific-readiness triage filter.

All tests are offline and synthetic; they exercise the strict author key,
the prior-author set, the title-based outcome index, the result metrics
(precision, base rate, lift, recall, volume reduction), and the full
analyze() pipeline.
"""

from __future__ import annotations

import math

import pytest

from src.readiness import (
    OutcomeIndex,
    ReadinessResult,
    analyze,
    build_prior_author_set,
    strict_author_key,
)


# ── strict_author_key ───────────────────────────────────────────────────


class TestStrictAuthorKey:
    def test_full_name(self) -> None:
        assert strict_author_key("Alice Smith") == "alice smith"

    def test_drops_accents(self) -> None:
        assert strict_author_key("Álvaro García") == "alvaro garcia"

    def test_drops_dblp_suffix(self) -> None:
        assert strict_author_key("Wei Wang 0001") == "wei wang"

    def test_rejects_initial_only(self) -> None:
        # A single-letter given name is not discriminative enough.
        assert strict_author_key("A. Smith") == ""

    def test_rejects_single_token(self) -> None:
        assert strict_author_key("Madonna") == ""

    def test_empty(self) -> None:
        assert strict_author_key("") == ""


class TestBuildPriorAuthorSet:
    def test_collects_keys(self) -> None:
        prior = build_prior_author_set([["Alice Smith", "Bob Jones"], ["Carol Lee"]])
        assert prior == frozenset({"alice smith", "bob jones", "carol lee"})

    def test_skips_non_discriminative(self) -> None:
        prior = build_prior_author_set([["A. Smith", "Madonna", "Real Name"]])
        assert prior == frozenset({"real name"})

    def test_empty(self) -> None:
        assert build_prior_author_set([]) == frozenset()


# ── OutcomeIndex ─────────────────────────────────────────────────────────


class TestOutcomeIndex:
    def test_exact_title_is_published(self) -> None:
        idx = OutcomeIndex(["Cache Side-Channel Attacks on ARM"])
        assert idx.is_published("Cache Side-Channel Attacks on ARM", threshold=0.6)

    def test_near_title_above_threshold(self) -> None:
        idx = OutcomeIndex(["Fuzzing the Linux Kernel Efficiently"])
        assert idx.is_published("Fuzzing the Linux Kernel", threshold=0.5)

    def test_unrelated_title_not_published(self) -> None:
        idx = OutcomeIndex(["Quantum Cryptography Protocols"])
        assert not idx.is_published("Cache Side-Channel Attacks", threshold=0.5)

    def test_threshold_respected(self) -> None:
        idx = OutcomeIndex(["Cache Side-Channel Attacks on ARM Processors"])
        # Partial overlap clears a low bar but not a high one.
        assert idx.is_published("Cache Side-Channel Attacks", threshold=0.4)
        assert not idx.is_published("Cache Side-Channel Attacks", threshold=0.95)

    def test_empty_query_title(self) -> None:
        idx = OutcomeIndex(["Some Real Title Here"])
        assert not idx.is_published("", threshold=0.5)

    def test_empty_index(self) -> None:
        idx = OutcomeIndex([])
        assert not idx.is_published("Anything At All", threshold=0.1)

    def test_ignores_stopword_only_published_titles(self) -> None:
        # A title that tokenises to nothing must not crash index construction.
        idx = OutcomeIndex(["the of and", "Concrete Attack Title"])
        assert idx.is_published("Concrete Attack Title", threshold=0.6)


# ── ReadinessResult metrics ──────────────────────────────────────────────


class TestReadinessResult:
    def _result(self, nw=100, nwo=100, cw=15, cwo=1) -> ReadinessResult:
        return ReadinessResult(
            threshold=0.6,
            n_with_track_record=nw,
            n_without_track_record=nwo,
            converted_with=cw,
            converted_without=cwo,
        )

    def test_precision(self) -> None:
        assert self._result(nw=100, cw=15).precision == pytest.approx(0.15)

    def test_base_rate(self) -> None:
        assert self._result(nwo=100, cwo=1).base_rate == pytest.approx(0.01)

    def test_lift(self) -> None:
        # precision 0.15 / base 0.01 = 15x
        assert self._result(nw=100, cw=15, nwo=100, cwo=1).lift == pytest.approx(15.0)

    def test_recall(self) -> None:
        # captured 15 of 16 total conversions
        assert self._result(cw=15, cwo=1).recall == pytest.approx(15 / 16)

    def test_volume_reduction(self) -> None:
        # 100 of 200 preprints excluded by the filter
        assert self._result(nw=100, nwo=100).volume_reduction == pytest.approx(0.5)

    def test_infinite_lift_when_base_zero(self) -> None:
        assert math.isinf(self._result(cwo=0).lift)

    def test_zero_denominators_do_not_raise(self) -> None:
        empty = ReadinessResult(0.6, 0, 0, 0, 0)
        assert empty.precision == 0.0
        assert empty.base_rate == 0.0
        assert empty.recall == 0.0
        assert empty.volume_reduction == 0.0


# ── analyze() end-to-end ────────────────────────────────────────────────


class TestAnalyze:
    def test_partitions_and_counts(self) -> None:
        prior = build_prior_author_set([["Alice Smith"]])  # only Alice has track record
        outcome = OutcomeIndex([
            "Cache Side-Channel Attacks",       # Alice's preprint becomes top-4
            "Fuzzing the Linux Kernel",         # newcomer's preprint becomes top-4
        ])
        preprints = [
            ("Cache Side-Channel Attacks", ["Alice Smith"]),         # track record + converts
            ("An Unrelated Survey of Nothing", ["Alice Smith"]),     # track record, no convert
            ("Fuzzing the Linux Kernel", ["Newcomer Person"]),       # no record, converts
            ("Another Unrelated Topic Entirely", ["Newcomer Person"]),  # no record, no convert
        ]
        result = analyze(preprints, prior, outcome, threshold=0.6)
        assert result.n_with_track_record == 2
        assert result.n_without_track_record == 2
        assert result.converted_with == 1
        assert result.converted_without == 1
        assert result.precision == pytest.approx(0.5)
        assert result.base_rate == pytest.approx(0.5)

    def test_track_record_filter_shows_lift(self) -> None:
        # Track-record authors convert; others mostly do not.
        prior = build_prior_author_set([["Alice Smith"]])
        outcome = OutcomeIndex(["Cache Side-Channel Attacks"])
        preprints = (
            [("Cache Side-Channel Attacks", ["Alice Smith"])]            # converts
            + [(f"Random Unrelated Title {i} Foo", ["Other Author"]) for i in range(99)]
        )
        result = analyze(preprints, prior, outcome, threshold=0.6)
        assert result.precision == pytest.approx(1.0)
        assert result.base_rate == pytest.approx(0.0)
        assert math.isinf(result.lift)
        assert result.recall == pytest.approx(1.0)

    def test_empty_cohort(self) -> None:
        result = analyze([], frozenset(), OutcomeIndex([]), threshold=0.6)
        assert result.n_with_track_record == 0
        assert result.precision == 0.0
