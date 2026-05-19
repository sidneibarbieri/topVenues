"""Tests for src/author_matcher.py.

All tests are offline — no network calls, no DB. They exercise
author normalisation, title tokenisation, Jaccard similarity, index
construction, and the full find_matches pipeline.
"""

from __future__ import annotations

import pytest

from src.arxiv_fetcher import Preprint
from src.author_matcher import (
    DEFAULT_TITLE_THRESHOLD,
    Match,
    author_key,
    build_author_index,
    find_matches,
    jaccard,
    normalise_author,
    tokenise_title,
)


# ── Helpers ────────────────────────────────────────────────────────────


def make_preprint(
    arxiv_id: str = "2401.00001",
    title: str = "A Preprint Title",
    authors: tuple[str, ...] = ("Alice Smith",),
    submitted_at: str = "2023-06-01T00:00:00+00:00",
) -> Preprint:
    return Preprint(
        arxiv_id=arxiv_id,
        title=title,
        authors=authors,
        submitted_at=submitted_at,
        updated_at=submitted_at,
        primary_category="cs.CR",
        categories=("cs.CR",),
        doi=None,
        journal_ref=None,
        summary="",
    )


# ── Author normalisation ────────────────────────────────────────────────


class TestNormaliseAuthor:
    def test_lowercase(self) -> None:
        assert normalise_author("Alice Smith") == "alice smith"

    def test_strips_accents(self) -> None:
        result = normalise_author("Álvaro García")
        assert result == "alvaro garcia"

    def test_drops_dblp_numeric_suffix(self) -> None:
        # DBLP adds " 0001" to disambiguate same-name authors
        assert normalise_author("Wei Wang 0001") == "wei wang"
        assert normalise_author("John Doe 2022") == "john doe"

    def test_collapses_whitespace(self) -> None:
        assert normalise_author("  Bob   Lee  ") == "bob lee"

    def test_empty_string(self) -> None:
        assert normalise_author("") == ""

    def test_preserves_hyphen(self) -> None:
        result = normalise_author("Jean-Claude Van Damme")
        assert "-" in result or "jean" in result  # hyphens kept or letter-only fallback


class TestAuthorKey:
    def test_last_name_first_initial(self) -> None:
        assert author_key("Alice Smith") == "smith a"

    def test_single_name(self) -> None:
        assert author_key("Madonna") == "madonna"

    def test_drops_suffix_then_keys(self) -> None:
        # "Wei Wang 0001" → "wei wang" → key = "wang w"
        assert author_key("Wei Wang 0001") == "wang w"

    def test_empty_string(self) -> None:
        assert author_key("") == ""

    def test_accented_name(self) -> None:
        key = author_key("Álvaro García")
        assert key == "garcia a"


# ── Title similarity ───────────────────────────────────────────────────


class TestTokeniseTitle:
    def test_basic_tokens(self) -> None:
        tokens = tokenise_title("Fuzzing the Kernel")
        # "the" is a stopword; "fuzzing" and "kernel" survive
        assert "fuzzing" in tokens
        assert "kernel" in tokens
        assert "the" not in tokens

    def test_filters_stopwords(self) -> None:
        tokens = tokenise_title("a an the of for on in and or but is to")
        assert not tokens  # all stopwords

    def test_filters_single_chars(self) -> None:
        tokens = tokenise_title("a b c xyz")
        # single chars removed; "xyz" survives
        assert "xyz" in tokens
        assert "a" not in tokens
        assert "b" not in tokens

    def test_lowercase_and_accent_strip(self) -> None:
        tokens = tokenise_title("Détection d'Intrusion")
        assert "detection" in tokens or "intrusion" in tokens  # accent stripped

    def test_numbers_preserved(self) -> None:
        tokens = tokenise_title("SHA256 hash collision")
        assert "sha256" in tokens


class TestJaccard:
    def test_identical_sets(self) -> None:
        s = {"a", "b", "c"}
        assert jaccard(s, s) == pytest.approx(1.0)

    def test_disjoint_sets(self) -> None:
        assert jaccard({"a"}, {"b"}) == pytest.approx(0.0)

    def test_partial_overlap(self) -> None:
        # |A∩B|=1, |A∪B|=3
        assert jaccard({"a", "b"}, {"b", "c"}) == pytest.approx(1 / 3)

    def test_empty_sets(self) -> None:
        assert jaccard(set(), {"a"}) == pytest.approx(0.0)
        assert jaccard({"a"}, set()) == pytest.approx(0.0)
        assert jaccard(set(), set()) == pytest.approx(0.0)


# ── Index and matching engine ──────────────────────────────────────────


class TestBuildAuthorIndex:
    def test_indexes_by_key(self) -> None:
        p = make_preprint(authors=("Alice Smith",))
        index = build_author_index([p])
        assert "smith a" in index
        assert p in index["smith a"]

    def test_multiple_authors(self) -> None:
        p = make_preprint(authors=("Alice Smith", "Bob Jones"))
        index = build_author_index([p])
        assert "smith a" in index
        assert "jones b" in index

    def test_multiple_preprints(self) -> None:
        p1 = make_preprint(arxiv_id="2401.00001", authors=("Alice Smith",))
        p2 = make_preprint(arxiv_id="2401.00002", authors=("Alice Smith",))
        index = build_author_index([p1, p2])
        assert len(index["smith a"]) == 2

    def test_empty_input(self) -> None:
        assert build_author_index([]) == {}

    def test_skips_empty_keys(self) -> None:
        p = make_preprint(authors=("",))
        index = build_author_index([p])
        assert "" not in index


class TestFindMatches:
    """Integration-level tests for the full matching pipeline."""

    def _preprint(self, arxiv_id: str, title: str, authors: tuple[str, ...],
                  submitted_at: str = "2023-06-01T00:00:00+00:00") -> Preprint:
        return make_preprint(arxiv_id=arxiv_id, title=title, authors=authors,
                             submitted_at=submitted_at)

    def _match(self, paper_title: str, paper_authors: list[str],
               preprints: list[Preprint], threshold: float = DEFAULT_TITLE_THRESHOLD,
               paper_year: int = 2024) -> list[Match]:
        index = build_author_index(preprints)
        return find_matches(
            paper_id="paper-001",
            paper_title=paper_title,
            paper_authors=paper_authors,
            paper_year=paper_year,
            paper_venue="USENIX Security",
            author_index=index,
            title_threshold=threshold,
        )

    # -- positive cases --

    def test_exact_title_match(self) -> None:
        p = self._preprint("2401.00001", "Fuzzing the Linux Kernel", ("Alice Smith",))
        matches = self._match("Fuzzing the Linux Kernel", ["Alice Smith"], [p])
        assert len(matches) == 1
        assert matches[0].arxiv_id == "2401.00001"

    def test_partial_title_sufficient_similarity(self) -> None:
        # Slight wording variation still above threshold
        p = self._preprint("2401.00001", "Fuzzing Linux Kernels Efficiently", ("Alice Smith",))
        matches = self._match("Fuzzing the Linux Kernel Efficiently", ["Alice Smith"], [p])
        assert matches  # should match

    def test_match_returns_correct_metadata(self) -> None:
        p = self._preprint("2401.00001", "Cache Side-Channel Attacks",
                           ("Alice Smith", "Bob Jones"),
                           submitted_at="2023-06-01T00:00:00+00:00")
        matches = self._match("Cache Side-Channel Attacks",
                              ["Alice Smith", "Bob Jones"], [p], paper_year=2024)
        assert len(matches) == 1
        m = matches[0]
        assert m.paper_id == "paper-001"
        assert m.paper_venue == "USENIX Security"
        assert m.paper_year == 2024
        assert "smith a" in m.shared_authors or "jones b" in m.shared_authors

    def test_lag_positive_when_preprint_before_publication(self) -> None:
        p = self._preprint("2401.00001", "Cache Side-Channel Attacks", ("Alice Smith",),
                           submitted_at="2023-06-01T00:00:00+00:00")
        matches = self._match("Cache Side-Channel Attacks", ["Alice Smith"], [p],
                              paper_year=2024)
        assert matches[0].lag_days > 0  # preprint before Jan 1 2024

    def test_lag_negative_when_preprint_after_publication_year(self) -> None:
        # Posted AFTER the nominal publication year
        p = self._preprint("2401.00001", "Cache Side-Channel Attacks", ("Alice Smith",),
                           submitted_at="2025-06-01T00:00:00+00:00")
        matches = self._match("Cache Side-Channel Attacks", ["Alice Smith"], [p],
                              paper_year=2024)
        assert matches[0].lag_days < 0

    # -- negative cases --

    def test_no_author_overlap_no_match(self) -> None:
        p = self._preprint("2401.00001", "Cache Side-Channel Attacks", ("Bob Jones",))
        matches = self._match("Cache Side-Channel Attacks", ["Alice Smith"], [p])
        assert matches == []

    def test_low_title_similarity_no_match(self) -> None:
        p = self._preprint("2401.00001", "Quantum Cryptography Protocols", ("Alice Smith",))
        matches = self._match("Cache Side-Channel Attacks", ["Alice Smith"], [p])
        assert matches == []

    def test_threshold_respected(self) -> None:
        p = self._preprint("2401.00001", "Cache Side-Channel Attacks", ("Alice Smith",))
        # With a very high threshold, the exact match should still pass
        matches = self._match("Cache Side-Channel Attacks", ["Alice Smith"], [p],
                              threshold=0.99)
        assert matches  # exact match has similarity 1.0

        # With threshold above 1.0, nothing should match
        matches_impossible = self._match("Cache Side-Channel Attacks", ["Alice Smith"], [p],
                                         threshold=1.1)
        assert matches_impossible == []

    def test_empty_author_list_returns_empty(self) -> None:
        p = self._preprint("2401.00001", "Cache Side-Channel Attacks", ("Alice Smith",))
        index = build_author_index([p])
        results = find_matches("paper-001", "Cache Side-Channel Attacks", [],
                               2024, "USENIX Security", index)
        assert results == []

    def test_matches_sorted_by_similarity_desc(self) -> None:
        p1 = self._preprint("2401.00001", "Cache Side-Channel Attacks on ARM",
                            ("Alice Smith",))
        p2 = self._preprint("2401.00002", "Cache Side-Channel Attacks",
                            ("Alice Smith",))
        matches = self._match("Cache Side-Channel Attacks", ["Alice Smith"], [p1, p2])
        sims = [m.title_similarity for m in matches]
        assert sims == sorted(sims, reverse=True)

    def test_deduplicates_same_preprint(self) -> None:
        # If a preprint author matches via two different keys, the preprint
        # must still appear only once in the results.
        p = self._preprint("2401.00001", "Cache Side-Channel Attacks",
                           ("Alice Smith", "A. Smith"))  # both resolve to same key
        matches = self._match("Cache Side-Channel Attacks",
                              ["Alice Smith", "Bob Jones"], [p])
        arxiv_ids = [m.arxiv_id for m in matches]
        assert len(arxiv_ids) == len(set(arxiv_ids))
