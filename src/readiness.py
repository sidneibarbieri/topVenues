"""Scientific-readiness analysis: does author track record predict which
arXiv preprints become top-tier publications?

This module quantifies a *triage filter* for early-signal scientific
monitoring. Given the flood of arXiv ``cs.CR`` preprints (~7,000/year),
which are worth watching? We test whether authorship by a researcher with
a *prior* top-tier publication predicts that a preprint will itself become
a top-tier paper.

The design avoids the two confounds that would make such a result
meaningless:

  * **Temporal split.** "Prior top-4 author" is determined strictly from
    publications *before* the preprint year, so the predictor cannot peek
    at the outcome.
  * **Author-independent outcome.** Whether a preprint "became" a top-4
    paper is decided purely by title similarity against the published
    corpus, with *no* author overlap required. The predictor (authorship)
    and the outcome (title match) are therefore measured independently --
    removing the circularity of an author-anchored matcher.

A full-name author key (not last-name-plus-initial) is used because this
is a population-level statistic where common-name collisions would
otherwise inflate the "prior-top-4" group.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from .author_matcher import jaccard, normalise_author, tokenise_title

DEFAULT_OUTCOME_THRESHOLD = 0.6


def strict_author_key(name: str) -> str:
    """Full normalised name, or ``""`` when too short to be discriminative.

    Unlike :func:`author_matcher.author_key` (last name + first initial),
    this keeps the whole given name so that population-level counts are not
    dominated by common-surname collisions ("Wang Y", "Li J").
    """
    normalised = normalise_author(name)
    parts = normalised.split()
    if len(parts) >= 2 and len(parts[0]) > 1:
        return normalised
    return ""


def build_prior_author_set(author_lists: Iterable[Iterable[str]]) -> frozenset[str]:
    """Collect strict author keys from the papers that define the track record."""
    keys: set[str] = set()
    for authors in author_lists:
        for name in authors:
            key = strict_author_key(name)
            if key:
                keys.add(key)
    return frozenset(keys)


class OutcomeIndex:
    """Decides whether a title matches any published paper by Jaccard overlap.

    An inverted token index keeps the check near-linear: a candidate paper
    is only scored when it shares at least one title token with the query.
    """

    def __init__(self, published_titles: Iterable[str]) -> None:
        self._paper_tokens: list[frozenset[str]] = []
        self._inverted: dict[str, list[int]] = defaultdict(list)
        for title in published_titles:
            tokens = frozenset(tokenise_title(title))
            if not tokens:
                continue
            index = len(self._paper_tokens)
            self._paper_tokens.append(tokens)
            for token in tokens:
                self._inverted[token].append(index)

    def is_published(self, title: str, threshold: float = DEFAULT_OUTCOME_THRESHOLD) -> bool:
        tokens = tokenise_title(title)
        if not tokens:
            return False
        candidates: set[int] = set()
        for token in tokens:
            candidates.update(self._inverted.get(token, ()))
        token_set = frozenset(tokens)
        return any(jaccard(token_set, self._paper_tokens[i]) >= threshold for i in candidates)


@dataclass(frozen=True)
class ReadinessResult:
    """Outcome of the readiness experiment for one preprint cohort."""

    threshold: float
    n_with_track_record: int
    n_without_track_record: int
    converted_with: int       # preprints by prior-top-4 authors that became top-4
    converted_without: int

    @property
    def precision(self) -> float:
        """P(becomes top-4 | preprint by a prior-top-4 author)."""
        return self.converted_with / self.n_with_track_record if self.n_with_track_record else 0.0

    @property
    def base_rate(self) -> float:
        """P(becomes top-4 | preprint without a prior-top-4 author)."""
        return self.converted_without / self.n_without_track_record if self.n_without_track_record else 0.0

    @property
    def lift(self) -> float:
        """How many times more precise the track-record filter is than the base rate."""
        base = self.base_rate
        return self.precision / base if base else float("inf")

    @property
    def recall(self) -> float:
        """Fraction of all future-top-4 preprints captured by the filter."""
        total_converted = self.converted_with + self.converted_without
        return self.converted_with / total_converted if total_converted else 0.0

    @property
    def volume_reduction(self) -> float:
        """Fraction of the preprint pool excluded by applying the filter."""
        total = self.n_with_track_record + self.n_without_track_record
        return self.n_without_track_record / total if total else 0.0


def analyze(
    preprints: Iterable[tuple[str, Iterable[str]]],
    prior_authors: frozenset[str],
    outcome_index: OutcomeIndex,
    threshold: float = DEFAULT_OUTCOME_THRESHOLD,
) -> ReadinessResult:
    """Partition ``preprints`` by track record and measure conversion to top-4.

    ``preprints`` yields ``(title, authors)`` pairs for a single submission
    year. ``prior_authors`` is the strict-key set of researchers with a
    top-4 paper *before* that year. ``outcome_index`` is built from the
    published top-4 papers that define "became top-4".
    """
    n_with = n_without = conv_with = conv_without = 0
    for title, authors in preprints:
        has_track_record = any(strict_author_key(a) in prior_authors for a in authors if strict_author_key(a))
        became = outcome_index.is_published(title, threshold)
        if has_track_record:
            n_with += 1
            conv_with += became
        else:
            n_without += 1
            conv_without += became
    return ReadinessResult(
        threshold=threshold,
        n_with_track_record=n_with,
        n_without_track_record=n_without,
        converted_with=conv_with,
        converted_without=conv_without,
    )
