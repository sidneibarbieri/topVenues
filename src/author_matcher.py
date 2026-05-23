"""Match arXiv preprints to papers in a configured publication scope.

Two signals are combined to decide whether an arXiv preprint corresponds
to a published paper:

  * **author overlap** — at least one normalized author name is shared,
  * **title similarity** — Jaccard similarity over normalized tokens.

A match is accepted when both pass their respective thresholds. Both
thresholds were chosen to favor false negatives over false positives so
the reported match rate is a conservative lower bound for the configured
scope.
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC

from .arxiv_fetcher import Preprint

# Tokens that carry no discriminative power for title matching.
_STOPWORDS = frozenset({
    "a", "an", "the", "of", "for", "on", "in", "and", "or", "but", "is",
    "to", "with", "by", "from", "via", "using", "based", "towards",
    "approach", "study", "evaluation", "analysis", "system", "systems",
})

DEFAULT_TITLE_THRESHOLD = 0.55
DEFAULT_AUTHOR_THRESHOLD = 1   # at least one author must overlap

# Fallback for event scopes without a configured presentation month.
DEFAULT_PUBLICATION_MONTH = 7


@dataclass(frozen=True)
class Match:
    paper_id: str
    arxiv_id: str
    paper_title: str
    arxiv_title: str
    title_similarity: float
    shared_authors: tuple[str, ...]
    lag_days: int       # paper.year (Jan 1) − preprint.submitted_at, in days
    paper_year: int
    paper_venue: str
    arxiv_submitted: str


# ── Author normalisation ───────────────────────────────────────────────


def normalize_author(name: str) -> str:
    """Lowercase, strip accents and DBLP numeric suffixes, collapse whitespace."""
    text = unicodedata.normalize("NFKD", name)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"\s+\d{4}$", "", text)            # drop "0001" disambiguator
    text = re.sub(r"[^a-z\s\-']", " ", text)         # keep letters and hyphens
    text = re.sub(r"\s+", " ", text).strip()
    return text


def author_key(name: str) -> str:
    """Compact key for index lookups: last name + first initial."""
    normalized = normalize_author(name)
    if not normalized:
        return ""
    parts = normalized.split()
    if len(parts) == 1:
        return parts[0]
    return f"{parts[-1]} {parts[0][0]}"   # "rana m" for "Md. Shohel Rana"


# ── Title similarity ───────────────────────────────────────────────────


def tokenize_title(title: str) -> set[str]:
    text = title.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = {t for t in text.split() if t and t not in _STOPWORDS and len(t) > 1}
    return tokens


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ── Matching engine ────────────────────────────────────────────────────


def build_author_index(preprints: Iterable[Preprint]) -> dict[str, list[Preprint]]:
    """Index preprints by author_key for fast author-overlap lookups."""
    index: dict[str, list[Preprint]] = defaultdict(list)
    for preprint in preprints:
        for author in preprint.authors:
            key = author_key(author)
            if key:
                index[key].append(preprint)
    return index


def find_matches(
    paper_id: str,
    paper_title: str,
    paper_authors: list[str],
    paper_year: int,
    paper_venue: str,
    author_index: dict[str, list[Preprint]],
    title_threshold: float = DEFAULT_TITLE_THRESHOLD,
    publication_months: Mapping[str, int] | None = None,
) -> list[Match]:
    """Return all arXiv preprints that match the given paper above thresholds."""
    paper_tokens = tokenize_title(paper_title)
    paper_author_keys = {author_key(a) for a in paper_authors if author_key(a)}
    if not paper_tokens or not paper_author_keys:
        return []

    # Candidate preprints are those sharing at least one author.
    candidates: dict[str, Preprint] = {}
    for key in paper_author_keys:
        for preprint in author_index.get(key, ()):
            candidates[preprint.arxiv_id] = preprint

    matches: list[Match] = []
    for preprint in candidates.values():
        similarity = jaccard(paper_tokens, tokenize_title(preprint.title))
        if similarity < title_threshold:
            continue

        preprint_author_keys = {author_key(a) for a in preprint.authors if author_key(a)}
        shared = paper_author_keys & preprint_author_keys
        if len(shared) < DEFAULT_AUTHOR_THRESHOLD:
            continue

        lag_days = _lag_days(paper_year, paper_venue, preprint.submitted_at, publication_months)
        matches.append(Match(
            paper_id=paper_id,
            arxiv_id=preprint.arxiv_id,
            paper_title=paper_title,
            arxiv_title=preprint.title,
            title_similarity=similarity,
            shared_authors=tuple(sorted(shared)),
            lag_days=lag_days,
            paper_year=paper_year,
            paper_venue=paper_venue,
            arxiv_submitted=preprint.submitted_at,
        ))

    matches.sort(key=lambda m: m.title_similarity, reverse=True)
    return matches


def _lag_days(
    paper_year: int,
    paper_venue: str,
    arxiv_submitted_at: str,
    publication_months: Mapping[str, int] | None = None,
) -> int:
    """Days from the v1 arXiv submission to the configured presentation date.

    The publication date is anchored at the first day of the configured
    presentation month. A positive lag means the preprint preceded publication.
    """
    from datetime import datetime
    months = publication_months or {}
    month = months.get(paper_venue, DEFAULT_PUBLICATION_MONTH)
    arxiv_date = datetime.fromisoformat(arxiv_submitted_at)
    paper_date = datetime(paper_year, month, 1, tzinfo=UTC)
    return (paper_date - arxiv_date).days
