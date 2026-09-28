"""Run the measured triage rule forward, over preprints that are not in the corpus.

The main-track paper measures the rule backwards: of the 2023 arXiv ``cs.CR``
preprints whose authors had published in the declared top-4 in the four previous
years, 16 in 100 reached one of those venues by 2026, against 1 in 100 for the
rest. A measurement a reader cannot act on is a curiosity, so the same rule runs
forward here, over preprints submitted after the corpus froze: the reader sees
the work early instead of learning about it at publication.

Two boundaries hold, and the interface states both:

* A flagged preprint is **not** a corpus record, and is never counted as one. It
  joins the corpus only when a declared venue publishes it, which is the only
  way anything joins this corpus.
* An author match is a **name match**, not a verified identity, and 16 in 100 is
  the measured rate: most flagged preprints will not reach a top-4 venue. The
  rule orders reading; it does not predict acceptance.
"""

from __future__ import annotations

import sqlite3
import unicodedata
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from src.tiers import TOP4, tier_for

ARXIV_ACKNOWLEDGMENT = "Thank you to arXiv for use of its open access interoperability."
PRIOR_WINDOW_YEARS = 4
FLAGGED_ARRIVAL_RATE = 0.159  # measured on the 2023 cohort, scripts/readiness_study.py
UNFLAGGED_ARRIVAL_RATE = 0.0097


class PriorRecord(BaseModel):
    """One author of a preprint who already publishes in the tracked scope."""

    author: str
    papers: int
    venues: list[str]
    years: list[int]


class FlaggedPreprint(BaseModel):
    arxiv_id: str
    title: str
    authors: list[str]
    abstract: str
    submitted: str
    url: str
    prior_authors: list[PriorRecord]

    @property
    def strongest(self) -> PriorRecord:
        return max(self.prior_authors, key=lambda record: record.papers)


class RadarSnapshot(BaseModel):
    """What was asked of arXiv, against which corpus, and what came back."""

    retrieved_at: str
    category: str
    submitted_since: str
    profile_id: str
    corpus_fingerprint: str
    tracked_tier: str
    prior_window_years: int = PRIOR_WINDOW_YEARS
    considered: int
    flagged: list[FlaggedPreprint] = Field(default_factory=list)

    @property
    def flagged_share(self) -> float:
        return len(self.flagged) / self.considered if self.considered else 0.0


def strict_author_key(name: str) -> str:
    """Full normalized name, or ``""`` when too short to discriminate.

    The study counted at population level, where "Wang Y" would collide with
    hundreds of people, so the whole given name is kept. Using the same key here
    is what lets the measured rates describe this list.
    """
    decomposed = unicodedata.normalize("NFKD", name)
    without_accents = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    normalized = " ".join(without_accents.lower().replace(".", " ").split())
    parts = normalized.split()
    if len(parts) >= 2 and len(parts[0]) > 1:
        return normalized
    return ""


def _split_authors(raw: str | None) -> list[str]:
    return [author.strip() for author in (raw or "").split(",") if author.strip()]


def build_prior_index(
    database_path: Path,
    *,
    tracked_tier: str = TOP4,
    first_year: int,
    last_year: int,
) -> dict[str, PriorRecord]:
    """Map author key -> their record in the tracked tier, within the window.

    The window is closed before the preprints being flagged, so the rule can
    never read the outcome it is supposed to anticipate.
    """
    papers: dict[str, int] = defaultdict(int)
    venues: dict[str, set[str]] = defaultdict(set)
    years: dict[str, set[int]] = defaultdict(set)
    names: dict[str, str] = {}

    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "select authors, event, year from papers where year between ? and ?",
            (first_year, last_year),
        )
        for raw_authors, event, year in rows:
            if tier_for(event) != tracked_tier:
                continue
            for name in _split_authors(raw_authors):
                key = strict_author_key(name)
                if not key:
                    continue
                papers[key] += 1
                venues[key].add(event)
                years[key].add(int(year))
                names.setdefault(key, name)
    finally:
        connection.close()

    return {
        key: PriorRecord(
            author=names[key],
            papers=count,
            venues=sorted(venues[key]),
            years=sorted(years[key]),
        )
        for key, count in papers.items()
    }


def flag(preprints, index: dict[str, PriorRecord]) -> list[FlaggedPreprint]:
    """Keep the preprints with at least one author already in the tracked tier."""
    flagged: list[FlaggedPreprint] = []
    for preprint in preprints:
        matches = [
            index[key]
            for key in (strict_author_key(name) for name in preprint.authors)
            if key and key in index
        ]
        if not matches:
            continue
        flagged.append(
            FlaggedPreprint(
                arxiv_id=preprint.arxiv_id,
                title=preprint.title,
                authors=preprint.authors,
                abstract=preprint.abstract,
                submitted=preprint.published,
                url=preprint.url,
                prior_authors=sorted(matches, key=lambda record: -record.papers),
            )
        )
    return sorted(flagged, key=lambda item: item.submitted, reverse=True)


def snapshot(
    *,
    category: str,
    submitted_since: str,
    profile_id: str,
    corpus_fingerprint: str,
    considered: int,
    flagged: list[FlaggedPreprint],
    tracked_tier: str = TOP4,
) -> RadarSnapshot:
    return RadarSnapshot(
        retrieved_at=datetime.now(UTC).isoformat(timespec="seconds"),
        category=category,
        submitted_since=submitted_since,
        profile_id=profile_id,
        corpus_fingerprint=corpus_fingerprint,
        tracked_tier=tracked_tier,
        considered=considered,
        flagged=flagged,
    )


def radar_path(root: Path) -> Path:
    """Where the published radar lives: beside the data, never inside a profile."""
    return root / "data" / "radar" / "preprint-radar.json"


def load_radar(path: Path) -> RadarSnapshot | None:
    if not path.exists():
        return None
    return RadarSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
