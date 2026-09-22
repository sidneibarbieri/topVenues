"""Paper-award metadata: load curated award records and join them to the corpus.

Award records are a side dataset under ``data/awards/``, kept separate from the
bibliographic ``papers`` table on purpose: a paper can exist in the corpus
without an award, and every award label traces back to the official award page
it was collected from (``source_url``). Records are joined to corpus papers by
normalized title within the same venue.
"""

from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from pathlib import Path

# Award-record venue label -> the ``papers.venue`` strings that denote the same
# venue. The corpus stores more than one string for some venues (IEEE S&P is
# both "SP" and "IEEE Symposium on Security and Privacy").
CONFERENCE_TO_CORPUS_VENUES: dict[str, tuple[str, ...]] = {
    "IEEE S&P": ("SP", "IEEE Symposium on Security and Privacy"),
    "ACM CCS": ("CCS",),
    "NDSS": ("NDSS",),
    "USENIX Security": ("USENIX Security Symposium",),
    "ACSAC": ("ACSAC",),
}


@dataclass(frozen=True)
class AwardRecord:
    venue: str
    year: int
    award: str
    title: str
    url: str | None
    source_url: str


@dataclass(frozen=True)
class AwardMatch:
    award: AwardRecord
    paper_id: str


def normalize_title(title: str) -> str:
    """Fold a title to a comparison key: accents stripped, lowercase, words only.

    Accents are removed by dropping combining marks (so ``é`` -> ``e``); every
    other non-alphanumeric run, including unicode punctuation such as an em dash,
    becomes a single space separator.
    """
    decomposed = unicodedata.normalize("NFKD", title)
    without_accents = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", without_accents.lower()).strip()


def load_award_records(awards_dir: Path) -> list[AwardRecord]:
    """Load every ``*_paper_awards.json`` award table directly under ``awards_dir``."""
    records: list[AwardRecord] = []
    for path in sorted(awards_dir.glob("*_paper_awards.json")):
        for row in json.loads(path.read_text(encoding="utf-8")):
            records.append(
                AwardRecord(
                    venue=row["venue"],
                    year=int(row["year"]),
                    award=row["award"],
                    title=row["title"],
                    url=row.get("url"),
                    source_url=row["source_url"],
                )
            )
    return records


def match_awards_to_corpus(
    records: list[AwardRecord], db_path: Path
) -> tuple[list[AwardMatch], list[AwardRecord]]:
    """Join awards to corpus papers by normalized title within the same venue.

    Returns the matched (award, paper_id) pairs and the awards with no corpus
    paper. Unmatched awards are honest gaps: the corpus may not yet hold that
    paper for that venue/year.
    """
    connection = sqlite3.connect(db_path)
    try:
        index = _build_title_index(connection)
    finally:
        connection.close()

    matched: list[AwardMatch] = []
    unmatched: list[AwardRecord] = []
    for record in records:
        paper_id = _lookup_paper_id(record, index)
        if paper_id is None:
            unmatched.append(record)
        else:
            matched.append(AwardMatch(award=record, paper_id=paper_id))
    return matched, unmatched


def build_corpus_award_map(awards_dir: Path, db_path: Path) -> dict[str, list[str]]:
    """Map corpus ``paper_id`` to human-readable award labels, computed live.

    A paper with no award is simply absent from the map. Used by the search CLI
    to annotate and filter results without changing the ``papers`` schema.
    """
    matched, _ = match_awards_to_corpus(load_award_records(awards_dir), db_path)
    award_map: dict[str, list[str]] = {}
    for match in matched:
        label = f"{match.award.award} ({match.award.venue} {match.award.year})"
        award_map.setdefault(match.paper_id, []).append(label)
    return award_map


def _build_title_index(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
    """Map (corpus_venue, normalized_title) -> paper_id over the whole corpus."""
    index: dict[tuple[str, str], str] = {}
    for paper_id, title, venue in connection.execute("select paper_id, title, venue from papers"):
        if title and venue:
            index[(venue, normalize_title(title))] = paper_id
    return index


def _lookup_paper_id(record: AwardRecord, index: dict[tuple[str, str], str]) -> str | None:
    corpus_venues = CONFERENCE_TO_CORPUS_VENUES.get(record.venue, (record.venue,))
    key_title = normalize_title(record.title)
    for corpus_venue in corpus_venues:
        paper_id = index.get((corpus_venue, key_title))
        if paper_id is not None:
            return paper_id
    return None
