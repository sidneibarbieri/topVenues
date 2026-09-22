"""Deterministic comparison between two materialized corpus snapshots."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pydantic import BaseModel


class CorpusObservation(BaseModel):
    papers: int
    abstracts: int
    bibtex: int
    venues: int
    year_min: int | None
    year_max: int | None


class ProfileComparison(BaseModel):
    previous: CorpusObservation
    successor: CorpusObservation
    added: int
    removed: int
    retained: int
    added_ids: list[str]
    removed_ids: list[str]


def _observe(connection: sqlite3.Connection) -> CorpusObservation:
    row = connection.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN TRIM(COALESCE(abstract, '')) <> '' THEN 1 ELSE 0 END),
               SUM(CASE WHEN TRIM(COALESCE(bibtex, '')) <> '' THEN 1 ELSE 0 END),
               COUNT(DISTINCT event), MIN(year), MAX(year)
        FROM papers
        """
    ).fetchone()
    return CorpusObservation(
        papers=row[0],
        abstracts=row[1],
        bibtex=row[2],
        venues=row[3],
        year_min=row[4],
        year_max=row[5],
    )


def compare_databases(previous_path: Path, successor_path: Path) -> ProfileComparison:
    """Compare exact paper identifiers and headline observations."""
    with sqlite3.connect(previous_path) as previous_connection:
        previous = _observe(previous_connection)
        previous_ids = {
            row[0] for row in previous_connection.execute("SELECT paper_id FROM papers")
        }
    with sqlite3.connect(successor_path) as successor_connection:
        successor = _observe(successor_connection)
        successor_ids = {
            row[0] for row in successor_connection.execute("SELECT paper_id FROM papers")
        }
    added_ids = sorted(successor_ids - previous_ids)
    removed_ids = sorted(previous_ids - successor_ids)
    return ProfileComparison(
        previous=previous,
        successor=successor,
        added=len(added_ids),
        removed=len(removed_ids),
        retained=len(previous_ids & successor_ids),
        added_ids=added_ids,
        removed_ids=removed_ids,
    )
