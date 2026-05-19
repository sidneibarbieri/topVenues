"""arXiv harvester for the early-signal study.

Pulls preprints from a set of arXiv categories and persists them locally,
so the matching stage can run offline against a stable snapshot.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import arxiv

logger = logging.getLogger(__name__)

# Cybersecurity papers cross-list mainly to cs.CR; harvesting that one
# category captures the vast majority of relevant preprints because
# arXiv counts cross-listings in category-restricted searches.
DEFAULT_CATEGORIES = ("cs.CR",)


@dataclass(frozen=True)
class Preprint:
    arxiv_id: str           # e.g. "2410.12345v1"
    title: str
    authors: tuple[str, ...]
    submitted_at: str       # ISO 8601 of the FIRST submission (v1)
    updated_at: str         # ISO 8601 of the latest revision
    primary_category: str
    categories: tuple[str, ...]
    doi: str | None
    journal_ref: str | None
    summary: str

    def submitted_date(self) -> datetime:
        return datetime.fromisoformat(self.submitted_at)


def harvest(
    categories: Iterable[str] = DEFAULT_CATEGORIES,
    since_year: int = 2022,
    until_year: int = 2026,
    page_size: int = 200,
    delay_seconds: float = 3.0,
) -> Iterator[Preprint]:
    """Stream all preprints in ``categories`` submitted in ``[since_year, until_year]``.

    arXiv recommends at most one query every three seconds and a maximum
    of 30 000 results per query. We respect both by paginating and
    sleeping between page fetches.
    """
    client = arxiv.Client(page_size=page_size, delay_seconds=delay_seconds, num_retries=4)

    for category in categories:
        # ``submittedDate`` is the v1 submission date; arXiv accepts a
        # ``[YYYYMMDDHHMM+TO+YYYYMMDDHHMM]`` range qualifier.
        query = (
            f"cat:{category} AND "
            f"submittedDate:[{since_year}01010000+TO+{until_year}12312359]"
        )
        search = arxiv.Search(
            query=query,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        for result in client.results(search):
            yield _from_arxiv_result(result)


def _from_arxiv_result(result: arxiv.Result) -> Preprint:
    return Preprint(
        arxiv_id=result.entry_id.rsplit("/", 1)[-1],
        title=result.title.strip(),
        authors=tuple(a.name for a in result.authors),
        submitted_at=result.published.astimezone(timezone.utc).isoformat(),
        updated_at=result.updated.astimezone(timezone.utc).isoformat(),
        primary_category=result.primary_category,
        categories=tuple(result.categories),
        doi=result.doi,
        journal_ref=result.journal_ref,
        summary=result.summary.strip(),
    )


def save_jsonl(preprints: Iterable[Preprint], target: Path) -> int:
    """Write preprints as JSON Lines so each line can be appended atomically."""
    target.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with target.open("w", encoding="utf-8") as fh:
        for preprint in preprints:
            fh.write(json.dumps(asdict(preprint), ensure_ascii=False) + "\n")
            written += 1
            if written % 500 == 0:
                logger.info("harvested %s preprints so far", written)
    return written


def load_jsonl(source: Path) -> list[Preprint]:
    """Read a saved snapshot of preprints back into memory."""
    out: list[Preprint] = []
    with source.open(encoding="utf-8") as fh:
        for line in fh:
            payload = json.loads(line)
            payload["authors"] = tuple(payload["authors"])
            payload["categories"] = tuple(payload["categories"])
            out.append(Preprint(**payload))
    return out
