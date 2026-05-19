"""arXiv harvester for the early-signal study.

Pulls preprints from a set of arXiv categories and persists them locally,
so the matching stage can run offline against a stable snapshot.

Per arXiv's API Terms of Use we acknowledge the data source via
:data:`ARXIV_ACKNOWLEDGMENT`; the study report prints it in its footer.

Operational notes drawn from the arXiv API User's Manual:
  * ``submittedDate`` is GMT and the response's ``<published>`` field is
    the v1 (original) submission date — exactly the anchor we need for lag.
  * A single query is capped at 30 000 results and must be paged in
    slices of at most 2 000; year-windowing keeps each query well under
    the cap (see :func:`year_windows`).
  * Results only change when new articles are added (~daily), so caching
    the JSONL snapshot and reusing it is both correct and encouraged.
"""

from __future__ import annotations

import json
import logging
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

# Required attribution under arXiv's API Terms of Use.
ARXIV_ACKNOWLEDGMENT = "Thank you to arXiv for use of its open access interoperability."


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


# arXiv caps a single query at 30 000 results and silently truncates
# beyond that. cs.CR alone is already ~28 000 for 2022-2026 and growing,
# so we partition the date range into per-year windows that each stay
# comfortably under the cap.
_MAX_RESULTS_PER_QUERY = 30_000


def year_windows(since_year: int, until_year: int) -> Iterator[tuple[str, str]]:
    """Yield disjoint ``(start, end)`` ``submittedDate`` bounds, one per year.

    Each bound is an arXiv timestamp ``YYYYMMDDHHMM``. Windows are
    half-inclusive on the calendar year (Jan 1 00:00 .. Dec 31 23:59) and
    never overlap, so no preprint is harvested twice.

    Granularity is per-year: fewest queries (~5), each ~6-8k results,
    comfortably under the cap. Isolating it here lets the choice change
    (e.g. to per-month) without touching the harvest loop.
    """
    for year in range(since_year, until_year + 1):
        yield (f"{year}01010000", f"{year}12312359")


def _stream_query(client: arxiv.Client, query: str) -> Iterator[Preprint]:
    """Stream every result of a single arXiv query, newest first.

    ``max_results=None`` overrides the library's default of 100, which
    would otherwise silently truncate each query to its first 100 hits.
    Per-year windows stay well under arXiv's 30 000-per-query server cap.
    """
    search = arxiv.Search(
        query=query,
        max_results=None,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending,
    )
    for result in client.results(search):
        yield _from_arxiv_result(result)


# arXiv serves a single query in slices of at most 2 000 results and
# cautions against very large slices; 1 000 halves the request count
# versus the old 200 while staying inside the recommended range.
DEFAULT_PAGE_SIZE = 1_000


def harvest(
    categories: Iterable[str] = DEFAULT_CATEGORIES,
    since_year: int = 2022,
    until_year: int = 2026,
    page_size: int = DEFAULT_PAGE_SIZE,
    delay_seconds: float = 3.0,
) -> Iterator[Preprint]:
    """Stream all preprints in ``categories`` submitted in ``[since_year, until_year]``.

    arXiv recommends at most one query every three seconds and caps a
    single query at 30 000 results. We respect the pause via the client's
    ``delay_seconds`` and stay under the cap by querying one year at a
    time (see :func:`year_windows`).
    """
    client = arxiv.Client(page_size=page_size, delay_seconds=delay_seconds, num_retries=4)

    for category in categories:
        for start, end in year_windows(since_year, until_year):
            # ``submittedDate`` is the v1 submission date. The arxiv client
            # URL-encodes the query itself, so the range separator must be a
            # literal space (" TO "); a "+" would arrive as %2B and arXiv
            # rejects it with HTTP 500.
            query = f"cat:{category} AND submittedDate:[{start} TO {end}]"
            yield from _stream_query(client, query)


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
