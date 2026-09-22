"""Backfill missing abstracts across the whole corpus, multi-source.

Routing per record, by its ``ee`` URL:
  * DOI            -> Semantic Scholar / OpenAlex / CrossRef (the DOI fetcher)
  * OpenReview     -> OpenAlex title match (OpenReview blocks automated access)
  * PMLR / NeurIPS -> the open landing page
  * anything else  -> OpenAlex title match

Every candidate passes the shared quality gate before it is stored. The run
is resumable: it only selects records that still lack an abstract, and it
commits in batches, so an interrupted run loses at most one batch.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.abstract_fetcher import AbstractFetcher
from src.collector import Collector
from src.open_abstract_sources import fetch_openalex_by_title, source_for
from src.sqlite_connection import managed_sqlite_connection

BATCH_SIZE = 200


def _doi(ee: str | None) -> str | None:
    if not ee:
        return None
    if "doi.org/" in ee:
        return ee.split("doi.org/", 1)[1]
    if ee.startswith("10."):
        return ee
    return None


async def _resolve(
    client: httpx.AsyncClient,
    doi_fetcher: AbstractFetcher,
    title: str,
    year: int | None,
    ee: str | None,
) -> str | None:
    doi = _doi(ee)
    if doi:
        result = await doi_fetcher.fetch_all(doi)
        if result:
            return result
    landing = source_for(ee)
    if landing:
        result = await landing(client, ee)
        if result:
            return result
    return await fetch_openalex_by_title(client, title, year)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=6)
    args = parser.parse_args()

    collector = Collector(base_dir=Path.cwd())
    db_path = collector.db.db_path
    with managed_sqlite_connection(db_path) as connection:
        query = (
            "SELECT paper_id, title, year, ee FROM papers "
            "WHERE (abstract IS NULL OR abstract = '') AND ee IS NOT NULL"
        )
        if args.limit:
            query += f" LIMIT {args.limit}"
        targets = connection.execute(query).fetchall()

    print(f"targets: {len(targets):,}", flush=True)
    doi_fetcher = AbstractFetcher(collector)
    semaphore = asyncio.Semaphore(args.concurrency)
    recovered = 0
    processed = 0
    started = time.perf_counter()

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers={"User-Agent": "TopVenues/1.0"},
    ) as client:

        async def worker(row: tuple) -> tuple[str, str | None]:
            paper_id, title, year, ee = row
            async with semaphore:
                return paper_id, await _resolve(client, doi_fetcher, title, year, ee)

        for start in range(0, len(targets), BATCH_SIZE):
            batch = targets[start : start + BATCH_SIZE]
            results = await asyncio.gather(*(worker(row) for row in batch))
            updates = [(abstract, pid) for pid, abstract in results if abstract]
            if updates:
                with managed_sqlite_connection(db_path) as connection:
                    connection.executemany(
                        "UPDATE papers SET abstract = ?, "
                        "updated_at = CURRENT_TIMESTAMP WHERE paper_id = ?",
                        updates,
                    )
                recovered += len(updates)
            processed += len(batch)
            rate = processed / (time.perf_counter() - started)
            print(
                f"  {processed:,}/{len(targets):,} processed, "
                f"{recovered:,} recovered ({rate:.1f}/s)",
                flush=True,
            )

    await doi_fetcher.close()
    print(f"DONE: recovered {recovered:,} of {len(targets):,}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
