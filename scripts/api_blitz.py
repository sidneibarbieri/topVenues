"""Parallel API blitz: fill missing abstracts via Semantic Scholar / OpenAlex / CrossRef.

Strategy:
  * Load every paper with no abstract but with an ``ee`` link.
  * Extract the DOI from each link.
  * Fan out fetch_all(doi) with bounded concurrency (no Cloudflare risk on these APIs).
  * Persist each result to the DB as it arrives — fully resumable.

Usage::

    python scripts/api_blitz.py            # process all eligible papers
    python scripts/api_blitz.py --limit 200
    python scripts/api_blitz.py --concurrency 16
"""

import argparse
import asyncio
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.abstract_fetcher import AbstractFetcher
from src.collector import Collector, _extract_doi
from src.extractors.base import AbstractExtractor


async def _process(paper_id: str, doi: str, fetcher: AbstractFetcher,
                   sem: asyncio.Semaphore, db_path: Path) -> tuple[str, bool]:
    async with sem:
        abstract = await fetcher.fetch_all(doi)
    if not abstract:
        return paper_id, False
    cleaned = AbstractExtractor._strip_leading_author_blocks(abstract)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE papers SET abstract=?, updated_at=CURRENT_TIMESTAMP WHERE paper_id=?",
            (cleaned, paper_id),
        )
    return paper_id, True


async def main(limit: int | None, concurrency: int) -> None:
    collector = Collector()
    db_path = collector.db.db_path

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT paper_id, ee FROM papers "
            "WHERE (abstract IS NULL OR abstract = '') AND ee IS NOT NULL AND ee != ''"
        ).fetchall()

    candidates = [(r["paper_id"], _extract_doi(r["ee"])) for r in rows]
    candidates = [(pid, doi) for pid, doi in candidates if doi]
    if limit:
        candidates = candidates[:limit]

    print(f"Targets: {len(candidates):,} papers (concurrency={concurrency})")
    if not candidates:
        return

    fetcher = AbstractFetcher(collector)
    sem = asyncio.Semaphore(concurrency)
    tasks = [
        asyncio.create_task(_process(pid, doi, fetcher, sem, db_path))
        for pid, doi in candidates
    ]

    start = time.time()
    recovered = 0
    for idx, coro in enumerate(asyncio.as_completed(tasks), 1):
        _, ok = await coro
        if ok:
            recovered += 1
        if idx % 25 == 0 or idx == len(tasks):
            rate = idx / max(time.time() - start, 0.1)
            eta_min = (len(tasks) - idx) / max(rate, 0.1) / 60
            pct = recovered / idx * 100
            print(f"  {idx:>5}/{len(tasks):,}  recovered {recovered:>4} ({pct:5.1f}%)  "
                  f"rate {rate:5.1f}/s  ETA {eta_min:5.1f} min")

    await fetcher.close()
    print(f"\nDone — {recovered:,}/{len(candidates):,} abstracts recovered "
          f"in {(time.time() - start) / 60:.1f} min.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=8)
    args = parser.parse_args()
    asyncio.run(main(args.limit, args.concurrency))
