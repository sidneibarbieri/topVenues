"""Concurrent BibTeX backfill from DBLP — durable, resumable, gentle.

Usage::

    python scripts/bibtex_blitz.py
    python scripts/bibtex_blitz.py --concurrency 4 --limit 1000
"""

import argparse
import asyncio
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bibtex_fetcher import BibTeXFetcher
from src.collector import Collector
from src.models import Paper


async def main(concurrency: int, limit: int | None) -> None:
    collector = Collector()
    rows = collector.db.get_papers_without_bibtex(limit=limit)
    db_path = collector.db.db_path
    papers = [Paper(**row) for row in rows]
    print(f"Targets: {len(papers):,} papers (concurrency={concurrency})", flush=True)

    recovered = failed = completed = 0
    start = time.time()

    def persist(paper: Paper, bibtex: str | None) -> None:
        nonlocal recovered, failed, completed
        completed += 1
        if bibtex:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "UPDATE papers SET bibtex=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE paper_id=?",
                    (bibtex, paper.paper_id),
                )
            recovered += 1
        else:
            failed += 1
        if completed % 250 == 0 or completed == len(papers):
            elapsed = time.time() - start
            rate = completed / max(elapsed, 0.1)
            eta_min = (len(papers) - completed) / max(rate, 0.1) / 60
            pct = recovered / completed * 100
            print(f"  {completed:>5}/{len(papers):,}  ok {recovered:>5} ({pct:5.1f}%)  "
                  f"rate {rate:5.1f}/s  ETA {eta_min:5.1f} min", flush=True)

    async with BibTeXFetcher(concurrency=concurrency) as fetcher:
        await fetcher.fetch_many(papers, on_result=persist)

    print(f"\nDone — {recovered:,}/{len(papers):,} BibTeX entries fetched in "
          f"{(time.time() - start) / 60:.1f} min  (failures: {failed})", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    asyncio.run(main(args.concurrency, args.limit))
