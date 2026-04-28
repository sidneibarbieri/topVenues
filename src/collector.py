"""Main collector orchestrating download, consolidate, and extract."""

import asyncio
import random
from datetime import datetime, timedelta
from pathlib import Path

from .abstract_fetcher import AbstractFetcher
from .cache import CacheManager
from .checkpoint import CheckpointManager
from .config import load_configuration
from .consolidator import DataConsolidator
from .database import DatabaseManager
from .downloader import JSONDownloader
from .extractors import get_extractor_for_event
from .models import Configuration, Paper, SearchFilters


def _extract_doi(ee_url: str | None) -> str | None:
    """Return the bare DOI from a doi.org URL, or None if not applicable."""
    if not ee_url:
        return None
    if "doi.org/" in ee_url:
        return ee_url.split("doi.org/", 1)[1]
    if ee_url.startswith("10."):
        return ee_url
    return None


class Collector:
    """Orchestrates DBLP JSON download, data consolidation, and abstract extraction."""

    def __init__(
        self,
        base_dir: Path | None = None,
        config_path: Path | None = None,
    ):
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.config: Configuration = load_configuration(config_path)

        self.data_dir = self.base_dir / self.config.data_dir
        self.log_dir = self.base_dir / self.config.log_dir
        self.json_dir = self.base_dir / self.config.json_dir
        self.cache_dir = self.base_dir / self.config.cache_dir
        self.checkpoint_dir = self.base_dir / self.config.checkpoint_dir

        for directory in (
            self.data_dir,
            self.log_dir,
            self.json_dir,
            self.cache_dir,
            self.checkpoint_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

        self.cache_manager = CacheManager(
            self.cache_dir,
            enabled=self.config.cache_enabled,
            ttl_hours=self.config.cache_ttl_hours,
        )
        self.checkpoint_manager = CheckpointManager(
            self.checkpoint_dir,
            enabled=self.config.checkpoint_enabled,
        )
        self.db = DatabaseManager(self.data_dir / "papers.db")

        self.acm_blocked_until: datetime | None = None
        self.acm_failure_counts: dict[str, int] = {}
        self.acm_backoff_seconds = self.config.acm_backoff_initial
        self.last_was_acm = False

        self.papers: list[Paper] = []

    def is_acm_blocked(self) -> bool:
        return self.acm_blocked_until is not None and datetime.now() < self.acm_blocked_until

    def get_acm_failure_count(self, url: str) -> int:
        return self.acm_failure_counts.get(url, 0)

    def increment_acm_failure_count(self, url: str) -> None:
        self.acm_failure_counts[url] = self.acm_failure_counts.get(url, 0) + 1

    def reset_acm_failure_count(self, url: str) -> None:
        self.acm_failure_counts.pop(url, None)

    def block_acm(self) -> None:
        self.acm_blocked_until = datetime.now() + timedelta(seconds=self.acm_backoff_seconds)
        self.acm_backoff_seconds = min(self.acm_backoff_seconds * 2, self.config.acm_backoff_max)

    def get_random_user_agent(self) -> str:
        return random.choice(self.config.user_agents)

    def get_random_interval(self) -> float:
        lo, hi = self.config.default_interval
        return random.uniform(lo, hi)

    async def run_download(self) -> None:
        async with JSONDownloader(self.config, self.log_dir) as downloader:
            await downloader.download_all(self.json_dir)

    async def run_consolidate(self) -> None:
        consolidator = DataConsolidator(self.json_dir, self.data_dir)
        # Consolidate emits JSON-normalized metadata; upsert COALESCE preserves abstracts.
        self.db.upsert_papers(consolidator.consolidate())
        self.db.export_to_csv(self.data_dir / "master_dataset.csv")
        # Reload from DB so self.papers reflects abstracts for the subsequent extract phase.
        self.papers = [Paper(**p) for p in self.db.get_all_papers()]
        print(f"Consolidated {len(self.papers)} papers")

    async def run_extract(self) -> None:
        if not self.papers:
            self.papers = self._load_papers_from_disk()

        to_process = [p for p in self.papers if not p.abstract]
        if not to_process:
            print("All papers already have abstracts")
            return

        print(f"Extracting abstracts for {len(to_process)} papers...")
        fetcher = AbstractFetcher(self)
        processed = 0

        while to_process:
            batch = self._interleave_batch(to_process[: self.config.batch_size])
            to_process = to_process[self.config.batch_size :]

            for paper in batch:
                await self._extract_single_abstract(paper, fetcher)
                processed += 1
                if processed % self.config.checkpoint_interval == 0:
                    self._save_checkpoint()

            self._save_dataset()
            await asyncio.sleep(random.uniform(self.config.acm_wait_min, self.config.acm_wait_max))

        await fetcher.close()
        print(f"Extracted abstracts for {processed} papers")

    async def run_full(self) -> None:
        print("Starting Top Venues Collector...")
        print("\n[1/3] Downloading JSON files...")
        await self.run_download()
        print("\n[2/3] Consolidating data...")
        await self.run_consolidate()
        print("\n[3/3] Extracting abstracts...")
        await self.run_extract()
        print("\nCollection complete!")

    def search(self, filters: SearchFilters, limit: int | None = None) -> list[Paper]:
        if not self.papers:
            self.papers = self._load_papers_from_disk()

        results = []
        for paper in self.papers:
            if (
                filters.title_contains
                and filters.title_contains.lower() not in (paper.title or "").lower()
            ):
                continue
            if (
                filters.abstract_contains
                and filters.abstract_contains.lower() not in (paper.abstract or "").lower()
            ):
                continue
            if (
                filters.author_contains
                and filters.author_contains.lower() not in (paper.authors or "").lower()
            ):
                continue
            if filters.event and paper.event != filters.event:
                continue
            if filters.year and paper.year != filters.year:
                continue
            if filters.technology:
                tech = filters.technology.lower()
                if (
                    tech not in (paper.title or "").lower()
                    and tech not in (paper.abstract or "").lower()
                ):
                    continue

            results.append(paper)
            if limit and len(results) >= limit:
                break

        return results

    def _load_papers_from_disk(self) -> list[Paper]:
        """Read papers from the SQLite DB — the single source of truth."""
        return [Paper(**p) for p in self.db.get_all_papers()]

    async def _extract_single_abstract(self, paper: Paper, fetcher: AbstractFetcher) -> None:
        if not paper.ee:
            return

        cache_key = f"abstract_{paper.paper_id}"
        cached = self.cache_manager.get(cache_key)
        if cached:
            paper.abstract = cached
            return

        extractor = get_extractor_for_event(paper.event or "")
        abstract = await extractor.extract(paper.ee, paper.paper_id, self)

        if not abstract:
            doi = _extract_doi(paper.ee)
            abstract = await fetcher.fetch_all(doi) if doi else None

        if abstract:
            paper.abstract = abstract
            self.cache_manager.set(cache_key, abstract)
            self.db.update_abstract(paper.paper_id, abstract)

    def _interleave_batch(self, papers: list[Paper]) -> list[Paper]:
        """Shuffle papers to alternate ACM and non-ACM requests."""
        acm = [p for p in papers if p.event and ("ACM" in p.event or "HotNets" in p.event)]
        non_acm = [p for p in papers if p not in acm]
        random.shuffle(acm)
        random.shuffle(non_acm)

        result: list[Paper] = []
        i_acm = i_non = 0
        while i_acm < len(acm) or i_non < len(non_acm):
            if i_non < len(non_acm) and (not self.last_was_acm or i_acm >= len(acm)):
                result.append(non_acm[i_non])
                i_non += 1
                self.last_was_acm = False
            elif i_acm < len(acm):
                result.append(acm[i_acm])
                i_acm += 1
                self.last_was_acm = True
        return result

    def _save_checkpoint(self) -> None:
        papers_with_abstracts = sum(1 for p in self.papers if p.abstract)
        self.checkpoint_manager.save(
            phase="extract",
            papers=[p.model_dump(by_alias=True) for p in self.papers],
            papers_with_abstracts=papers_with_abstracts,
        )

    def _save_dataset(self) -> None:
        """Export the current in-memory papers to CSV and PKL via the consolidator."""
        DataConsolidator(self.json_dir, self.data_dir).save_dataset(self.papers)
