"""Fallback abstract APIs."""

import asyncio
import logging
import re
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx

if TYPE_CHECKING:
    from .collector import Collector

logger = logging.getLogger(__name__)


class AbstractFetcher:
    """Tries fallback APIs to retrieve abstract by DOI."""

    def __init__(self, collector: "Collector"):
        self.collector = collector
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0),
            headers=collector.config.headers,
        )

    async def fetch_semanticscholar(self, doi: str) -> str | None:
        if not doi or not doi.startswith("10."):
            return None

        cache_key = f"semanticscholar_{doi}"
        cached = self.collector.cache_manager.get(cache_key)
        if cached:
            return cached

        url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=abstract"
        try:
            response = await self.client.get(
                url, headers={"User-Agent": self.collector.get_random_user_agent()}
            )
        except httpx.HTTPError as e:
            logger.warning("Semantic Scholar HTTP error for %s: %s", doi, e)
            return None

        if response.status_code != 200:
            return None

        abstract = response.json().get("abstract")
        if not abstract or len(abstract) < 100:
            return None

        self.collector.cache_manager.set(cache_key, abstract)
        return abstract.strip()

    async def fetch_openalex(self, doi: str) -> str | None:
        if not doi or not doi.startswith("10."):
            return None

        cache_key = f"openalex_{doi}"
        cached = self.collector.cache_manager.get(cache_key)
        if cached:
            return cached

        url = f"https://api.openalex.org/works/https://doi.org/{quote(doi, safe='')}"
        try:
            response = await self.client.get(
                url, headers={"User-Agent": self.collector.get_random_user_agent()}
            )
        except httpx.HTTPError as e:
            logger.warning("OpenAlex HTTP error for %s: %s", doi, e)
            return None

        if response.status_code != 200:
            return None

        inverted_index = response.json().get("abstract_inverted_index")
        if not inverted_index:
            return None

        max_pos = max(max(positions) for positions in inverted_index.values())
        words = [""] * (max_pos + 1)
        for word, positions in inverted_index.items():
            for pos in positions:
                words[pos] = word
        abstract = " ".join(words)

        if len(abstract) < 100:
            return None

        self.collector.cache_manager.set(cache_key, abstract)
        return abstract.strip()

    async def fetch_crossref(self, doi: str) -> str | None:
        if not doi or not doi.startswith("10."):
            return None

        cache_key = f"crossref_{doi}"
        cached = self.collector.cache_manager.get(cache_key)
        if cached:
            return cached

        url = f"https://api.crossref.org/works/{doi}?mailto=topvenues@example.com"
        try:
            response = await self.client.get(
                url, headers={"User-Agent": self.collector.get_random_user_agent()}
            )
        except httpx.HTTPError as e:
            logger.warning("CrossRef HTTP error for %s: %s", doi, e)
            return None

        if response.status_code != 200:
            return None

        abstract = response.json().get("message", {}).get("abstract")
        if not abstract:
            return None

        abstract = re.sub(r"<jats:title>.*?</jats:title>", "", abstract, flags=re.DOTALL)
        abstract = re.sub(r"</?jats:[a-z]+>", "", abstract)
        abstract = re.sub(r"<.*?>", "", abstract)
        abstract = re.sub(r"\s+", " ", abstract).strip()

        if len(abstract) < 100:
            return None

        self.collector.cache_manager.set(cache_key, abstract)
        return abstract

    async def fetch_all(self, doi: str) -> str | None:
        """Fire all three APIs in parallel; return the first successful result."""
        tasks = [
            asyncio.create_task(self.fetch_semanticscholar(doi)),
            asyncio.create_task(self.fetch_openalex(doi)),
            asyncio.create_task(self.fetch_crossref(doi)),
        ]
        try:
            for completed in asyncio.as_completed(tasks):
                result = await completed
                if result:
                    for task in tasks:
                        task.cancel()
                    return result
        except Exception as exc:
            logger.warning("fetch_all error for %s: %s", doi, exc)
        finally:
            for task in tasks:
                task.cancel()
        return None

    async def close(self) -> None:
        await self.client.aclose()
