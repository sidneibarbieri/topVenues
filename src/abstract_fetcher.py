"""Fallback abstract APIs."""

import asyncio
import logging
import re
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx

from .abstract_quality import looks_like_abstract, normalize_abstract_text, select_best_abstract

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
        self.semantic_scholar_disabled = False
        self.semantic_scholar_disable_logged = False

    async def fetch_semanticscholar(self, doi: str) -> str | None:
        if not doi or not doi.startswith("10."):
            return None
        if self.semantic_scholar_disabled:
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
            if "CERTIFICATE_VERIFY_FAILED" in str(e):
                self.semantic_scholar_disabled = True
                if not self.semantic_scholar_disable_logged:
                    logger.warning(
                        "Semantic Scholar disabled after TLS verification failure: %s", e
                    )
                    self.semantic_scholar_disable_logged = True
            else:
                logger.warning("Semantic Scholar HTTP error for %s: %s", doi, e)
            return None

        if response.status_code != 200:
            return None

        abstract = response.json().get("abstract")
        if not abstract or len(abstract) < 100:
            return None

        abstract = normalize_abstract_text(abstract)
        self.collector.cache_manager.set(cache_key, abstract)
        return abstract

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

        all_positions = [pos for positions in inverted_index.values() for pos in positions]
        # Reject reconstructions that omit the leading words — the abstract
        # would start mid-sentence and be visibly truncated.
        if not all_positions or min(all_positions) > 0:
            return None

        max_pos = max(all_positions)
        words = [""] * (max_pos + 1)
        for word, positions in inverted_index.items():
            for pos in positions:
                words[pos] = word
        abstract = " ".join(w for w in words if w)

        if len(abstract) < 100:
            return None

        abstract = normalize_abstract_text(abstract)
        self.collector.cache_manager.set(cache_key, abstract)
        return abstract

    async def fetch_crossref(self, doi: str) -> str | None:
        if not doi or not doi.startswith("10."):
            return None

        cache_key = f"crossref_{doi}"
        cached = self.collector.cache_manager.get(cache_key)
        if cached:
            return cached

        url = f"https://api.crossref.org/works/{doi}"
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
        abstract = normalize_abstract_text(abstract)

        if len(abstract) < 100:
            return None

        self.collector.cache_manager.set(cache_key, abstract)
        return abstract

    async def fetch_all(self, doi: str) -> str | None:
        """Fetch all three APIs in parallel and retain the strongest result.

        A source can return author-list metadata instead of a real abstract
        (CrossRef does this for some ACL Anthology records); results that do
        not pass :func:`looks_like_abstract` are skipped so junk never reaches
        the database.
        """
        tasks = [
            asyncio.create_task(self.fetch_semanticscholar(doi)),
            asyncio.create_task(self.fetch_openalex(doi)),
            asyncio.create_task(self.fetch_crossref(doi)),
        ]
        results = await asyncio.gather(*tasks)
        return select_best_abstract(result for result in results if looks_like_abstract(result))

    async def close(self) -> None:
        await self.client.aclose()
