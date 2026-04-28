"""Async BibTeX fetcher backed by the DBLP ``.bib`` endpoint.

DBLP exposes a stable, plain-text BibTeX endpoint per record::

    https://dblp.org/rec/{key}.bib?param=1

``param=1`` returns the standard self-contained format (no cross-ref to a
``@proceedings`` entry), which is the safest variant for consumers who copy
the ``.bib`` into a LaTeX project.

The fetcher is bounded by a :class:`asyncio.Semaphore` and uses
exponential backoff on transient errors (``ReadError``, timeouts, 5xx,
429). Failures of one request never abort the rest of the batch.
"""

import asyncio
import logging
import random
import re
from collections.abc import Callable, Iterable

import httpx

from .models import Paper

logger = logging.getLogger(__name__)

DBLP_BIB_URL = "https://dblp.org/rec/{key}.bib?param=1"
RETRY_STATUS = {429, 500, 502, 503, 504}
TRANSIENT_ERRORS = (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError,
                    httpx.PoolTimeout, httpx.ReadTimeout, httpx.ConnectTimeout)

# Rotated to look like a normal browser fleet rather than a single script,
# which DBLP's rate limiter handles more gently in practice.
USER_AGENTS = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
)

# Loose validator: a BibTeX entry must start with ``@<type>{<key>,`` and
# contain at least the ``title``, ``year`` and ``author``-or-editor fields.
_ENTRY_HEAD_RE = re.compile(r"@\w+\{[^,]+,")


def is_valid_bibtex(text: str | None) -> bool:
    """Return True when ``text`` parses as a single, plausible BibTeX entry."""
    if not text or not _ENTRY_HEAD_RE.match(text.lstrip()):
        return False
    lower = text.lower()
    return "title" in lower and "year" in lower and ("author" in lower or "editor" in lower)


class BibTeXFetcher:
    """Concurrent, retrying client for DBLP's ``.bib`` endpoint."""

    def __init__(
        self,
        concurrency: int = 4,
        request_timeout: float = 30.0,
        max_retries: int = 5,
        backoff_base: float = 3.0,
        per_request_delay: float = 0.4,
    ) -> None:
        self._semaphore = asyncio.Semaphore(concurrency)
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._per_request_delay = per_request_delay
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(request_timeout),
            limits=httpx.Limits(max_connections=concurrency,
                                max_keepalive_connections=concurrency),
            headers={"Accept": "text/x-bibtex, text/plain, */*"},
            follow_redirects=True,
        )

    async def __aenter__(self) -> "BibTeXFetcher":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._client.aclose()

    async def fetch_one(self, dblp_key: str) -> str | None:
        """Fetch the BibTeX entry for a single DBLP record key.

        Retries with exponential backoff on transient errors. Returns ``None``
        on permanent failures (404, validation reject, exhausted retries).
        """
        if not dblp_key:
            return None
        url = DBLP_BIB_URL.format(key=dblp_key)

        for attempt in range(1, self._max_retries + 1):
            async with self._semaphore:
                if self._per_request_delay > 0:
                    await asyncio.sleep(self._per_request_delay
                                        + random.uniform(0, self._per_request_delay))
                try:
                    response = await self._client.get(
                        url, headers={"User-Agent": random.choice(USER_AGENTS)}
                    )
                except TRANSIENT_ERRORS as exc:
                    if attempt == self._max_retries:
                        logger.warning("DBLP transient error for %s: %s", dblp_key, exc)
                        return None
                    await asyncio.sleep(self._backoff_base ** attempt + random.uniform(0, 1))
                    continue

            if response.status_code in RETRY_STATUS:
                if attempt == self._max_retries:
                    logger.warning("DBLP %s exhausted for %s", response.status_code, dblp_key)
                    return None
                await asyncio.sleep(self._backoff_base ** attempt + random.uniform(0, 1))
                continue
            if response.status_code != 200:
                return None

            bibtex = response.text.strip()
            return bibtex if is_valid_bibtex(bibtex) else None
        return None

    async def fetch_many(
        self,
        papers: Iterable[Paper],
        on_result: Callable[[Paper, str | None], None] | None = None,
    ) -> dict[str, str]:
        """Fetch BibTeX for every paper that has a DBLP key.

        ``on_result`` is invoked per completion with ``(paper, bibtex_or_none)``
        so the caller can persist incrementally.
        """
        targets = [paper for paper in papers if paper.key]

        async def _resolve(paper: Paper) -> tuple[Paper, str | None]:
            try:
                bibtex = await self.fetch_one(paper.key or "")
            except Exception as exc:
                logger.warning("Unexpected error for %s: %s", paper.paper_id, exc)
                bibtex = None
            if on_result:
                on_result(paper, bibtex)
            return paper, bibtex

        results: dict[str, str] = {}
        for coro in asyncio.as_completed([_resolve(p) for p in targets]):
            paper, bibtex = await coro
            if bibtex:
                results[paper.paper_id] = bibtex
        return results


def cite_key(bibtex: str | None) -> str | None:
    """Pull the entry key out of a BibTeX string."""
    if not bibtex:
        return None
    match = re.search(r"@\w+\{([^,\s]+)\s*,", bibtex)
    return match.group(1) if match else None
