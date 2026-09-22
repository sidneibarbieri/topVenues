import asyncio
from unittest.mock import AsyncMock

from src.abstract_fetcher import AbstractFetcher


def test_fetch_all_waits_for_sources_and_selects_complete_candidate() -> None:
    complete = (
        "We present a reproducible security analysis with controlled experiments, "
        "transparent measurements, and evidence that supports the reported conclusion."
    )
    truncated = (
        "We present a longer security analysis with controlled experiments, multiple datasets, "
        "extensive baselines, practitioner interviews, and detailed evidence that continues..."
    )
    fetcher = object.__new__(AbstractFetcher)
    fetcher.fetch_semanticscholar = AsyncMock(return_value=truncated)
    fetcher.fetch_openalex = AsyncMock(return_value=complete)
    fetcher.fetch_crossref = AsyncMock(return_value=None)

    selected = asyncio.run(fetcher.fetch_all("10.1145/example"))

    assert selected == complete
    fetcher.fetch_semanticscholar.assert_awaited_once()
    fetcher.fetch_openalex.assert_awaited_once()
    fetcher.fetch_crossref.assert_awaited_once()
