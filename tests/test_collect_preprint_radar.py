"""The radar collector must cover the whole window it states, or fail."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts.collect_preprint_radar import PAGE_SIZE, RadarTruncatedError, fetch_recent

SINCE = datetime(2026, 9, 1, tzinfo=UTC)


def _atom(count: int, start: int = 0) -> str:
    entries = "".join(
        f"""<entry><id>http://arxiv.org/abs/2609.{start + index:05d}v1</id>
        <title>Preprint {start + index}</title><summary>An abstract.</summary>
        <published>2026-09-20T00:00:00Z</published><updated>2026-09-20T00:00:00Z</updated>
        <author><name>Ada Lovelace</name></author></entry>"""
        for index in range(count)
    )
    return f'<feed xmlns="http://www.w3.org/2005/Atom">{entries}</feed>'


class _Pages:
    """Serves the given page sizes in order, then empty pages."""

    def __init__(self, sizes: list[int]) -> None:
        self.sizes = sizes
        self.served = 0

    def __call__(self, url: str) -> str:
        size = self.sizes[self.served] if self.served < len(self.sizes) else 0
        payload = _atom(size, start=self.served * PAGE_SIZE)
        self.served += 1
        return payload


def test_a_short_page_ends_the_window() -> None:
    pages = _Pages([PAGE_SIZE, 37])
    preprints = fetch_recent("cs.CR", SINCE, page_limit=5, fetch_page=pages)
    assert len(preprints) == PAGE_SIZE + 37
    assert pages.served == 2


def test_running_out_of_pages_is_an_error_not_a_shorter_radar() -> None:
    """The first weekly run hit the cap and stated a window it did not cover."""
    with pytest.raises(RadarTruncatedError, match="raise --pages"):
        fetch_recent("cs.CR", SINCE, page_limit=3, fetch_page=_Pages([PAGE_SIZE] * 3))
