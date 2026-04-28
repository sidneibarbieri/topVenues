"""NDSS abstract extractor."""

from typing import TYPE_CHECKING

from .base import AbstractExtractor

if TYPE_CHECKING:
    from ..collector import Collector


class NDSSExtractor(AbstractExtractor):
    """Extractor for NDSS papers."""

    def __init__(self, timeout_seconds: int = 30):
        super().__init__(timeout_seconds)
        self.source_name = "NDSS"
        self.xpaths = [
            'normalize-space(string-join((//div[@class="paper-data"]//p)[position() > 1], " "))',
            'normalize-space(string-join(//div[@id="abstract"]//p, " "))',
            'normalize-space(string-join(//section[contains(@class,"abstract")]//p, " "))',
            'normalize-space(string-join(//div[contains(@class,"abstract")]//p, " "))',
        ]

    async def extract(
        self,
        paper_url: str,
        paper_id: str,
        collector: "Collector",
    ) -> str | None:
        user_agent = self._get_random_user_agent(collector)
        for xpath in self.xpaths:
            result = await self._run_xidel(paper_url, user_agent, xpath)
            if result and self._is_valid_abstract(result):
                return self._clean_abstract(result)
        return None
