"""USENIX Security abstract extractor."""

from typing import TYPE_CHECKING

from .base import AbstractExtractor

if TYPE_CHECKING:
    from ..collector import Collector


class USENIXExtractor(AbstractExtractor):
    """Extractor for USENIX Security papers."""

    def __init__(self, timeout_seconds: int = 30):
        super().__init__(timeout_seconds)
        self.source_name = "USENIX"
        # Abstracts here are frequently split across several <p> elements, so
        # every selector joins the whole paragraph sequence. Selectors that can
        # only ever yield one paragraph are last resorts: reaching for them
        # first silently truncated multi-paragraph abstracts to their opening
        # paragraph.
        self.xpaths = [
            'normalize-space(string-join(//div[contains(@class,"field-name-field-paper-description")]//p, " "))',
            'normalize-space(string-join(((//main//section)[1]//p)[position() > 1 and position() < last()], " "))',
            "normalize-space(((//main//section)[1]//p)[2])",
            "normalize-space(//div[@class='content']//p[not(ancestor::header)])",
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
