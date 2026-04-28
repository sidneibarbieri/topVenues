"""ACM Digital Library abstract extractor."""

import re
from typing import TYPE_CHECKING

from .base import AbstractExtractor

if TYPE_CHECKING:
    from ..collector import Collector


class ACMExtractor(AbstractExtractor):
    """Extractor for ACM papers (CCS, ASIA CCS, SACMAT, HotNets, ACM CSUR)."""

    def __init__(self, timeout_seconds: int = 30):
        super().__init__(timeout_seconds)
        self.source_name = "ACM"

    async def extract(
        self,
        paper_url: str,
        paper_id: str,
        collector: "Collector",
    ) -> str | None:
        if collector.is_acm_blocked():
            return None

        failure_count = collector.get_acm_failure_count(paper_url)
        if failure_count >= collector.config.acm_failure_threshold:
            return None

        user_agent = self._get_random_user_agent(collector)
        xpaths = [
            'normalize-space(string(//*[@id="abstract"]))',
            'normalize-space(string(//div[contains(@class,"abstract")]))',
            'normalize-space(string(//section[contains(@class,"abstract")]))',
            'normalize-space(string(//div[contains(@class,"abstractInFull")]))',
            'normalize-space(string(//div[@class="abstractSection"]))',
            'normalize-space(string(//div[contains(@class,"abstractText")]))',
        ]

        for xpath in xpaths:
            result = await self._run_xidel(paper_url, user_agent, xpath)
            if result:
                result = re.sub(r"^Abstract\s*[:\?\-]?\s*", "", result, flags=re.IGNORECASE)
                result = re.sub(r"\s+", " ", result).strip()

                if self._is_valid_abstract(result):
                    cleaned = self._clean_abstract(result)
                    if cleaned:
                        collector.reset_acm_failure_count(paper_url)
                        return cleaned

        collector.increment_acm_failure_count(paper_url)
        return None
