"""IEEE Xplore abstract extractor using xidel and JSON parsing."""

import json
import re
import subprocess
from typing import TYPE_CHECKING

from .base import AbstractExtractor

if TYPE_CHECKING:
    from ..collector import Collector


class IEEEExtractor(AbstractExtractor):
    """Extractor for IEEE S&P and Euro S&P papers."""

    def __init__(self, timeout_seconds: int = 30):
        super().__init__(timeout_seconds)
        self.source_name = "IEEE"

    async def extract(
        self,
        paper_url: str,
        paper_id: str,
        collector: "Collector",
    ) -> str | None:
        user_agent = self._get_random_user_agent(collector)
        script_content = await self._get_metadata_script(paper_url, user_agent)
        if not script_content:
            return None

        abstract = self._extract_from_regex(script_content)
        if abstract and self._is_valid_abstract(abstract):
            return self._clean_abstract(abstract)

        abstract = self._extract_from_json(script_content)
        if abstract and self._is_valid_abstract(abstract):
            return self._clean_abstract(abstract)

        return None

    async def _get_metadata_script(self, url: str, user_agent: str) -> str | None:
        """Retrieve xplGlobal.document.metadata script block via xidel."""
        xpath = "string(//script[contains(.,'xplGlobal.document.metadata')])"
        try:
            result = await self._run_xidel(url, user_agent, xpath)
            return result or None
        except (TimeoutError, subprocess.SubprocessError, FileNotFoundError):
            return None

    def _extract_from_regex(self, script_text: str) -> str | None:
        """Pull abstract value with regex pattern on raw script text."""
        # [^"\\] = any char except quote/backslash; \\. = escaped sequence.
        matches = re.findall(r'"abstract"\s*:\s*"((?:[^"\\]|\\.)+)"', script_text)
        if not matches:
            return None

        abstract = matches[0].replace('\\"', '"').replace("\\n", " ")
        return re.sub(r"\s+", " ", abstract).strip()

    def _extract_from_json(self, script_text: str) -> str | None:
        """Parse metadata JSON object and extract abstract field."""
        match = re.search(r"xplGlobal\.document\.metadata\s*=\s*(\{.+?\});", script_text, re.DOTALL)
        if not match:
            return None

        try:
            metadata = json.loads(match.group(1))
        except json.JSONDecodeError:
            return None

        abstract = metadata.get("abstract")
        if not abstract:
            return None

        abstract = abstract.replace('\\"', '"').replace("\\n", " ")
        return re.sub(r"\s+", " ", abstract).strip()
