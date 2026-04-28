"""Base extractor with xidel runner and text cleaning."""

import asyncio
import random
import re
import subprocess
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..collector import Collector


class AbstractExtractor(ABC):
    """Base class for venue-specific extractors."""

    def __init__(self, timeout_seconds: int = 30):
        self.source_name = self.__class__.__name__
        self.timeout_seconds = timeout_seconds

    @abstractmethod
    async def extract(
        self,
        paper_url: str,
        paper_id: str,
        collector: "Collector",
    ) -> str | None:
        raise NotImplementedError

    def _get_random_user_agent(self, collector: "Collector") -> str:
        return random.choice(collector.config.user_agents)

    def _clean_abstract(self, text: str | None) -> str | None:
        if not text:
            return None
        text = re.sub(r"^([A-Z][a-z]+\s)+\([^)]+\)[,:]?\s*", "", text)
        text = re.sub(r"USENIX is committed to Open Access.*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^Abstract\s*[:\?\-]?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text)
        return text.strip() or None

    def _is_valid_abstract(self, text: str | None, min_length: int = 100) -> bool:
        if not text or len(text.strip()) < min_length:
            return False
        return text.strip().lower() not in {"true", "false", "null", "undefined", "error"}

    async def _run_xidel(self, url: str, user_agent: str, xpath: str) -> str | None:
        cmd = [
            "xidel",
            url,
            f"--user-agent={user_agent}",
            "--header=Accept: text/html,application/xhtml+xml,application/xml;q=0.9",
            "-se",
            xpath,
        ]
        try:
            proc = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                ),
                timeout=self.timeout_seconds,
            )
            stdout, _ = await proc.communicate()
            result = stdout.decode("utf-8", errors="ignore").strip()
            return result or None
        except (TimeoutError, subprocess.SubprocessError, FileNotFoundError):
            return None
