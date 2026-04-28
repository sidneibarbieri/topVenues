"""Base extractor with xidel runner and text cleaning."""

import asyncio
import html
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

    # One "Name (Affiliation)" block plus optional separator (comma / semicolon / spaces).
    _AUTHOR_BLOCK_RE = re.compile(
        r"[A-Za-zÀ-ÿ][\w\.\-'\sÀ-ÿ]*?\(([^)]{2,120})\)\s*([,;]?)\s*"
    )

    def _clean_abstract(self, text: str | None) -> str | None:
        if not text:
            return None
        text = html.unescape(text)
        text = self._strip_leading_author_blocks(text)
        text = re.sub(r"USENIX is committed to Open Access.*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^Abstract\s*[:\?\-]?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text)
        return text.strip() or None

    @classmethod
    def _strip_leading_author_blocks(cls, text: str) -> str:
        """Strip a contiguous block of "Name (Affiliation), …" pairs at the start.

        NDSS pages render author lists as ``Name (Affil), Name (Affil), … Last
        (Affil) <abstract body>`` — commas between authors, no comma before the
        body. The iteration consumes comma-terminated blocks and accepts a
        single trailing no-comma block before stopping. This means a phrase
        like ``Industrial Control Systems (ICS), consisting of …`` cannot be
        eaten: ``(ICS)`` follows a no-comma block and the loop has already
        broken.
        """
        pos = 0
        matches = 0
        while True:
            m = cls._AUTHOR_BLOCK_RE.match(text, pos)
            if not m or m.end() == pos:
                break
            separator = m.group(2)
            if separator in (",", ";"):
                pos = m.end()
                matches += 1
                if matches > 30:
                    break
                continue
            # No comma after this block — it can only be the final author of
            # the list, and only if we have already consumed at least one
            # comma-terminated block.
            if matches >= 1:
                pos = m.end()
                matches += 1
            break
        if matches < 2:
            return text
        remainder = text[pos:].lstrip()
        if len(remainder) < 100:
            return text
        return remainder

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
