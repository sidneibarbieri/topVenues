"""Shared quality gate for fetched abstracts.

Some sources (notably CrossRef for ACL Anthology records) return an author
list and proceedings title in the abstract field instead of a real abstract.
Storing those pollutes search and topic analysis, so every fetched abstract
passes through :func:`looks_like_abstract` before it reaches the database.
"""

from __future__ import annotations

import html
import re
from collections.abc import Iterable

MIN_ABSTRACT_LENGTH = 100

# "Jane Doe, John Roe, ... . Proceedings of the ..." — the ACL Anthology
# metadata string that CrossRef sometimes returns in place of an abstract.
# A name token is a capitalized word or single initial (e.g. "S", "Sakshi").
_NAME = r"[A-Z][\w.'-]*"
_AUTHOR_LIST_PROCEEDINGS = re.compile(
    rf"^(?:{_NAME} )+{_NAME}"  # a full name
    rf"(?:,\s*(?:and\s+)?(?:{_NAME} )+{_NAME})*"  # more names, comma-separated
    r"\.?\s*Proceedings of",
    re.IGNORECASE,
)

_BLOCK_END = re.compile(r"</(?:p|div|section|li|jats:p)>\s*", re.IGNORECASE)
_LINE_BREAK = re.compile(r"<br\s*/?>", re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")
_TRUNCATED_END = re.compile(r"(?:\.{3}|…|[,;:\-])(?:[\"')\]]*)$")
_TERMINAL_END = re.compile(r"[.!?](?:[\"')\]]*)$")


def normalize_abstract_text(text: str | None) -> str | None:
    """Normalize prose while preserving paragraph boundaries exposed by a source."""
    if not text:
        return None
    normalized = html.unescape(text).replace("\\n", "\n")
    normalized = _BLOCK_END.sub("\n\n", normalized)
    normalized = _LINE_BREAK.sub("\n", normalized)
    normalized = _TAG.sub(" ", normalized)
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = re.split(r"\n\s*\n+", normalized)
    cleaned = [re.sub(r"\s+", " ", paragraph).strip() for paragraph in paragraphs]
    result = "\n\n".join(paragraph for paragraph in cleaned if paragraph)
    return result or None


def looks_like_abstract(text: str | None) -> bool:
    """Return True when ``text`` reads like a real abstract, not metadata.

    Rejects empty/short text, author-list-plus-proceedings strings, and text
    with no lowercase prose (e.g. an all-caps title echoed back).
    """
    if not text:
        return False
    stripped = text.strip()
    if len(stripped) < MIN_ABSTRACT_LENGTH:
        return False
    if _AUTHOR_LIST_PROCEEDINGS.match(stripped):
        return False
    letters = [character for character in stripped if character.isalpha()]
    if not letters:
        return False
    lowercase_ratio = sum(character.islower() for character in letters) / len(letters)
    return lowercase_ratio >= 0.5


def abstract_quality_key(text: str | None) -> tuple[int, int, int, int, int, str]:
    """Rank abstract candidates by validity and likely completeness, deterministically."""
    normalized = normalize_abstract_text(text) or ""
    stripped = normalized.strip()
    likely_complete = bool(_TERMINAL_END.search(stripped)) and not bool(
        _TRUNCATED_END.search(stripped)
    )
    return (
        int(looks_like_abstract(normalized)),
        int(likely_complete),
        len(normalized.split()),
        normalized.count("\n\n") + int(bool(normalized)),
        len(normalized),
        normalized,
    )


def select_best_abstract(candidates: Iterable[str | None]) -> str | None:
    """Return the strongest normalized candidate without conflating length with quality."""
    normalized = [normalize_abstract_text(candidate) for candidate in candidates]
    available = [candidate for candidate in normalized if candidate]
    return max(available, key=abstract_quality_key) if available else None
