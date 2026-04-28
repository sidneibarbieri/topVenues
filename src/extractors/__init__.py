"""Abstract extractors for different publisher types."""

from .acm import ACMExtractor
from .base import AbstractExtractor
from .ieee import IEEEExtractor
from .ndss import NDSSExtractor
from .usenix import USENIXExtractor

__all__ = [
    "AbstractExtractor",
    "USENIXExtractor",
    "NDSSExtractor",
    "IEEEExtractor",
    "ACMExtractor",
]


def get_extractor_for_event(event: str) -> AbstractExtractor:
    """Return appropriate extractor based on event name."""
    event_lower = event.lower()

    if "usenix" in event_lower or "uss" in event_lower:
        return USENIXExtractor()
    if "ndss" in event_lower:
        return NDSSExtractor()
    if (
        "ieee" in event_lower
        or "euro s&p" in event_lower
        or "communications surveys" in event_lower
        or "comsur" in event_lower
        or "s&p" in event_lower
    ):
        return IEEEExtractor()
    return ACMExtractor()
