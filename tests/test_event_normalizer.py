"""Venue strings must normalize to the event that published the paper."""

from __future__ import annotations

import pytest

from src.event_normalizer import EventNormalizer


@pytest.mark.parametrize(
    ("venue", "event"),
    [
        ("USENIX Security Symposium", "USENIX Security"),
        ("WOOT @ USENIX Security Symposium", "USENIX WOOT"),
        ("AISec@CCS", "ACM AISec"),
        ("CCS", "ACM CCS"),
        ("NDSS", "NDSS"),
    ],
)
def test_venue_normalizes_to_its_event(venue: str, event: str) -> None:
    assert EventNormalizer().normalize(venue) == event


def test_a_colocated_workshop_is_not_filed_under_its_host() -> None:
    """Security-20 filed 49 WOOT papers as USENIX Security because the host matched first."""
    assert (
        EventNormalizer().normalize("Unknown Workshop @ USENIX Security Symposium")
        != "USENIX Security"
    )
