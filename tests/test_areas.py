"""Tests for venue-to-area mapping."""

from __future__ import annotations

from src.areas import SECURITY, UNKNOWN, area_distribution, area_for


def test_area_for_known_and_unknown_venues() -> None:
    assert area_for("ACM CCS") == SECURITY
    assert area_for("usenix security") == SECURITY  # case-insensitive
    assert area_for("HotNets") == "networks"
    assert area_for("NeurIPS") == "ai"
    assert area_for("ACM Computing Surveys") == "cross-area"
    assert area_for("Some Unconfigured Venue") == UNKNOWN
    assert area_for(None) == UNKNOWN


def test_area_distribution_counts_per_area() -> None:
    dist = area_distribution(["ACM CCS", "IEEE S&P", "HotNets", None])
    assert dist == {SECURITY: 2, "networks": 1, UNKNOWN: 1}
