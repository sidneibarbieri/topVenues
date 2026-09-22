"""Tests for venue-to-tier mapping."""

from __future__ import annotations

from src.tiers import (
    SECURITY_TIER_1_SCOPE,
    TOP4,
    TOP4_REGIONAL,
    UNKNOWN,
    tier_for,
    tiers_in_scope,
)


def test_tier_for_big_four_and_neighbors() -> None:
    for big_four in ("ACM CCS", "IEEE S&P", "USENIX Security", "NDSS"):
        assert tier_for(big_four) == TOP4
    assert tier_for("ACM ASIA CCS") == TOP4_REGIONAL
    assert tier_for("usenix security") == TOP4  # case-insensitive
    assert tier_for("ACSAC") == "top-tier"
    assert tier_for("HotNets") == "workshop"
    assert tier_for("ACM Computing Surveys") == "journal"
    assert tier_for("Unconfigured Venue") == UNKNOWN
    assert tier_for(None) == UNKNOWN


def test_security_tier_1_scope_is_exactly_the_big_four() -> None:
    assert tiers_in_scope(SECURITY_TIER_1_SCOPE) == frozenset({TOP4})
