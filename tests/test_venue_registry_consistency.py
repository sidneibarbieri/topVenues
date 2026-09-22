"""Cross-registry consistency for the venue vocabulary.

The normalizer, the area map, and the tier map are deliberately decoupled modules,
but they share one implicit contract: every canonical name the normalizer can emit
must be classifiable by *both* the area registry and the tier registry. A venue added
to one module but not the others would silently report ``unknown`` downstream. These
tests make that contract executable so the gap surfaces at test time, not in a table.
"""

from __future__ import annotations

from src.areas import AREA_BY_EVENT, area_for
from src.areas import UNKNOWN as AREA_UNKNOWN
from src.event_normalizer import EventNormalizer
from src.tiers import UNKNOWN as TIER_UNKNOWN
from src.tiers import tier_for


def _canonical_names() -> set[str]:
    """Every canonical name the normalizer's rules can produce."""
    return {
        rule.normalized_name for rule in EventNormalizer().rules if hasattr(rule, "normalized_name")
    }


def test_every_canonical_name_has_an_area_and_a_tier() -> None:
    missing_area = sorted(c for c in _canonical_names() if area_for(c) == AREA_UNKNOWN)
    missing_tier = sorted(c for c in _canonical_names() if tier_for(c) == TIER_UNKNOWN)
    assert not missing_area, f"canonical names without an area: {missing_area}"
    assert not missing_tier, f"canonical names without a tier: {missing_tier}"


def test_area_and_tier_registries_cover_the_same_venues() -> None:
    # Both classifiers should know the same vocabulary; a venue in one but not the
    # other is a maintenance gap even before the normalizer can emit it.
    from src.tiers import TIER_BY_EVENT

    area_only = sorted(set(AREA_BY_EVENT) - set(TIER_BY_EVENT))
    tier_only = sorted(set(TIER_BY_EVENT) - set(AREA_BY_EVENT))
    assert not area_only, f"venues with an area but no tier: {area_only}"
    assert not tier_only, f"venues with a tier but no area: {tier_only}"


def test_usenix_security_rule_does_not_capture_other_usenix_venues() -> None:
    # Regression: the generic "usenix" rule used to swallow ATC/NSDI/WOOT.
    normalizer = EventNormalizer()
    assert normalizer.normalize("USENIX Annual Technical Conference") == "USENIX ATC"
    assert normalizer.normalize("USENIX Security Symposium") == "USENIX Security"
    assert normalizer.normalize("USENIX NSDI") == "USENIX NSDI"
