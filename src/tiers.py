"""Map each venue to a relevance tier, for tier-aware literature work.

Deterministic: a venue has exactly one tier, keyed on the canonical event name in
``papers.event``. The security top-4 venues are the reference top tier; their
regional editions and the top venues of adjacent areas sit just below. Extend
``TIER_BY_EVENT`` when a venue is added; an unmapped venue reports ``unknown`` so
coverage gaps stay visible.
"""

from __future__ import annotations

# Tier labels, ordered strongest first.
TOP4 = "top-4"  # the four top-ranked security venues
TOP4_REGIONAL = "top-4-regional"  # regional editions of a top-4 venue
TOP_TIER = "top-tier"  # A* venue, top of its (non-security) area or strong security
STRONG = "strong"  # solid A/B venue
WORKSHOP = "workshop"
JOURNAL = "journal"
UNKNOWN = "unknown"

ALL_TIERS_SCOPE = "All declared venues"
SECURITY_TIER_1_SCOPE = "Security top-4"
REGIONAL_TIER_1_SCOPE = "Tier 1 plus regional editions"
OTHER_TOP_TIER_SCOPE = "Other top-tier venues"
STRONG_SCOPE = "Strong venues"
SURVEY_SCOPE = "Survey journals"

TIER_BY_EVENT: dict[str, str] = {
    # The security top-4.
    "ACM CCS": TOP4,
    "IEEE S&P": TOP4,
    "USENIX Security": TOP4,
    "NDSS": TOP4,
    # Regional editions of a top-4 venue.
    "ACM ASIA CCS": TOP4_REGIONAL,
    "IEEE EURO S&P": TOP4_REGIONAL,
    # Top tier of an area (security and adjacent fields).
    "ACSAC": TOP_TIER,
    "RAID": TOP_TIER,
    "ESORICS": TOP_TIER,
    "ACM SIGCOMM": TOP_TIER,
    "USENIX NSDI": TOP_TIER,
    "ACM IMC": TOP_TIER,
    "ACM SIGMETRICS": TOP_TIER,
    "ACM MobiCom": TOP_TIER,
    "ACM MobiSys": TOP_TIER,
    "USENIX ATC": TOP_TIER,
    "ACM EuroSys": TOP_TIER,
    "NeurIPS": TOP_TIER,
    "ICML": TOP_TIER,
    "ICLR": TOP_TIER,
    "AAAI": TOP_TIER,
    "IJCAI": TOP_TIER,
    "ACM KDD": TOP_TIER,
    "ACL": TOP_TIER,
    "EMNLP": TOP_TIER,
    # Solid venues.
    "IEEE CNS": STRONG,
    "ACM CODASPY": STRONG,
    "ACM WiSec": STRONG,
    "ACM SACMAT": STRONG,
    "IEEE SaTML": STRONG,
    "USENIX WOOT": STRONG,
    "TrustCom": STRONG,
    "ACM SenSys": STRONG,
    "NAACL": STRONG,
    # Workshops.
    "HotNets": WORKSHOP,
    "ACM HotMobile": WORKSHOP,
    "ACM AISec": WORKSHOP,
    # Journals.
    "ACM Computing Surveys": JOURNAL,
    "IEEE Communications Surveys & Tutorials": JOURNAL,
    "Foundations and Trends in Privacy and Security": JOURNAL,
}

# Author-ranking weight per tier. The scale encodes the review heuristic that
# a top-4 paper is worth several strong-venue papers: a name that recurs in
# the top tier is far more representative of a topic than one that recurs in
# tier two. Survey journals sit between the two because they anchor review work.
WEIGHT_BY_TIER: dict[str, float] = {
    TOP4: 5.0,
    TOP4_REGIONAL: 3.0,
    TOP_TIER: 3.0,
    JOURNAL: 2.0,
    STRONG: 1.5,
    WORKSHOP: 0.5,
    UNKNOWN: 0.25,
}


def weight_for(tier: str) -> float:
    """Return the author-ranking weight for a tier label."""
    return WEIGHT_BY_TIER.get(tier, WEIGHT_BY_TIER[UNKNOWN])


_TIER_BY_LOWER = {event.lower(): tier for event, tier in TIER_BY_EVENT.items()}


def tier_for(event: str | None) -> str:
    """Return the relevance tier for a venue's canonical event name."""
    if not event:
        return UNKNOWN
    return _TIER_BY_LOWER.get(event.strip().lower(), UNKNOWN)


def tier_scope_options() -> tuple[str, ...]:
    """Return researcher-facing scopes in a stable, non-overlapping order."""
    return (
        ALL_TIERS_SCOPE,
        SECURITY_TIER_1_SCOPE,
        REGIONAL_TIER_1_SCOPE,
        OTHER_TOP_TIER_SCOPE,
        STRONG_SCOPE,
        SURVEY_SCOPE,
    )


def tiers_in_scope(scope: str) -> frozenset[str] | None:
    """Map a declared UI scope to tiers; ``None`` means no tier restriction."""
    scopes = {
        ALL_TIERS_SCOPE: None,
        SECURITY_TIER_1_SCOPE: frozenset({TOP4}),
        REGIONAL_TIER_1_SCOPE: frozenset({TOP4, TOP4_REGIONAL}),
        OTHER_TOP_TIER_SCOPE: frozenset({TOP_TIER}),
        STRONG_SCOPE: frozenset({STRONG}),
        SURVEY_SCOPE: frozenset({JOURNAL}),
    }
    try:
        return scopes[scope]
    except KeyError as error:
        raise ValueError(f"unknown tier scope {scope!r}") from error
