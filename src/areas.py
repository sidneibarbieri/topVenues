"""Map each venue to a research area, for area-level analytics over the corpus.

Deterministic: a venue belongs to exactly one area, keyed on the canonical event
name stored in ``papers.event``. Journals and surveys that span areas are
``cross-area``. Extend ``AREA_BY_EVENT`` when a venue is added; an unmapped venue
reports ``unknown`` so coverage gaps stay visible instead of being silently bucketed.
"""

from __future__ import annotations

import collections

SECURITY = "security"
NETWORKS = "networks"
MOBILE = "mobile"
AI = "ai"
SYSTEMS = "systems"
CROSS_AREA = "cross-area"
UNKNOWN = "unknown"

AREA_BY_EVENT: dict[str, str] = {
    # Security (the core corpus).
    "USENIX Security": SECURITY,
    "ACM CCS": SECURITY,
    "IEEE S&P": SECURITY,
    "NDSS": SECURITY,
    "ACM ASIA CCS": SECURITY,
    "ACSAC": SECURITY,
    "IEEE EURO S&P": SECURITY,
    "ACM SACMAT": SECURITY,
    "ESORICS": SECURITY,
    "ACM CODASPY": SECURITY,
    "RAID": SECURITY,
    "IEEE CNS": SECURITY,
    "USENIX WOOT": SECURITY,
    "ACM WiSec": SECURITY,
    "ACM AISec": SECURITY,
    "IEEE SaTML": SECURITY,
    "TrustCom": SECURITY,
    # Networks.
    "HotNets": NETWORKS,
    "ACM SIGCOMM": NETWORKS,
    "USENIX NSDI": NETWORKS,
    "ACM IMC": NETWORKS,
    "ACM SIGMETRICS": NETWORKS,
    # Mobile.
    "ACM MobiCom": MOBILE,
    "ACM MobiSys": MOBILE,
    "ACM HotMobile": MOBILE,
    "ACM SenSys": MOBILE,
    # Systems.
    "USENIX ATC": SYSTEMS,
    "ACM EuroSys": SYSTEMS,
    # AI (planned coverage).
    "AAAI": AI,
    "IJCAI": AI,
    "ICLR": AI,
    "ICML": AI,
    "NeurIPS": AI,
    "ACM KDD": AI,
    "ACL": AI,
    "EMNLP": AI,
    "NAACL": AI,
    # Cross-area journals / surveys.
    "ACM Computing Surveys": CROSS_AREA,
    "IEEE Communications Surveys & Tutorials": CROSS_AREA,
    "Foundations and Trends in Privacy and Security": CROSS_AREA,
}

_AREA_BY_LOWER = {event.lower(): area for event, area in AREA_BY_EVENT.items()}


def area_for(event: str | None) -> str:
    """Return the research area for a venue's canonical event name."""
    if not event:
        return UNKNOWN
    return _AREA_BY_LOWER.get(event.strip().lower(), UNKNOWN)


def area_distribution(events: list[str | None]) -> dict[str, int]:
    """Count papers per area for a sequence of venue event names."""
    counter: collections.Counter[str] = collections.Counter(area_for(event) for event in events)
    return dict(counter)
