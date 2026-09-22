"""How a corpus release is named for a reader and for an auditor.

Two audiences ask different questions. A researcher choosing whether to trust
a result asks when the corpus was captured and what it covers. An auditor
verifying a claim asks which exact snapshot produced it.

Serving both with the internal profile id ("security-20-v4") answers only the
auditor and leaks a version counter into every productivity screen, so the two
are separated here.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class ReleaseIdentity:
    """The two names a corpus release carries."""

    profile_id: str
    captured_on: str | None
    venue_count: int | None
    first_year: int | None
    last_year: int | None

    @property
    def reader_label(self) -> str:
        """What the corpus covers, for someone deciding whether to trust it."""
        parts = []
        if self.captured_on:
            parts.append(_month_and_year(self.captured_on))
        if self.venue_count:
            parts.append(f"{self.venue_count} venues")
        if self.first_year and self.last_year:
            parts.append(f"{self.first_year}–{self.last_year}")
        return " · ".join(parts) or self.profile_id

    @property
    def auditor_label(self) -> str:
        """The exact snapshot a claim is bound to."""
        return self.profile_id


def _month_and_year(iso_date: str) -> str:
    parsed = datetime.date.fromisoformat(iso_date)
    return parsed.strftime("%B %Y")


def identity_from_manifest(profile_id: str, manifest: dict) -> ReleaseIdentity:
    """Read the release identity out of a verified profile manifest."""
    snapshot = manifest.get("snapshot") or {}
    return ReleaseIdentity(
        profile_id=profile_id,
        captured_on=manifest.get("built_on"),
        venue_count=snapshot.get("venues"),
        first_year=snapshot.get("observed_year_min"),
        last_year=snapshot.get("observed_year_max"),
    )
