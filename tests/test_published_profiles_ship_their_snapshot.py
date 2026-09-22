"""A profile a published paper names must ship the snapshot it names.

Dropping data/profiles/security-20/papers.db.gz left the profile's manifest and
config in place, so nothing failed until a reviewer ran the command the
tools-track paper prints and hit FileNotFoundError. Adding a newer profile is
never a reason to remove one a published paper still tells readers to run.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ROOT / "data" / "profiles"


@dataclass(frozen=True)
class PublishedProfile:
    """A profile some published text depends on, and the text that depends on it."""

    name: str
    required_by: str


PUBLISHED_PROFILES = (
    PublishedProfile(
        "security-20",
        "the SBSeg 2026 tools-track paper, which prints "
        "`bash reproduce.sh --profile security-20` as the reviewer's command",
    ),
    PublishedProfile(
        "security-20-v4",
        "the current release, which reproduce.sh runs when given no profile",
    ),
)


@pytest.mark.parametrize("profile", PUBLISHED_PROFILES, ids=lambda item: item.name)
def test_the_snapshot_is_present(profile: PublishedProfile):
    snapshot = PROFILES / profile.name / "papers.db.gz"
    assert snapshot.exists(), f"{snapshot} is required by {profile.required_by}"


@pytest.mark.parametrize("profile", PUBLISHED_PROFILES, ids=lambda item: item.name)
def test_the_manifest_is_present(profile: PublishedProfile):
    manifest = PROFILES / profile.name / "manifest.json"
    assert manifest.exists(), f"{manifest} is required by {profile.required_by}"
