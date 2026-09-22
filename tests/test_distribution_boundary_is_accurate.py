"""The README's distribution boundary must match what the package ships.

It claimed the package bundled only `security-20-v4` while `security-20-v3` was
also present, so a reviewer reading it would expect a fetch step that is not
needed. A prose claim about file contents goes stale the moment files move.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ROOT / "data" / "profiles"


def _bundled_profiles() -> set[str]:
    return {path.parent.name for path in PROFILES.glob("*/papers.db.gz")}


def _profiles_named_as_bundled() -> set[str]:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    section = readme.split("## Distribution boundary", 1)[1].split("\n## ", 1)[0]
    sentence = section.split(".", 1)[0]
    return set(re.findall(r"`(security-[\w-]+)`", sentence))


def test_the_readme_names_exactly_the_bundled_snapshots():
    assert _profiles_named_as_bundled() == _bundled_profiles()
