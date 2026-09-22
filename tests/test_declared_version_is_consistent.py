"""The declared version must agree everywhere a reader can read it.

v1.5.1 and v1.5.2 were tagged without bumping any of these, so the repository
told reviewers it was v1.5.0 while the tag said otherwise, and the documented
clone command checked out an older tree than the one under test.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import src

ROOT = Path(__file__).resolve().parents[1]


def _pyproject_version() -> str:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "version"
    ]


def test_package_and_project_declare_the_same_version():
    assert src.__version__ == _pyproject_version()


def test_the_readme_release_row_matches_the_package():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    row = re.search(r"^\| Tool release \| `v([0-9.]+)` \|$", readme, flags=re.M)
    assert row, "the README no longer states a tool release"
    assert row.group(1) == src.__version__


def test_every_documented_clone_checks_out_this_release():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    tags = set(re.findall(r"--branch v([0-9.]+) https://github\.com/", readme))
    assert tags == {src.__version__}, f"the README clones {sorted(tags)}"


def test_the_citation_file_declares_this_release():
    """CITATION.cff stayed at 1.3.0 through six releases; GitHub shows it verbatim."""
    cff = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    declared = re.search(r'^version: "([0-9.]+)"$', cff, flags=re.M)
    assert declared, "CITATION.cff no longer declares a version"
    assert declared.group(1) == src.__version__
