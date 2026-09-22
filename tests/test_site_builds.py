"""The project page is built from this repository, never typed by hand.

Its figures about the current release come from the profile manifest at build
time. The two papers are frozen, so their values are constants in the build
script; they must agree with docs/PAPERS.md, the registry readers are sent to.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from tests.repository_only import skip_unless_repository

skip_unless_repository()

ROOT = Path(__file__).resolve().parents[1]


def _builder():
    specification = importlib.util.spec_from_file_location(
        "build_site", ROOT / "site" / "build_site.py"
    )
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_the_page_builds_with_every_placeholder_filled(tmp_path):
    index = _builder().build(ROOT, tmp_path)
    page = index.read_text(encoding="utf-8")
    assert not re.search(r"\{\{[A-Z_0-9]+\}\}", page)
    assert (tmp_path / "assets" / "brand" / "topvenues-app-icon.svg").is_file()


def test_the_frozen_paper_values_match_the_registry():
    registry = (ROOT / "docs" / "PAPERS.md").read_text(encoding="utf-8")
    for paper in _builder().PAPERS:
        for value in (paper.sha256, paper.release, paper.doi, paper.sol_url, paper.pages):
            assert value in registry, f"{value} of {paper.release} is not in PAPERS.md"
        assert f"{paper.records:,}" in registry
        for block in paper.commands:
            assert block.text in registry, f"a command of {paper.release} is not in PAPERS.md"
