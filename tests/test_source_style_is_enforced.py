"""Style is checked by the gate, not by reviewers reading diffs.

Without this the tree drifts: `ruff format` was never part of the gate, so 54
files had accumulated layout differences that show up as noise in every diff
and hide the lines that actually changed.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run_ruff(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ruff", *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def test_the_tree_is_formatted():
    result = _run_ruff("format", "--check", ".")
    assert result.returncode == 0, result.stdout or result.stderr


def test_the_tree_passes_lint():
    result = _run_ruff("check", ".")
    assert result.returncode == 0, result.stdout or result.stderr
