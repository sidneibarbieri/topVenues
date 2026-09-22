"""Paper claims belong to the snapshot they were measured on.

Checking them against another profile compares the paper to a corpus it never
described. Run against security-20-v4, all six reported failures and the whole
reproduction exited non-zero -- for a profile the paper says nothing about.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _claims_module():
    """`scripts/` is not a package, so load the file directly."""
    spec = importlib.util.spec_from_file_location(
        "verify_paper_claims", ROOT / "scripts" / "verify_paper_claims.py"
    )
    module = importlib.util.module_from_spec(spec)
    # @dataclass resolves __module__ through sys.modules, so register it first.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _run(profile: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "scripts/verify_paper_claims.py", "--profile", profile],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def test_every_claim_names_the_profile_it_was_measured_on():
    module = _claims_module()

    assert module.CLAIMS
    assert all(claim.profile == module.CLAIMED_PROFILE for claim in module.CLAIMS)


def test_the_claimed_profile_satisfies_every_claim():
    result = _run("security-20")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "All 6 paper claims hold" in result.stdout


def test_another_profile_is_reported_as_out_of_scope_not_as_a_failure():
    result = _run("security-20-v4")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "states no claim about profile security-20-v4" in result.stdout
