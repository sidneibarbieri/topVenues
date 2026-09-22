"""Check each claim the tools-track paper makes against the released snapshot.

The paper states its numbers in prose. This turns every one of them into an
assertion a reviewer can run, so the mapping from a sentence in the paper to
evidence in the artifact is executable rather than something to take on trust.

    python scripts/verify_paper_claims.py
    python scripts/verify_paper_claims.py --profile security-20
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profiles import PROFILE_IDS, verified_profile_snapshot  # noqa: E402

PAPER = "TopVenues: An Executable Corpus and Research Tool for Cybersecurity Literature Reviews"


# Every number the paper prints was measured on one named snapshot. Checking a
# claim against a different profile compares it to a corpus the paper never
# described, so each claim carries the profile it is bound to.
CLAIMED_PROFILE = "security-20"


@dataclass(frozen=True)
class Claim:
    """One sentence in the paper, and the query that settles it."""

    number: str
    section: str
    statement: str
    query: str
    expected: object
    profile: str = CLAIMED_PROFILE


CLAIMS = (
    Claim(
        "1",
        "Sec. 4 (The Corpus as an Artifact)",
        "The released snapshot contains 20,305 papers",
        "SELECT COUNT(*) FROM papers",
        20305,
    ),
    Claim(
        "2",
        "Sec. 4 (The Corpus as an Artifact)",
        "over 2017-2026",
        "SELECT MIN(year) || '-' || MAX(year) FROM papers",
        "2017-2026",
    ),
    Claim(
        "3",
        "Sec. 3 (Design)",
        "20 declared venues define the scientific scope",
        "SELECT COUNT(DISTINCT event) FROM papers",
        20,
    ),
    Claim(
        "4",
        "Sec. 4 (Coverage)",
        "17,491 of 20,305 records carry an abstract",
        "SELECT COUNT(*) FROM papers WHERE abstract IS NOT NULL AND TRIM(abstract) != ''",
        17491,
    ),
    Claim(
        "5",
        "Sec. 4 (Coverage)",
        "abstract coverage is 86.1 percent",
        "SELECT ROUND(100.0 * SUM(CASE WHEN abstract IS NOT NULL AND TRIM(abstract) != ''"
        " THEN 1 ELSE 0 END) / COUNT(*), 1) FROM papers",
        86.1,
    ),
    Claim(
        "6",
        "Sec. 5 (Exports)",
        "every record carries a BibTeX entry",
        "SELECT COUNT(*) FROM papers WHERE bibtex IS NOT NULL AND TRIM(bibtex) != ''",
        20305,
    ),
)


def _run(connection: sqlite3.Connection, claim: Claim) -> tuple[bool, object]:
    observed = connection.execute(claim.query).fetchone()[0]
    return observed == claim.expected, observed


def main() -> int:
    parser = argparse.ArgumentParser(description=f"Verify the claims of: {PAPER}")
    parser.add_argument("--profile", choices=PROFILE_IDS, default="security-20")
    arguments = parser.parse_args()

    print(f"Paper: {PAPER}")
    print(f"Profile under test: {arguments.profile}\n")

    applicable = [claim for claim in CLAIMS if claim.profile == arguments.profile]
    if not applicable:
        print(f"  The paper states no claim about profile {arguments.profile}.")
        print(f"  Its numbers are bound to {CLAIMED_PROFILE}; run:")
        print(f"    python scripts/verify_paper_claims.py --profile {CLAIMED_PROFILE}")
        return 0

    failures = 0
    with verified_profile_snapshot(arguments.profile, ROOT) as verified:
        uri = f"file:{verified.database_path.resolve()}?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True)
        try:
            for claim in applicable:
                passed, observed = _run(connection, claim)
                failures += not passed
                mark = "ok  " if passed else "FAIL"
                print(f"  {mark} Claim #{claim.number}  {claim.statement}")
                print(
                    f"       {claim.section} | expected {claim.expected!r}, observed {observed!r}"
                )
        finally:
            connection.close()

    print()
    if failures:
        print(f"{failures} of {len(applicable)} claims do not hold for {arguments.profile}.")
        return 1
    print(f"All {len(applicable)} paper claims hold for profile {arguments.profile}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
