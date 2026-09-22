"""Regenerate Table 2 of the paper and compare it against the published values.

The reviewer instructions ask that the artifact let a reviewer reproduce the
article's tables, not only its prose numbers. This rebuilds the abstract-coverage
table from the snapshot and prints the published value beside the observed one,
so a mismatch is visible per row rather than hidden in a total.

    python scripts/reproduce_paper_table2.py --profile security-20
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.areas import area_for  # noqa: E402
from src.profiles import PROFILE_IDS, verified_profile_snapshot  # noqa: E402

CLAIMED_PROFILE = "security-20"
CAPTION = "Table 2 — Abstract coverage in the security core (July 2026 snapshot)"

# The rows the paper prints, in the order it prints them.
NAMED_VENUES = (
    "ACM CCS",
    "USENIX Security",
    "TrustCom",
    "NDSS",
    "IEEE S&P",
    "ACM ASIA CCS",
    "ESORICS",
    "ACSAC",
)
PUBLISHED = {
    "ACM CCS": (4329, 4090),
    "USENIX Security": (2542, 2056),
    "TrustCom": (2311, 1914),
    "NDSS": (1535, 1534),
    "IEEE S&P": (1420, 1165),
    "ACM ASIA CCS": (1062, 957),
    "ESORICS": (598, 39),
    "ACSAC": (593, 593),
    "Other nine venues": (2416, 1942),
    "Security core": (16806, 14290),
}


def _security_core(connection: sqlite3.Connection) -> dict[str, tuple[int, int]]:
    rows = connection.execute(
        "SELECT event, COUNT(*), SUM(CASE WHEN abstract IS NOT NULL "
        "AND TRIM(abstract) != '' THEN 1 ELSE 0 END) FROM papers GROUP BY event"
    ).fetchall()
    return {
        event: (papers, abstracts)
        for event, papers, abstracts in rows
        if area_for(event) == "security"
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=CAPTION)
    parser.add_argument("--profile", choices=PROFILE_IDS, default=CLAIMED_PROFILE)
    arguments = parser.parse_args()

    if arguments.profile != CLAIMED_PROFILE:
        print(
            f"{CAPTION} was measured on {CLAIMED_PROFILE}; "
            f"the paper states no table for {arguments.profile}."
        )
        return 0

    with verified_profile_snapshot(arguments.profile, ROOT) as verified:
        uri = f"file:{verified.database_path.resolve()}?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True)
        try:
            core = _security_core(connection)
        finally:
            connection.close()

    others = [venue for venue in core if venue not in NAMED_VENUES]
    observed = {venue: core[venue] for venue in NAMED_VENUES}
    observed["Other nine venues"] = (
        sum(core[v][0] for v in others),
        sum(core[v][1] for v in others),
    )
    observed["Security core"] = (sum(v[0] for v in core.values()), sum(v[1] for v in core.values()))

    print(f"{CAPTION}\nProfile: {arguments.profile}\n")
    header = f"  {'Venue':<22}{'Papers':>8}{'Abstracts':>11}{'Coverage':>10}   published"
    print(header)
    print("  " + "-" * (len(header) + 4))

    mismatches = 0
    for venue, (papers, abstracts) in observed.items():
        expected = PUBLISHED[venue]
        agrees = (papers, abstracts) == expected
        mismatches += not agrees
        mark = "" if agrees else f"   != {expected[0]:,}/{expected[1]:,}"
        print(
            f"  {venue:<22}{papers:>8,}{abstracts:>11,}{abstracts / papers:>9.1%}"
            f"   {expected[0]:,}/{expected[1]:,}{mark}"
        )

    print()
    if mismatches:
        print(f"{mismatches} row(s) differ from the published table.")
        return 1
    print(f"All {len(observed)} rows reproduce the published table exactly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
