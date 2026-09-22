"""Assert that the released artifact matches its snapshot manifest."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profiles import (  # noqa: E402
    PROFILE_IDS,
    select_profile_id,
    verified_profile_snapshot,
)
from src.sqlite_connection import managed_sqlite_connection  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        choices=PROFILE_IDS,
        default=select_profile_id(),
        help="immutable corpus profile (default: TOPVENUES_PROFILE or security-20)",
    )
    args = parser.parse_args()

    with verified_profile_snapshot(
        args.profile,
        ROOT,
    ) as verified:
        uri = f"file:{verified.database_path.resolve()}?mode=ro&immutable=1"
        with managed_sqlite_connection(uri, uri=True) as conn:
            total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
            with_abstract = conn.execute(
                "SELECT COUNT(*) FROM papers WHERE abstract IS NOT NULL AND TRIM(abstract) != ''"
            ).fetchone()[0]
            with_bibtex = conn.execute(
                "SELECT COUNT(*) FROM papers WHERE bibtex IS NOT NULL AND TRIM(bibtex) != ''"
            ).fetchone()[0]
        snapshot = verified.profile.manifest["snapshot"]

    checks = (
        ("papers", total, snapshot["papers"]),
        ("abstracts", with_abstract, snapshot["abstracts"]),
        ("bibtex", with_bibtex, snapshot["bibtex"]),
    )
    failures = [name for name, actual, expected in checks if actual != expected]

    print(f"  profile    {args.profile}")
    for name, actual, expected in checks:
        mark = "ok " if actual == expected else "FAIL"
        print(f"  {mark} {name:<10} {actual:>5} (expected {expected})")

    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
