#!/usr/bin/env python3
"""Report where the active corpus has no abstract, grouped by source.

The gap matters for interpreting any abstract-dependent query, so it is
reported from the snapshot rather than repeated from documentation that can
drift away from the data.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profiles import select_profile_id, verified_profile_snapshot  # noqa: E402

OPEN_ACCESS_HOSTS = ("usenix.org", "ndss-symposium")

SOURCE_QUERY = """
SELECT
    CASE
        WHEN ee LIKE '%usenix.org%' THEN 'USENIX'
        WHEN ee LIKE '%ndss-symposium%' THEN 'NDSS'
        WHEN ee LIKE '%doi.org/10.1007%' OR event = 'ESORICS' THEN 'Springer'
        WHEN ee LIKE '%doi.org/10.1145%' THEN 'ACM Digital Library'
        WHEN ee LIKE '%doi.org/10.1109%' THEN 'IEEE Xplore'
        ELSE 'no resolvable link'
    END AS source,
    COUNT(*)
FROM papers
WHERE abstract IS NULL OR TRIM(abstract) = ''
GROUP BY source
ORDER BY 2 DESC
"""


def main() -> int:
    profile_id = select_profile_id()
    with verified_profile_snapshot(profile_id) as verified:
        connection = sqlite3.connect(verified.database_path)
        try:
            total, with_abstract = connection.execute(
                "SELECT COUNT(*), SUM(CASE WHEN abstract IS NOT NULL AND TRIM(abstract) <> ''"
                " THEN 1 ELSE 0 END) FROM papers"
            ).fetchone()
            rows = connection.execute(SOURCE_QUERY).fetchall()
        finally:
            connection.close()

    missing = total - with_abstract
    print(f"profile {profile_id}: {with_abstract:,} of {total:,} records carry an abstract")
    print(f"missing: {missing:,} ({100 * missing / total:.1f}%)\n")
    print(f"{'source':<22}{'missing':>9}  recoverable")
    recoverable = 0
    for source, count in rows:
        is_open = source in ("USENIX", "NDSS")
        recoverable += count if is_open else 0
        print(f"{source:<22}{count:>9}  {'yes, open access' if is_open else 'no, subscription'}")
    print(f"\nrecoverable without institutional risk: {recoverable:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
