#!/usr/bin/env python3
"""Evaluate a portable watchlist against a verified corpus profile."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profiles import DEFAULT_PROFILE_ID, verified_profile_snapshot  # noqa: E402
from src.research_intelligence import (  # noqa: E402
    ResearchWatchlist,
    unseen_watchlist_matches,
    watchlist_matching_ids,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("watchlist", type=Path)
    parser.add_argument("--profile", default=DEFAULT_PROFILE_ID)
    args = parser.parse_args()
    watchlist = ResearchWatchlist.model_validate_json(args.watchlist.read_text())
    with verified_profile_snapshot(args.profile, ROOT) as verified:
        matches = watchlist_matching_ids(verified.database_path, watchlist)
        unseen = unseen_watchlist_matches(verified.database_path, watchlist)
    print(
        json.dumps(
            {
                "watchlist": watchlist.name,
                "baseline_profile": watchlist.profile_id,
                "evaluated_profile": args.profile,
                "matches": len(matches),
                "unseen": len(unseen),
                "unseen_paper_ids": unseen,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
