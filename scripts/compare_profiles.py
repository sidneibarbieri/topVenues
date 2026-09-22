#!/usr/bin/env python3
"""Compare two verified immutable profiles before promoting a successor."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profile_comparison import compare_databases  # noqa: E402
from src.profiles import PROFILE_IDS, verified_profile_snapshot  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("previous", choices=PROFILE_IDS)
    parser.add_argument("successor", choices=PROFILE_IDS)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with (
        verified_profile_snapshot(args.previous, ROOT) as previous,
        verified_profile_snapshot(args.successor, ROOT) as successor,
    ):
        comparison = compare_databases(previous.database_path, successor.database_path)
    payload = comparison.model_dump_json(indent=2)
    if args.output:
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
