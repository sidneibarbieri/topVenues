#!/usr/bin/env python3
"""Generate or summarize a human abstract-quality audit."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.manual_audit import build_audit_sample, summarize_audit  # noqa: E402
from src.profiles import DEFAULT_PROFILE_ID, verified_profile_snapshot  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=DEFAULT_PROFILE_ID)
    parser.add_argument("--sample-size", type=int, default=200)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--summarize", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.summarize:
        summary = summarize_audit(pd.read_csv(args.summarize, keep_default_na=False))
        print(summary.model_dump_json(indent=2))
        return 0

    output = args.output or Path(f"{args.profile}-manual-audit.csv")
    with verified_profile_snapshot(args.profile, ROOT) as verified:
        sample = build_audit_sample(verified.database_path, sample_size=args.sample_size)
    sample.to_csv(output, index=False)
    print(f"wrote {len(sample)} deterministic audit rows to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
