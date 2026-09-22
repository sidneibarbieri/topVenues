#!/usr/bin/env python3
"""Append explicit corrections when saved audit provenance is inaccurate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.manual_audit import append_audit_decision, save_audit_progress  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--progress", type=Path, required=True)
    parser.add_argument("--decision-log", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--start-sample-id", type=int, required=True)
    parser.add_argument("--end-sample-id", type=int, required=True)
    parser.add_argument("--reviewer", required=True)
    return parser.parse_args()


def _latest_events(path: Path) -> dict[int, dict[str, object]]:
    latest: dict[int, dict[str, object]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        latest[int(event["sample_id"])] = event
    return latest


def main() -> int:
    args = parse_args()
    frame = pd.read_csv(args.progress, keep_default_na=False)
    latest = _latest_events(args.decision_log)
    target_ids = range(args.start_sample_id, args.end_sample_id + 1)
    corrected = 0

    for sample_id in target_ids:
        matches = frame.index[frame["sample_id"] == sample_id].tolist()
        if len(matches) != 1:
            raise ValueError(f"expected one progress row for sample_id {sample_id}")
        prior = latest.get(sample_id)
        if prior is None:
            raise ValueError(f"missing prior decision event for sample_id {sample_id}")
        row_index = matches[0]
        frame.loc[row_index, "decision_mode"] = "human_supervised_codex_assisted"
        frame.loc[row_index, "reviewer"] = args.reviewer
        already_correct = (
            prior.get("decision_mode") == "human_supervised_codex_assisted"
            and prior.get("reviewer") == args.reviewer
        )
        if already_correct:
            continue
        correction = append_audit_decision(
            frame.loc[row_index],
            profile_id=args.profile,
            sample_size=len(frame),
            progress_path=args.progress,
            decision_log_path=args.decision_log,
            source_mode=str(prior["source_mode"]),
            event_type="provenance_correction",
            supersedes_decision_id=str(prior["decision_id"]),
        )
        latest[sample_id] = correction.model_dump(mode="json")
        corrected += 1

    save_audit_progress(frame, args.progress)
    print(f"appended {corrected} provenance correction event(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
