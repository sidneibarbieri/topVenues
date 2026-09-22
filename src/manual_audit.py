"""Deterministic sampling and validation for a human abstract-quality audit."""

from __future__ import annotations

import hashlib
import math
import os
import sqlite3
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel

AUDIT_LABELS = ("label_complete", "label_uncontaminated", "label_matches_paper")
AUDIT_ID_COLUMNS = ("sample_id", "paper_id")
AUDIT_DECISION_MODES = ("human_only", "human_supervised_codex_assisted")


class AuditDecision(BaseModel):
    """Append-only provenance for one saved audit decision."""

    schema_version: str = "1.1"
    decision_id: str
    timestamp_utc: datetime
    policy_version: str = "manual-abstract-audit-v3.1"
    event_type: Literal["decision", "provenance_correction"] = "decision"
    supersedes_decision_id: str | None = None
    profile_id: str
    sample_size: int
    sample_id: int
    paper_id: str
    source_url: str
    source_mode: Literal["live_publisher_or_landing_page", "crossref_deposited_metadata"]
    decision_mode: Literal["human_only", "human_supervised_codex_assisted"]
    reviewer: str
    label_complete: str
    label_uncontaminated: str
    label_matches_paper: str
    extracted_abstract_sha256: str
    notes_sha256: str
    progress_file: str


class AuditSummary(BaseModel):
    sampled: int
    labelled: int
    usable: int
    usable_rate: float | None
    ci95_low: float | None
    ci95_high: float | None


def _allocation(counts: dict[str, int], sample_size: int) -> dict[str, int]:
    """Proportional venue allocation using deterministic largest remainders."""
    total = sum(counts.values())
    if sample_size > total:
        raise ValueError("sample_size exceeds corpus size")
    exact = {venue: sample_size * count / total for venue, count in counts.items()}
    allocated = {venue: int(value) for venue, value in exact.items()}
    remaining = sample_size - sum(allocated.values())
    order = sorted(counts, key=lambda venue: (-(exact[venue] - allocated[venue]), venue))
    for venue in order[:remaining]:
        allocated[venue] += 1
    return allocated


def build_audit_sample(
    db_path: Path, *, sample_size: int = 200, seed: str = "security-20-v3-audit-1"
) -> pd.DataFrame:
    """Create a reproducible venue-stratified sample of corpus records."""
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            "SELECT paper_id, event, year, title, ee, abstract FROM papers ORDER BY paper_id"
        ).fetchall()
    counts: dict[str, int] = {}
    for _, event, *_ in rows:
        counts[event] = counts.get(event, 0) + 1
    allocated = _allocation(counts, sample_size)
    by_venue: dict[str, list[tuple]] = {venue: [] for venue in counts}
    for row in rows:
        by_venue[row[1]].append(row)

    selected: list[tuple] = []
    for venue, venue_rows in by_venue.items():
        ranked = sorted(
            venue_rows,
            key=lambda row: hashlib.sha256(f"{seed}\0{row[0]}".encode()).hexdigest(),
        )
        selected.extend(ranked[: allocated[venue]])
    selected.sort(key=lambda row: (row[1], row[0]))
    return pd.DataFrame(
        [
            {
                "sample_id": index,
                "paper_id": paper_id,
                "venue": event,
                "year": year,
                "title": title,
                "source_url": ee or "",
                "abstract_present": bool(abstract and abstract.strip()),
                "abstract": abstract or "",
                "label_complete": "",
                "label_uncontaminated": "",
                "label_matches_paper": "",
                "reviewer": "",
                "decision_mode": "",
                "notes": "",
            }
            for index, (paper_id, event, year, title, ee, abstract) in enumerate(selected, start=1)
        ]
    )


def _as_label(value: object) -> bool | None:
    normalized = str(value).strip().casefold()
    if normalized in {"true", "yes", "1", "y"}:
        return True
    if normalized in {"false", "no", "0", "n"}:
        return False
    return None


def load_audit_progress(sample: pd.DataFrame, progress_path: Path) -> pd.DataFrame:
    """Load labels only when they belong to the exact deterministic sample."""
    if not progress_path.exists():
        return sample.copy()
    progress = pd.read_csv(progress_path, keep_default_na=False)
    if "decision_mode" not in progress.columns:
        progress["decision_mode"] = progress.apply(
            lambda row: (
                "human_only"
                if all(_as_label(row[label]) is not None for label in AUDIT_LABELS)
                else ""
            ),
            axis=1,
        )
    missing_columns = set(sample.columns) - set(progress.columns)
    if missing_columns:
        raise ValueError(f"audit progress is missing columns: {sorted(missing_columns)}")
    if len(progress) != len(sample):
        raise ValueError("audit progress does not match the selected sample size")
    for column in AUDIT_ID_COLUMNS:
        if progress[column].astype(str).tolist() != sample[column].astype(str).tolist():
            raise ValueError(f"audit progress does not match the deterministic {column} sequence")
    return progress.loc[:, sample.columns].copy()


def save_audit_progress(frame: pd.DataFrame, progress_path: Path) -> None:
    """Persist human labels atomically so an interrupted write cannot corrupt progress."""
    missing_columns = set(AUDIT_LABELS + AUDIT_ID_COLUMNS) - set(frame.columns)
    if missing_columns:
        raise ValueError(f"audit progress is missing columns: {sorted(missing_columns)}")
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        dir=progress_path.parent,
        prefix=f".{progress_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as temporary_file:
        frame.to_csv(temporary_file, index=False)
        temporary_path = Path(temporary_file.name)
    os.replace(temporary_path, progress_path)


def append_audit_decision(
    frame_row: pd.Series,
    *,
    profile_id: str,
    sample_size: int,
    progress_path: Path,
    decision_log_path: Path,
    source_mode: Literal[
        "live_publisher_or_landing_page", "crossref_deposited_metadata"
    ] = "live_publisher_or_landing_page",
    event_type: Literal["decision", "provenance_correction"] = "decision",
    supersedes_decision_id: str | None = None,
) -> AuditDecision:
    """Append one replayable decision record without rewriting prior entries."""
    decision = AuditDecision(
        decision_id=str(uuid.uuid4()),
        timestamp_utc=datetime.now(UTC),
        event_type=event_type,
        supersedes_decision_id=supersedes_decision_id,
        profile_id=profile_id,
        sample_size=sample_size,
        sample_id=int(frame_row["sample_id"]),
        paper_id=str(frame_row["paper_id"]),
        source_url=str(frame_row["source_url"]),
        source_mode=source_mode,
        decision_mode=str(frame_row["decision_mode"]),
        reviewer=str(frame_row["reviewer"]),
        label_complete=str(frame_row["label_complete"]),
        label_uncontaminated=str(frame_row["label_uncontaminated"]),
        label_matches_paper=str(frame_row["label_matches_paper"]),
        extracted_abstract_sha256=hashlib.sha256(
            str(frame_row["abstract"]).encode("utf-8")
        ).hexdigest(),
        notes_sha256=hashlib.sha256(str(frame_row["notes"]).encode("utf-8")).hexdigest(),
        progress_file=str(progress_path),
    )
    decision_log_path.parent.mkdir(parents=True, exist_ok=True)
    with decision_log_path.open("a", encoding="utf-8") as decision_log:
        decision_log.write(decision.model_dump_json() + "\n")
        decision_log.flush()
        os.fsync(decision_log.fileno())
    return decision


def _wilson_interval(successes: int, total: int) -> tuple[float, float]:
    z = 1.959963984540054
    proportion = successes / total
    denominator = 1 + z**2 / total
    centre = (proportion + z**2 / (2 * total)) / denominator
    margin = (
        z * math.sqrt(proportion * (1 - proportion) / total + z**2 / (4 * total**2)) / denominator
    )
    return centre - margin, centre + margin


def summarize_audit(frame: pd.DataFrame) -> AuditSummary:
    """Summarize fully labelled rows; incomplete rows are never silently imputed."""
    missing_columns = set(AUDIT_LABELS) - set(frame.columns)
    if missing_columns:
        raise ValueError(f"missing audit columns: {sorted(missing_columns)}")
    labelled = 0
    usable = 0
    for _, row in frame.iterrows():
        labels = [_as_label(row[column]) for column in AUDIT_LABELS]
        if any(label is None for label in labels):
            continue
        labelled += 1
        usable += int(all(labels))
    if labelled == 0:
        return AuditSummary(
            sampled=len(frame),
            labelled=0,
            usable=0,
            usable_rate=None,
            ci95_low=None,
            ci95_high=None,
        )
    low, high = _wilson_interval(usable, labelled)
    return AuditSummary(
        sampled=len(frame),
        labelled=labelled,
        usable=usable,
        usable_rate=round(usable / labelled, 4),
        ci95_low=round(low, 4),
        ci95_high=round(high, 4),
    )
