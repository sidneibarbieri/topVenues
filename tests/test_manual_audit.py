import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.manual_audit import (
    append_audit_decision,
    build_audit_sample,
    load_audit_progress,
    save_audit_progress,
    summarize_audit,
)


def _audit_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE papers (paper_id TEXT, event TEXT, year INT, title TEXT, ee TEXT, abstract TEXT)"
        )
        connection.executemany(
            "INSERT INTO papers VALUES (?, ?, ?, ?, ?, ?)",
            [(f"a{i}", "A", 2024, f"A {i}", "https://a", "abstract") for i in range(8)]
            + [(f"b{i}", "B", 2025, f"B {i}", "https://b", None) for i in range(2)],
        )


def test_sample_is_deterministic_and_proportional(tmp_path: Path) -> None:
    database = tmp_path / "papers.db"
    _audit_database(database)
    first = build_audit_sample(database, sample_size=5)
    second = build_audit_sample(database, sample_size=5)
    pd.testing.assert_frame_equal(first, second)
    assert first["venue"].value_counts().to_dict() == {"A": 4, "B": 1}


def test_summary_uses_only_fully_labelled_rows() -> None:
    frame = pd.DataFrame(
        [
            {"label_complete": "yes", "label_uncontaminated": "yes", "label_matches_paper": "yes"},
            {"label_complete": "no", "label_uncontaminated": "yes", "label_matches_paper": "yes"},
            {"label_complete": "", "label_uncontaminated": "yes", "label_matches_paper": "yes"},
        ]
    )
    summary = summarize_audit(frame)
    assert summary.sampled == 3
    assert summary.labelled == 2
    assert summary.usable == 1
    assert summary.usable_rate == 0.5


def test_audit_progress_round_trip_is_atomic(tmp_path: Path) -> None:
    database = tmp_path / "papers.db"
    progress_path = tmp_path / "progress.csv"
    _audit_database(database)
    sample = build_audit_sample(database, sample_size=5)
    sample.loc[0, "label_complete"] = "yes"
    save_audit_progress(sample, progress_path)

    loaded = load_audit_progress(sample, progress_path)

    pd.testing.assert_frame_equal(loaded, sample)
    assert not list(tmp_path.glob("*.tmp"))


def test_audit_progress_rejects_a_different_sample(tmp_path: Path) -> None:
    database = tmp_path / "papers.db"
    progress_path = tmp_path / "progress.csv"
    _audit_database(database)
    sample = build_audit_sample(database, sample_size=5)
    save_audit_progress(sample, progress_path)
    different_sample = sample.copy()
    different_sample.loc[0, "paper_id"] = "different"

    with pytest.raises(ValueError, match="paper_id"):
        load_audit_progress(different_sample, progress_path)


def test_decision_log_is_append_only(tmp_path: Path) -> None:
    database = tmp_path / "papers.db"
    progress_path = tmp_path / "progress.csv"
    decision_log_path = tmp_path / "decisions.jsonl"
    _audit_database(database)
    sample = build_audit_sample(database, sample_size=5)
    sample.loc[0, ["label_complete", "label_uncontaminated", "label_matches_paper"]] = "yes"
    sample.loc[0, "reviewer"] = "Human supervisor; Codex assistant"
    sample.loc[0, "decision_mode"] = "human_supervised_codex_assisted"

    first = append_audit_decision(
        sample.loc[0],
        profile_id="test-profile",
        sample_size=len(sample),
        progress_path=progress_path,
        decision_log_path=decision_log_path,
    )
    second = append_audit_decision(
        sample.loc[0],
        profile_id="test-profile",
        sample_size=len(sample),
        progress_path=progress_path,
        decision_log_path=decision_log_path,
    )

    rows = [json.loads(line) for line in decision_log_path.read_text().splitlines()]
    assert [row["decision_id"] for row in rows] == [first.decision_id, second.decision_id]
    assert all(row["decision_mode"] == "human_supervised_codex_assisted" for row in rows)
    assert all(row["event_type"] == "decision" for row in rows)


def test_decision_log_records_provenance_correction(tmp_path: Path) -> None:
    database = tmp_path / "papers.db"
    progress_path = tmp_path / "progress.csv"
    decision_log_path = tmp_path / "decisions.jsonl"
    _audit_database(database)
    sample = build_audit_sample(database, sample_size=5)
    sample.loc[0, ["label_complete", "label_uncontaminated", "label_matches_paper"]] = "yes"
    sample.loc[0, "reviewer"] = "Sidnei Barbieri"
    sample.loc[0, "decision_mode"] = "human_only"
    original = append_audit_decision(
        sample.loc[0],
        profile_id="test-profile",
        sample_size=len(sample),
        progress_path=progress_path,
        decision_log_path=decision_log_path,
    )

    sample.loc[0, "reviewer"] = "Human supervisor; Codex assistant"
    sample.loc[0, "decision_mode"] = "human_supervised_codex_assisted"
    correction = append_audit_decision(
        sample.loc[0],
        profile_id="test-profile",
        sample_size=len(sample),
        progress_path=progress_path,
        decision_log_path=decision_log_path,
        event_type="provenance_correction",
        supersedes_decision_id=original.decision_id,
    )

    rows = [json.loads(line) for line in decision_log_path.read_text().splitlines()]
    assert rows[-1]["event_type"] == "provenance_correction"
    assert rows[-1]["supersedes_decision_id"] == original.decision_id
    assert correction.decision_mode == "human_supervised_codex_assisted"
