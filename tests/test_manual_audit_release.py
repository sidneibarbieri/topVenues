"""Release-level checks for the completed manual abstract audit."""

import json
from pathlib import Path

import pandas as pd

from src.manual_audit import summarize_audit
from tests.repository_only import skip_unless_repository

skip_unless_repository()

ROOT = Path(__file__).resolve().parent.parent


def test_manual_audit_primary_evidence_matches_published_summary() -> None:
    evaluation = ROOT / "evaluation" / "security-20-v3"
    frame = pd.read_csv(evaluation / "manual_abstract_audit.csv", keep_default_na=False)
    published = json.loads(
        (evaluation / "manual_abstract_audit_summary.json").read_text(encoding="utf-8")
    )
    computed = summarize_audit(frame)

    assert len(frame) == 200
    assert set(frame["reviewer"]) == {"Sidnei Barbieri"}
    assert set(frame["decision_mode"]) == {"human_only"}
    assert computed.labelled == published["labelled"] == 200
    assert computed.usable == published["usable"] == 169
    assert computed.usable_rate == published["usable_rate"] == 0.845


def test_v4_audit_transfer_is_explicit_and_lossless() -> None:
    transfer = json.loads(
        (ROOT / "evaluation" / "security-20-v4" / "audit_transfer.json").read_text(encoding="utf-8")
    )

    assert transfer["transfer_valid"] is True
    assert transfer["same_paper_id_set"] is True
    assert transfer["source_records"] == transfer["target_records"] == 14_859
    assert transfer["changed_abstracts"] == 0
    assert transfer["changed_titles_in_audit_sample"] == 0
