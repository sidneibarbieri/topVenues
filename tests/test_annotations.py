"""Tests for the portable annotation sidecar."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.annotations import (
    PaperAnnotation,
    dump_annotation_bundle,
    load_annotation_bundle,
    normalize_tags,
)


def test_tags_are_trimmed_and_deduplicated_case_insensitively() -> None:
    assert normalize_tags("RAG, usable security, rag, , LLM") == [
        "RAG",
        "usable security",
        "LLM",
    ]


def test_annotation_bundle_round_trips_in_stable_order() -> None:
    annotations = {
        "paper-b": PaperAnnotation(paper_id="paper-b", notes="Second"),
        "paper-a": PaperAnnotation(
            paper_id="paper-a", status="read", tags=["survey"], notes="First"
        ),
    }

    payload = dump_annotation_bundle(annotations)
    restored = load_annotation_bundle(payload)

    assert [annotation.paper_id for annotation in restored.annotations] == [
        "paper-a",
        "paper-b",
    ]
    assert restored.by_paper_id()["paper-a"].status == "read"


def test_annotation_bundle_rejects_unknown_fields() -> None:
    payload = '{"schema_version":1,"annotations":[],"unexpected":true}'

    with pytest.raises(ValidationError):
        load_annotation_bundle(payload)
