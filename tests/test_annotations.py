"""Tests for the portable annotation sidecar."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.annotations import (
    AnnotationBundle,
    PaperAnnotation,
    dump_annotation_bundle,
    load_annotation_bundle,
    merge_bundle,
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


def test_an_import_keeps_notes_on_papers_it_does_not_cover() -> None:
    """Importing a backup must not erase what was written after it was exported."""
    current = {
        "paper-a": PaperAnnotation(paper_id="paper-a", notes="written today"),
        "paper-b": PaperAnnotation(paper_id="paper-b", notes="old"),
    }
    bundle = AnnotationBundle(
        annotations=[PaperAnnotation(paper_id="paper-b", status="read", notes="from backup")]
    )

    merged = merge_bundle(current, bundle)

    assert merged["paper-a"].notes == "written today"
    assert merged["paper-b"].notes == "from backup"
    assert merged["paper-b"].status == "read"
