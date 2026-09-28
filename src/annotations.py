"""Portable, user-owned annotations kept separate from the corpus."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ReadingStatus = Literal["unread", "reading", "read", "excluded"]


class PaperAnnotation(BaseModel):
    """A note attached to a corpus identifier without changing the corpus."""

    model_config = ConfigDict(extra="forbid")

    paper_id: str = Field(min_length=1)
    status: ReadingStatus = "unread"
    tags: list[str] = Field(default_factory=list)
    notes: str = ""


class AnnotationBundle(BaseModel):
    """Versioned exchange format for annotations."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    annotations: list[PaperAnnotation] = Field(default_factory=list)

    def by_paper_id(self) -> dict[str, PaperAnnotation]:
        return {annotation.paper_id: annotation for annotation in self.annotations}


def normalize_tags(raw_tags: str) -> list[str]:
    """Return unique, case-insensitive tags while preserving display spelling."""
    tags: list[str] = []
    seen: set[str] = set()
    for candidate in raw_tags.split(","):
        tag = candidate.strip()
        normalized = tag.casefold()
        if tag and normalized not in seen:
            tags.append(tag)
            seen.add(normalized)
    return tags


def load_annotation_bundle(payload: str | bytes) -> AnnotationBundle:
    """Validate a JSON annotation bundle and expose the real validation error."""
    return AnnotationBundle.model_validate_json(payload)


def merge_bundle(
    current: dict[str, PaperAnnotation], bundle: AnnotationBundle
) -> dict[str, PaperAnnotation]:
    """Apply an imported bundle on top of the current notes.

    Imported notes win for the papers they cover; notes on every other paper are
    kept. Replacing the whole set would silently drop what was written since the
    file was exported.
    """
    return {**current, **bundle.by_paper_id()}


def dump_annotation_bundle(annotations: dict[str, PaperAnnotation]) -> str:
    """Serialize annotations in stable paper-id order."""
    bundle = AnnotationBundle(
        annotations=[annotations[paper_id] for paper_id in sorted(annotations)]
    )
    return bundle.model_dump_json(indent=2)
