"""Deterministic bibliographic identity rules for corpus construction.

DBLP keys identify records in DBLP.  A corpus can nevertheless receive the
same publication through a legacy import and a DBLP listing with different
keys.  This module treats an exact canonical resource locator (DOI or landing
page) as one publication while retaining distinct papers that merely have a
similar title.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from src.abstract_quality import select_best_abstract
from src.models import Paper


@dataclass(frozen=True)
class DeduplicationReport:
    """Observable outcome of one deterministic identity pass."""

    input_records: int
    output_records: int
    merged_records: int
    groups_merged: int


def canonical_resource_locator(value: str | None) -> str | None:
    """Normalize a DOI or stable landing page without inferring identity.

    Empty values return ``None``.  Query strings and fragments are discarded
    because they do not identify a different bibliographic work in the source
    data.  Title similarity is deliberately not used as an identity signal.
    """
    if not value or not value.strip():
        return None
    candidate = value.strip()
    if candidate.lower().startswith("10."):
        return f"doi:{candidate.lower()}"
    parsed = urlsplit(candidate)
    if not parsed.scheme or not parsed.netloc:
        return candidate.casefold()
    host = parsed.netloc.casefold()
    path = parsed.path.rstrip("/") or "/"
    if host in {"doi.org", "dx.doi.org", "www.doi.org"}:
        return f"doi:{path.lstrip('/').casefold()}"
    return urlunsplit((parsed.scheme.casefold(), host, path, "", ""))


def paper_identity(paper: Paper) -> str:
    """Return the evidence-backed identity used to merge duplicate imports."""
    locator = canonical_resource_locator(paper.ee)
    return locator if locator is not None else f"dblp:{paper.paper_id}"


def _is_dblp_key(paper_id: str) -> bool:
    return paper_id.startswith(("conf/", "journals/", "books/", "series/"))


def _candidate_key(paper: Paper) -> tuple[int, int, int, int, int, str]:
    """Prefer DBLP keys and complete fields, with a stable final tie-break."""
    return (
        int(_is_dblp_key(paper.paper_id)),
        int(bool(paper.bibtex)),
        len(paper.bibtex or ""),
        int(bool(paper.abstract)),
        len(paper.abstract or ""),
        # Reverse lexical ordering below makes the final choice deterministic.
        paper.paper_id,
    )


def _best_text(candidates: list[Paper], attribute: str) -> str | None:
    values = [getattr(paper, attribute) for paper in candidates if getattr(paper, attribute)]
    return max(values, key=lambda value: (len(value), value)) if values else None


def _merge_group(candidates: list[Paper]) -> Paper:
    primary = max(candidates, key=_candidate_key)
    return primary.model_copy(
        update={
            "abstract": select_best_abstract(paper.abstract for paper in candidates),
            "bibtex": _best_text(candidates, "bibtex"),
        }
    )


def deduplicate_papers(papers: list[Paper]) -> tuple[list[Paper], DeduplicationReport]:
    """Merge only records that share an exact canonical resource locator."""
    groups: dict[str, list[Paper]] = defaultdict(list)
    for paper in papers:
        groups[paper_identity(paper)].append(paper)
    unique = [_merge_group(groups[identity]) for identity in sorted(groups)]
    merged_groups = sum(len(group) > 1 for group in groups.values())
    report = DeduplicationReport(
        input_records=len(papers),
        output_records=len(unique),
        merged_records=len(papers) - len(unique),
        groups_merged=merged_groups,
    )
    return unique, report
