#!/usr/bin/env python3
"""Verify high-confidence audit rows against DOI metadata deposited in Crossref."""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Literal

import httpx
import pandas as pd
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.abstract_quality import normalize_abstract_text  # noqa: E402
from src.deduplication import canonical_resource_locator  # noqa: E402
from src.manual_audit import (  # noqa: E402
    AUDIT_LABELS,
    append_audit_decision,
    save_audit_progress,
)

POLICY_VERSION = "crossref-exact-match-v1"
REVIEWER = "Sidnei Barbieri (human supervisor); OpenAI Codex (mechanical assistant)"


class VerificationOutcome(BaseModel):
    sample_id: int
    paper_id: str
    status: Literal[
        "verified_exact",
        "verified_canonical",
        "missing_doi",
        "crossref_not_found",
        "crossref_missing_abstract",
        "title_mismatch",
        "text_difference",
    ]
    detail: str


def _doi(source_url: str) -> str | None:
    locator = canonical_resource_locator(source_url)
    return locator.removeprefix("doi:") if locator and locator.startswith("doi:") else None


def _title_key(title: str) -> str:
    folded = unicodedata.normalize("NFKD", title)
    ascii_text = "".join(character for character in folded if not unicodedata.combining(character))
    return re.sub(r"[^a-z0-9]", "", ascii_text.casefold())


def _canonical_text(text: str) -> str:
    normalized = normalize_abstract_text(text) or ""
    return re.sub(r"\s+([.,;:!?])", r"\1", re.sub(r"\s+", " ", normalized)).strip()


def _first_difference(left: str, right: str) -> int:
    for index, characters in enumerate(zip(left, right, strict=False)):
        if characters[0] != characters[1]:
            return index
    return min(len(left), len(right))


def _crossref_record(client: httpx.Client, doi: str) -> dict | None:
    response = client.get(f"https://api.crossref.org/works/{doi}")
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["message"]


def _verify_row(client: httpx.Client, row: pd.Series) -> tuple[VerificationOutcome, str | None]:
    sample_id = int(row["sample_id"])
    paper_id = str(row["paper_id"])
    doi = _doi(str(row["source_url"]))
    if not doi:
        return VerificationOutcome(
            sample_id=sample_id,
            paper_id=paper_id,
            status="missing_doi",
            detail="source URL does not expose a DOI",
        ), None
    record = _crossref_record(client, doi)
    if record is None:
        return VerificationOutcome(
            sample_id=sample_id,
            paper_id=paper_id,
            status="crossref_not_found",
            detail=f"DOI {doi} was not found",
        ), None
    abstract = normalize_abstract_text(record.get("abstract"))
    if not abstract:
        return VerificationOutcome(
            sample_id=sample_id,
            paper_id=paper_id,
            status="crossref_missing_abstract",
            detail=f"DOI {doi} has no deposited abstract",
        ), None
    deposited_titles = record.get("title") or []
    if not deposited_titles or _title_key(deposited_titles[0]) != _title_key(str(row["title"])):
        return VerificationOutcome(
            sample_id=sample_id,
            paper_id=paper_id,
            status="title_mismatch",
            detail=f"corpus={row['title']!s}; crossref={deposited_titles[:1]!r}",
        ), None
    corpus_abstract = normalize_abstract_text(str(row["abstract"])) or ""
    exact = corpus_abstract == abstract
    canonical = _canonical_text(corpus_abstract) == _canonical_text(abstract)
    if not canonical:
        difference = _first_difference(_canonical_text(corpus_abstract), _canonical_text(abstract))
        return VerificationOutcome(
            sample_id=sample_id,
            paper_id=paper_id,
            status="text_difference",
            detail=(
                f"DOI {doi}; first canonical difference at {difference}; "
                f"corpus chars={len(corpus_abstract)}; crossref chars={len(abstract)}"
            ),
        ), None
    status = "verified_exact" if exact else "verified_canonical"
    detail = (
        f"DOI {doi}; publisher-deposited Crossref abstract "
        f"{'exactly matches' if exact else 'matches after punctuation-whitespace normalization'}; "
        f"corpus chars={len(corpus_abstract)}; crossref chars={len(abstract)}; "
        f"policy={POLICY_VERSION}"
    )
    return VerificationOutcome(
        sample_id=sample_id,
        paper_id=paper_id,
        status=status,
        detail=detail,
    ), detail


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("progress", type=Path)
    parser.add_argument("decisions", type=Path)
    parser.add_argument("--profile", default="security-20-v3")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--start-after-sample-id", type=int, default=0)
    args = parser.parse_args()

    frame = pd.read_csv(args.progress, keep_default_na=False)
    outcomes: list[VerificationOutcome] = []
    inspected = 0
    headers = {"User-Agent": "TopVenues/1.4 (mailto:sidneisb@ita.br)"}
    with httpx.Client(timeout=30.0, headers=headers, follow_redirects=True) as client:
        for index, row in frame.iterrows():
            if int(row["sample_id"]) <= args.start_after_sample_id:
                continue
            if all(str(row[label]).strip() for label in AUDIT_LABELS):
                continue
            if inspected >= args.limit:
                break
            outcome, note = _verify_row(client, row)
            outcomes.append(outcome)
            inspected += 1
            if note is None:
                continue
            for label in AUDIT_LABELS:
                frame.loc[index, label] = "yes"
            frame.loc[index, "reviewer"] = REVIEWER
            frame.loc[index, "decision_mode"] = "human_supervised_codex_assisted"
            frame.loc[index, "notes"] = note
            save_audit_progress(frame, args.progress)
            append_audit_decision(
                frame.loc[index],
                profile_id=args.profile,
                sample_size=len(frame),
                progress_path=args.progress,
                decision_log_path=args.decisions,
                source_mode="crossref_deposited_metadata",
            )

    print(json.dumps([outcome.model_dump() for outcome in outcomes], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
