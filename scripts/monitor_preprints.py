#!/usr/bin/env python3
"""Fetch arXiv name-match candidates for a portable TopVenues watchlist."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.preprints import arxiv_api_url, parse_arxiv_atom  # noqa: E402
from src.research_intelligence import ResearchWatchlist  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("watchlist", type=Path)
    parser.add_argument("--output", type=Path, default=Path("preprint-candidates.json"))
    parser.add_argument("--max-results", type=int, default=10)
    args = parser.parse_args()
    watchlist = ResearchWatchlist.model_validate_json(args.watchlist.read_text())
    candidates = []
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        for author in watchlist.authors:
            response = client.get(arxiv_api_url(author, max_results=args.max_results))
            response.raise_for_status()
            candidates.extend(parse_arxiv_atom(response.text, author))
    payload = {
        "schema_version": "1.0",
        "fetched_at": datetime.now(UTC).isoformat(),
        "watchlist": watchlist.name,
        "identity_policy": (
            "Candidates are arXiv name matches. Confirm author identity before use."
        ),
        "candidates": [candidate.model_dump() for candidate in candidates],
    }
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(candidates)} name-match candidates to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
