#!/usr/bin/env python3
"""Collect recent arXiv cs.CR preprints and flag the ones the rule points at.

The rule is the one the main-track paper measured: a preprint whose authors
already published in the declared top-4 in the previous four years. Here it runs
forward, so a reader can read that work months before a venue publishes it.

The result is written to ``data/radar/preprint-radar.json`` and is a separate
artifact by design: it never enters the corpus, and the corpus counts never
include it.

Usage::

    python scripts/collect_preprint_radar.py --days 45
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.preprints import arxiv_window_url, parse_arxiv_atom  # noqa: E402
from src.profiles import (  # noqa: E402
    load_profile,
    select_profile_id,
    verified_profile_snapshot,
)
from src.radar import build_prior_index, flag, radar_path, snapshot  # noqa: E402
from src.tiers import TOP4  # noqa: E402

ACKNOWLEDGMENT = "Thank you to arXiv for use of its open access interoperability."
PAGE_SIZE = 100
SECONDS_BETWEEN_REQUESTS = 3.0  # arXiv asks callers to stay under one request every three seconds
RETRIES = 4
USER_AGENT = "TopVenues/1.14 (+https://github.com/sidneibarbieri/topVenues)"


def _fetch_page(url: str) -> str:
    """One API page, waiting first and backing off when arXiv asks us to slow down.

    The standard library is deliberate: arXiv answers some HTTP clients with 406
    regardless of headers, and this call has no reason to need more than a GET.
    """
    delay = SECONDS_BETWEEN_REQUESTS
    for attempt in range(RETRIES):
        time.sleep(delay)
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            if attempt == RETRIES - 1:
                raise
            print(f"    arXiv answered {error.code}; waiting {delay * 2:.0f}s")
            delay *= 2
    return ""


def fetch_recent(category: str, since: datetime, *, page_limit: int) -> list:
    """Page newest-first until the submissions predate the cutoff."""
    collected = []
    cutoff = since.isoformat()
    for page in range(page_limit):
        url = arxiv_window_url(
            category,
            since=since.date(),
            until=datetime.now(UTC).date(),
            start=page * PAGE_SIZE,
            max_results=PAGE_SIZE,
        )
        batch = parse_arxiv_atom(_fetch_page(url), queried_author=f"cat:{category}")
        if not batch:
            break
        collected.extend(batch)
        oldest = min(item.published for item in batch)
        print(f"  page {page + 1}: {len(batch)} preprints, oldest {oldest[:10]}")
        if oldest < cutoff:
            break
    return [item for item in collected if item.published >= cutoff]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=45, help="how far back to look")
    parser.add_argument("--category", default="cs.CR")
    parser.add_argument("--profile", default=select_profile_id())
    parser.add_argument("--pages", type=int, default=12, help="safety cap on API pages")
    arguments = parser.parse_args()

    since = datetime.now(UTC) - timedelta(days=arguments.days)
    print(f"arXiv {arguments.category} since {since.date()} — {ACKNOWLEDGMENT}")
    preprints = fetch_recent(arguments.category, since, page_limit=arguments.pages)
    print(f"  considered: {len(preprints)} preprints")

    profile = load_profile(arguments.profile)
    with verified_profile_snapshot(arguments.profile) as verified:
        last_year = datetime.now(UTC).year
        index = build_prior_index(
            verified.database_path,
            tracked_tier=TOP4,
            first_year=last_year - 4,
            last_year=last_year - 1,
        )
    print(f"  authors with a {TOP4} record in the window: {len(index)}")

    flagged = flag(preprints, index)
    result = snapshot(
        category=arguments.category,
        submitted_since=since.date().isoformat(),
        profile_id=arguments.profile,
        corpus_fingerprint=profile.manifest["snapshot"]["gzip_sha256"],
        considered=len(preprints),
        flagged=flagged,
    )
    target = radar_path(ROOT)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(result.model_dump_json(indent=1), encoding="utf-8")
    print(f"  flagged: {len(flagged)} ({result.flagged_share:.1%}) -> {target.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
