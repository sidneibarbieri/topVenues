"""
Live extractor verification script.

Runs one paper through each extractor type using real network requests and xidel.
Compare results against Semantic Scholar to confirm correctness.

Requirements: xidel installed and reachable in PATH, network access.
Usage:
    python scripts/verify_extractors.py
"""

import asyncio
import csv
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx

from src.collector import _extract_doi
from src.extractors import get_extractor_for_event
from src.models import Configuration

# One real paper per extractor type, taken from master_dataset.csv
TEST_PAPERS = [
    {
        "event": "USENIX Security",
        "title": "Birthday, Name and Bifacial-security",
        "ee": "https://www.usenix.org/conference/usenixsecurity19/presentation/wang-ding",
        "paper_id": "usenix_test",
        "doi": None,
    },
    {
        "event": "NDSS",
        "title": "JavaScript Template Attacks",
        "ee": "https://www.ndss-symposium.org/ndss-paper/javascript-template-attacks-automatically-inferring-host-information-for-targeted-exploits/",
        "paper_id": "ndss_test",
        "doi": None,
    },
    {
        "event": "IEEE S&P",
        "title": "Using Safety Properties to Generate Vulnerability Patches",
        "ee": "https://doi.org/10.1109/SP.2019.00071",
        "paper_id": "ieee_sp_test",
        "doi": "10.1109/SP.2019.00071",
    },
    {
        "event": "IEEE EURO S&P",
        "title": "PILOT: Practical Privacy-Preserving Indoor Localization",
        "ee": "https://doi.org/10.1109/EuroSP.2019.00040",
        "paper_id": "ieee_eurosp_test",
        "doi": "10.1109/EuroSP.2019.00040",
    },
    {
        "event": "ACM CCS",
        "title": "Membership Privacy for Fully Dynamic Group Signatures",
        "ee": "https://doi.org/10.1145/3319535.3354257",
        "paper_id": "acm_ccs_test",
        "doi": "10.1145/3319535.3354257",
    },
    {
        "event": "ACM ASIA CCS",
        "title": "The Lazarus Effect",
        "ee": "https://doi.org/10.1145/3320269.3384723",
        "paper_id": "acm_asiaccs_test",
        "doi": "10.1145/3320269.3384723",
    },
]


def _fetch_semanticscholar(doi: str) -> str | None:
    """Fetch abstract from Semantic Scholar for ground-truth comparison."""
    if not doi:
        return None
    url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=abstract"
    try:
        r = httpx.get(url, timeout=15)
        if r.status_code == 200:
            return r.json().get("abstract")
    except httpx.HTTPError:
        pass
    return None


class _FakeCollector:
    """Minimal Collector-compatible object for extractor.extract() calls."""

    def __init__(self):
        self.config = Configuration()
        self._acm_failures: dict[str, int] = {}
        self._blocked = False

    def get_random_user_agent(self) -> str:
        import random
        return random.choice(self.config.user_agents)

    def is_acm_blocked(self) -> bool:
        return self._blocked

    def get_acm_failure_count(self, url: str) -> int:
        return self._acm_failures.get(url, 0)

    def reset_acm_failure_count(self, url: str) -> None:
        self._acm_failures.pop(url, None)

    def increment_acm_failure_count(self, url: str) -> None:
        self._acm_failures[url] = self._acm_failures.get(url, 0) + 1


async def _verify_paper(paper: dict) -> dict:
    event = paper["event"]
    extractor = get_extractor_for_event(event)
    collector = _FakeCollector()

    print(f"\n{'─'*60}")
    print(f"Event  : {event}")
    print(f"Title  : {paper['title'][:70]}")
    print(f"EE     : {paper['ee']}")
    print(f"Extractor: {type(extractor).__name__}")

    abstract = await extractor.extract(paper["ee"], paper["paper_id"], collector)

    if abstract:
        print(f"✓ Abstract extracted ({len(abstract)} chars)")
        print(f"  Preview: {abstract[:120]}…")
    else:
        print("✗ Extractor returned None — trying fallback APIs…")
        doi = paper.get("doi") or _extract_doi(paper["ee"])
        if doi:
            # AbstractFetcher needs a full Collector; call Semantic Scholar directly here
            ground_truth = _fetch_semanticscholar(doi)
            if ground_truth:
                print(f"  Semantic Scholar has abstract ({len(ground_truth)} chars)")
                print(f"  Preview: {ground_truth[:120]}…")
            else:
                print("  Semantic Scholar also returned None")
        else:
            print("  No DOI available — cannot use fallback APIs")

    # Ground-truth comparison for DOI papers
    comparison = None
    if paper.get("doi"):
        truth = _fetch_semanticscholar(paper["doi"])
        if truth and abstract:
            # Both present: check overlap
            truth_words = set(truth.lower().split())
            extracted_words = set(abstract.lower().split())
            overlap = len(truth_words & extracted_words) / max(len(truth_words), 1)
            comparison = f"Word overlap with Semantic Scholar: {overlap:.0%}"
            print(f"  {comparison}")

    return {
        "event": event,
        "extractor": type(extractor).__name__,
        "extracted": abstract is not None,
        "length": len(abstract) if abstract else 0,
        "comparison": comparison or "N/A",
    }


async def main() -> None:
    print("Big4 Extractor Verification")
    print("=" * 60)

    results = []
    for paper in TEST_PAPERS:
        result = await _verify_paper(paper)
        results.append(result)
        await asyncio.sleep(2)  # be polite to servers

    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    ok = sum(1 for r in results if r["extracted"])
    print(f"Extracted: {ok}/{len(results)}")
    for r in results:
        status = "✓" if r["extracted"] else "✗"
        print(f"  {status} {r['event']:<35} {r['extractor']:<20} {r['length']:>5} chars  {r['comparison']}")

    # Write results CSV
    out = Path("data/log/extractor_verification.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["event", "extractor", "extracted", "length", "comparison"])
        w.writeheader()
        w.writerows(results)
    print(f"\nResults saved to {out}")


if __name__ == "__main__":
    asyncio.run(main())
