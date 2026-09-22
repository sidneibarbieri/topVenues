"""Abstract sources for open venues that do not expose a DOI.

The DOI-based fetcher covers roughly the DOI-bearing half of the corpus. The
remaining AI-venue records (ICLR on OpenReview, ICML on PMLR, NeurIPS on its
own proceedings site) publish their abstracts on open landing pages. Each
function below maps one ``ee`` URL to an abstract, or ``None`` when the source
does not apply or has no abstract.
"""

from __future__ import annotations

import re

import httpx

from .abstract_quality import looks_like_abstract, normalize_abstract_text


def source_for(ee: str | None):
    """Return the fetch coroutine factory matching an ``ee`` URL, or None."""
    if not ee:
        return None
    if "openreview.net" in ee:
        return fetch_openreview
    if "proceedings.mlr.press" in ee:
        return fetch_pmlr
    if "proceedings.neurips.cc" in ee or "papers.nips.cc" in ee:
        return fetch_neurips
    return None


async def fetch_openreview(client: httpx.AsyncClient, ee: str) -> str | None:
    match = re.search(r"[?&]id=([^&]+)", ee)
    if not match:
        return None
    forum_id = match.group(1)
    # OpenReview API v2 first, then the legacy v1 endpoint.
    for url in (
        f"https://api2.openreview.net/notes?forum={forum_id}",
        f"https://api.openreview.net/notes?forum={forum_id}",
    ):
        try:
            response = await client.get(url)
        except httpx.HTTPError:
            continue
        if response.status_code != 200:
            continue
        for note in response.json().get("notes", []):
            content = note.get("content", {})
            abstract = content.get("abstract")
            if isinstance(abstract, dict):
                abstract = abstract.get("value")
            if abstract and looks_like_abstract(abstract):
                return normalize_abstract_text(abstract)
    return None


async def fetch_pmlr(client: httpx.AsyncClient, ee: str) -> str | None:
    try:
        response = await client.get(ee)
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    match = re.search(r'<div[^>]*class="abstract"[^>]*>(.*?)</div>', response.text, re.DOTALL)
    if not match:
        return None
    abstract = normalize_abstract_text(match.group(1))
    return abstract if looks_like_abstract(abstract) else None


async def fetch_neurips(client: httpx.AsyncClient, ee: str) -> str | None:
    try:
        response = await client.get(ee)
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    match = re.search(r'<p class="paper-abstract">(.*?)</p>\s*</div>', response.text, re.DOTALL)
    if not match:
        return None
    abstract = normalize_abstract_text(match.group(1))
    return abstract if looks_like_abstract(abstract) else None


async def fetch_openalex_by_title(
    client: httpx.AsyncClient, title: str, year: int | None = None
) -> str | None:
    """Universal fallback: match a paper by title in OpenAlex.

    Used for venues without a DOI or a scrapable landing page (e.g. ICLR on
    OpenReview, which blocks automated access). Requires the matched work's
    title to equal the query title after normalization, so a fuzzy hit does
    not attach the wrong abstract.
    """
    if not title:
        return None
    params = {"filter": f"title.search:{title[:120]}", "per_page": 3}
    if year:
        params["filter"] += f",publication_year:{year}"
    try:
        response = await client.get("https://api.openalex.org/works", params=params)
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None

    wanted = _normalize_title(title)
    for work in response.json().get("results", []):
        if _normalize_title(work.get("title") or "") != wanted:
            continue
        inverted = work.get("abstract_inverted_index")
        if not inverted:
            continue
        positions: dict[int, str] = {}
        for word, indexes in inverted.items():
            for index in indexes:
                positions[index] = word
        abstract = " ".join(positions[index] for index in sorted(positions))
        if looks_like_abstract(abstract):
            return normalize_abstract_text(abstract)
    return None


def _normalize_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]", "", title.lower())
