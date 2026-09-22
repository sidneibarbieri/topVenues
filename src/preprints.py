"""arXiv candidate retrieval with explicit cross-source identity limits."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from urllib.parse import urlencode

from pydantic import BaseModel

ATOM = "{http://www.w3.org/2005/Atom}"
ARXIV = "{http://arxiv.org/schemas/atom}"


class PreprintCandidate(BaseModel):
    arxiv_id: str
    title: str
    authors: list[str]
    abstract: str
    published: str
    updated: str
    url: str
    doi: str | None = None
    queried_author: str
    identity_status: str = "name-match candidate"


def arxiv_api_url(author: str, *, max_results: int = 20) -> str:
    return "https://export.arxiv.org/api/query?" + urlencode(
        {
            "search_query": f'au:"{author}"',
            "start": 0,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
    )


def parse_arxiv_atom(payload: str, queried_author: str) -> list[PreprintCandidate]:
    """Parse an arXiv Atom response without upgrading name matches to identities."""
    root = ET.fromstring(payload)
    candidates: list[PreprintCandidate] = []
    for entry in root.findall(f"{ATOM}entry"):
        identifier = (entry.findtext(f"{ATOM}id") or "").strip()
        candidates.append(
            PreprintCandidate(
                arxiv_id=identifier.rsplit("/", 1)[-1],
                title=" ".join((entry.findtext(f"{ATOM}title") or "").split()),
                authors=[
                    (author.findtext(f"{ATOM}name") or "").strip()
                    for author in entry.findall(f"{ATOM}author")
                ],
                abstract=" ".join((entry.findtext(f"{ATOM}summary") or "").split()),
                published=(entry.findtext(f"{ATOM}published") or "").strip(),
                updated=(entry.findtext(f"{ATOM}updated") or "").strip(),
                url=identifier,
                doi=(entry.findtext(f"{ARXIV}doi") or "").strip() or None,
                queried_author=queried_author,
            )
        )
    return candidates
