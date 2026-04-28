"""Generate BibTeX entries offline from fields already stored on each Paper.

This module is the zero-network fallback to :mod:`bibtex_dump` and the
DBLP per-record API. Output is a syntactically valid BibTeX entry that
covers the essential bibliographic fields (author, title, venue, year,
pages, DOI, URL). It does *not* fill in optional fields like ``volume``
and ``number`` because those are not part of the JSON metadata DBLP
emits to its search API.

For canonical, fully-populated entries (with crossref-resolved
``editor`` / ``booktitle`` / ``publisher``) prefer
``parse_dump_for_keys`` from :mod:`bibtex_dump`.
"""

from __future__ import annotations

from .bibtex_dump import format_bibtex
from .models import Paper, PaperType

# Maps the DBLP article type to the matching BibTeX entry tag.
_ENTRY_TAGS = {
    PaperType.ARTICLE: "inproceedings",
    PaperType.PROCEEDINGS: "proceedings",
}

# Venues that are journals → ``@article``; everything else is a conference paper.
_JOURNAL_VENUES = {
    "ACM Computing Surveys",
    "IEEE Communications Surveys & Tutorials",
    "Foundations and Trends in Privacy and Security",
}


def paper_to_bibtex(paper: Paper) -> str | None:
    """Render a Paper as a BibTeX entry. Returns ``None`` if no DBLP key."""
    if not paper.key:
        return None

    tag = _entry_tag(paper)
    fields: dict[str, object] = {}

    if paper.authors:
        fields["author"] = [name.strip() for name in paper.authors.split(",") if name.strip()]
    if paper.title:
        fields["title"] = paper.title.rstrip(".")
    if paper.event:
        fields["journal" if tag == "article" else "booktitle"] = paper.event
    if paper.pages:
        fields["pages"] = paper.pages
    fields["year"] = str(paper.year)
    if paper.doi:
        fields["doi"] = paper.doi
    if paper.ee:
        fields["url"] = paper.ee

    return format_bibtex(tag, paper.key, fields)


def _entry_tag(paper: Paper) -> str:
    if paper.paper_type == PaperType.PROCEEDINGS:
        return "proceedings"
    if paper.event in _JOURNAL_VENUES:
        return "article"
    return "inproceedings"
