"""A selector that yields one paragraph must never be tried first.

USENIX abstracts are often split across several <p> elements. Reaching for a
single-paragraph selector first truncates them to their opening paragraph, and
the truncated text still looks like a valid abstract, so nothing downstream
notices.
"""

from __future__ import annotations

from src.extractors.usenix import USENIXExtractor


def _joins_whole_sequence(xpath: str) -> bool:
    return "string-join" in xpath


def test_joining_selectors_precede_single_paragraph_ones():
    xpaths = USENIXExtractor().xpaths
    joining = [i for i, xpath in enumerate(xpaths) if _joins_whole_sequence(xpath)]
    single = [i for i, xpath in enumerate(xpaths) if not _joins_whole_sequence(xpath)]
    assert joining, "at least one selector must join the paragraph sequence"
    assert max(joining) < min(single), (
        "a single-paragraph selector is tried before a joining one, "
        "which silently truncates multi-paragraph abstracts"
    )


def test_the_paper_description_block_is_tried_first():
    """It is the element USENIX actually marks as the abstract."""
    assert "field-name-field-paper-description" in USENIXExtractor().xpaths[0]
