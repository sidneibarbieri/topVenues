"""DBLP marks up titles; reading element.text truncated them at the first tag."""

import xml.etree.ElementTree as ET

import pytest

from src.dblp_dump_materializer import _element_text


@pytest.mark.parametrize(
    ("xml", "expected"),
    [
        # Superscript inside a system name: no space is introduced.
        ("<title>D<sup>3</sup>FL: Label-Free Defense.</title>", "D3FL: Label-Free Defense."),
        (
            "<title>S<sup>2</sup>NeRF: Privacy-preserving Training.</title>",
            "S2NeRF: Privacy-preserving Training.",
        ),
        ("<title>SP<sup>2</sup>-RD2D: Secure Protocol.</title>", "SP2-RD2D: Secure Protocol."),
        # Superscript followed by a real word: the separating space survives.
        (
            "<title>The SPHINCS<sup>+</sup> Signature Framework.</title>",
            "The SPHINCS+ Signature Framework.",
        ),
        (
            "<title>Attacks Against the IND-CPA<sup>D</sup> Security of Exact FHE Schemes.</title>",
            "Attacks Against the IND-CPAD Security of Exact FHE Schemes.",
        ),
        # Emphasis carries no textual meaning.
        (
            "<title>Poster: <i>LLMalware</i>: An LLM-Powered Detector.</title>",
            "Poster: LLMalware: An LLM-Powered Detector.",
        ),
        # Unmarked elements keep their previous behaviour.
        ("<title>Plain title without markup.</title>", "Plain title without markup."),
        ("<author>Jane Doe</author>", "Jane Doe"),
    ],
)
def test_element_text_preserves_marked_up_titles(xml: str, expected: str) -> None:
    assert _element_text(ET.fromstring(xml)) == expected


def test_element_text_reads_past_the_first_child() -> None:
    """The regression itself: element.text alone would return only 'D'."""
    element = ET.fromstring("<title>D<sup>3</sup>FL: rest of the title.</title>")
    assert element.text == "D"
    assert _element_text(element) != "D"
