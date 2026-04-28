"""Tests for the DBLP-dump-based BibTeX builder."""

import gzip
import io
import xml.etree.ElementTree as ET

import pytest

from src.bibtex_dump import (
    _EntitySubstitutingStream,
    _extract_fields,
    _resolve_crossrefs_and_format,
    format_bibtex,
    parse_dump_for_keys,
)


def _xml_element(xml_text: str) -> ET.Element:
    return ET.parse(io.StringIO(xml_text)).getroot()


class TestExtractFields:
    def test_pulls_authors_in_order(self):
        elem = _xml_element(
            "<article key='x'>"
            "<author>Alice</author>"
            "<author>Bob</author>"
            "<title>T</title>"
            "<year>2024</year>"
            "</article>"
        )
        fields = _extract_fields(elem)
        assert fields["author"] == ["Alice", "Bob"]
        assert fields["title"] == "T"

    def test_doi_extracted_from_ee(self):
        elem = _xml_element(
            "<article key='x'>"
            "<ee>https://doi.org/10.1145/3000000</ee>"
            "<title>T</title><year>2024</year>"
            "</article>"
        )
        fields = _extract_fields(elem)
        assert fields["doi"] == "10.1145/3000000"
        assert fields["url"] == "https://doi.org/10.1145/3000000"

    def test_first_url_used_when_no_doi(self):
        elem = _xml_element(
            "<article key='x'>"
            "<ee>https://www.usenix.org/conference/foo</ee>"
            "<title>T</title><year>2024</year>"
            "</article>"
        )
        fields = _extract_fields(elem)
        assert "doi" not in fields
        assert fields["url"] == "https://www.usenix.org/conference/foo"

    def test_crossref_captured(self):
        elem = _xml_element(
            "<inproceedings key='conf/sp/Smith24'>"
            "<author>Smith</author>"
            "<title>Foo</title>"
            "<crossref>conf/sp/2024</crossref>"
            "<year>2024</year>"
            "</inproceedings>"
        )
        fields = _extract_fields(elem)
        assert fields["__crossref"] == "conf/sp/2024"


class TestFormatBibtex:
    def test_renders_dblp_style_article(self):
        out = format_bibtex("article", "journals/x/Smith24", {
            "author": ["Alice Smith", "Bob Jones"],
            "title": "On Foo",
            "journal": "Journal of Foo",
            "volume": "42",
            "pages": "1-15",
            "year": "2024",
            "doi": "10.1145/3000000",
            "url": "https://doi.org/10.1145/3000000",
        })
        assert out.startswith("@article{DBLP:journals/x/Smith24,")
        assert "  author       = {Alice Smith and\n                  Bob Jones}," in out
        assert "  pages        = {1--15}," in out
        assert "biburl       = {https://dblp.org/rec/journals/x/Smith24.bib}" in out
        assert out.endswith("}")

    def test_pages_already_double_dashed(self):
        out = format_bibtex("article", "x", {
            "author": ["A"], "title": "T", "year": "2024", "pages": "10--20",
        })
        assert "{10--20}" in out

    def test_omits_missing_optional_fields(self):
        out = format_bibtex("inproceedings", "conf/sp/x", {
            "author": ["A"], "title": "T", "year": "2024",
        })
        assert "volume" not in out
        assert "doi" not in out


class TestCrossrefResolution:
    def test_inherits_editor_and_booktitle(self):
        matched = {
            "conf/sp/Smith24": ("inproceedings", {
                "author": ["Smith"], "title": "Foo", "year": "2024",
                "__crossref": "conf/sp/2024",
            })
        }
        proceedings = {
            "conf/sp/2024": {
                "editor": ["Jane Doe"],
                "booktitle": "S&P 2024",
                "publisher": "{IEEE}",
            }
        }
        result = _resolve_crossrefs_and_format(matched, proceedings)
        out = result["conf/sp/Smith24"]
        assert "editor       = {Jane Doe}" in out
        assert "booktitle    = {S&P 2024}" in out
        assert "publisher    = {{IEEE}}" in out

    def test_no_crossref_passes_through(self):
        matched = {
            "conf/sp/Smith24": ("inproceedings", {
                "author": ["Smith"], "title": "Foo", "year": "2024",
            })
        }
        result = _resolve_crossrefs_and_format(matched, {})
        assert "Smith" in result["conf/sp/Smith24"]


class TestEntitySubstitutingStream:
    def test_replaces_named_entity_with_numeric_ref(self):
        table = {b"uuml": b"&#252;", b"Auml": b"&#196;"}
        source = io.BytesIO(b"<author>M\xc3\xb6ller &uuml; Test</author>")
        out = _EntitySubstitutingStream(source, table).read()
        assert b"&#252;" in out
        assert b"&uuml;" not in out

    def test_leaves_xml_builtins_alone(self):
        source = io.BytesIO(b"<x>foo &amp; bar &lt;y&gt;</x>")
        out = _EntitySubstitutingStream(source, {}).read()
        assert b"&amp;" in out
        assert b"&lt;" in out
        assert b"&gt;" in out

    def test_handles_entity_at_chunk_boundary(self):
        table = {b"uuml": b"&#252;"}
        # Force a small read that splits "&uuml;" across two read() calls.
        source = io.BytesIO(b"a" * 60 + b"&uuml;rest of doc")
        stream = _EntitySubstitutingStream(source, table)
        first = stream.read(50)
        rest = b""
        while True:
            chunk = stream.read(64)
            if not chunk:
                break
            rest += chunk
        full = first + rest
        assert b"&#252;" in full
        assert b"&uuml;" not in full

    def test_end_to_end_match_returns_full_fields(self, tmp_path):
        """Regression: child end events used to clear text before parent fired."""
        dtd = tmp_path / "dblp.dtd"
        dtd.write_text("")  # empty DTD is fine — no named entities used
        xml = tmp_path / "dblp.xml.gz"
        body = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<dblp>"
            b"<article key='journals/x/Smith24'>"
            b"<author>Alice Smith</author>"
            b"<author>Bob Jones</author>"
            b"<title>On Foo</title>"
            b"<journal>Journal of Foo</journal>"
            b"<volume>42</volume>"
            b"<pages>1-15</pages>"
            b"<year>2024</year>"
            b"<ee>https://doi.org/10.1145/3000000</ee>"
            b"</article>"
            b"</dblp>"
        )
        with gzip.open(xml, "wb") as fh:
            fh.write(body)

        result = parse_dump_for_keys(xml, ["journals/x/Smith24"])
        assert "journals/x/Smith24" in result
        bib = result["journals/x/Smith24"]
        assert "author       = {Alice Smith and\n                  Bob Jones}" in bib
        assert "title        = {On Foo}" in bib
        assert "journal      = {Journal of Foo}" in bib
        assert "volume       = {42}" in bib
        assert "pages        = {1--15}" in bib
        assert "doi          = {10.1145/3000000}" in bib

    def test_entity_falling_exactly_on_carry_boundary(self):
        # An ``&szlig;`` placed near the carry boundary must not be split.
        table = {b"szlig": b"&#223;"}
        # Place the entity right where buf len - CARRY would land on its
        # interior, which is the regression that previously leaked unresolved
        # entities to the parser.
        source = io.BytesIO(b"x" * 1000 + b"&szlig;" + b"y" * 1000)
        stream = _EntitySubstitutingStream(source, table)
        full = b""
        while True:
            chunk = stream.read(128)
            if not chunk:
                break
            full += chunk
        assert b"&szlig;" not in full
        assert b"&#223;" in full
