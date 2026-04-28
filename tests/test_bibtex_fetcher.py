"""Unit tests for the BibTeX fetcher and parsing helpers."""

import pytest

from src.bibtex_fetcher import cite_key, is_valid_bibtex


class TestIsValidBibtex:
    def test_well_formed_entry_passes(self):
        assert is_valid_bibtex(
            "@article{DBLP:journals/cmc/RanaGNRS26,\n"
            "  author = {Smith},\n  title = {Foo},\n  year = {2026}\n}"
        )

    def test_proceedings_entry_with_editor(self):
        assert is_valid_bibtex(
            "@proceedings{DBLP:conf/sp/2024,\n"
            "  editor = {Jane Doe},\n  title = {Proc.},\n  year = {2024}\n}"
        )

    def test_empty_string_fails(self):
        assert not is_valid_bibtex("")

    def test_none_fails(self):
        assert not is_valid_bibtex(None)

    def test_random_text_fails(self):
        assert not is_valid_bibtex("not a bibtex entry at all")

    def test_missing_year_fails(self):
        assert not is_valid_bibtex("@article{x, author = {S}, title = {T}}")

    def test_missing_author_fails(self):
        assert not is_valid_bibtex("@article{x, title = {T}, year = {2024}}")


class TestCiteKey:
    def test_extracts_dblp_key(self):
        bib = "@article{DBLP:journals/sp/Smith24,\n  title = {x}\n}"
        assert cite_key(bib) == "DBLP:journals/sp/Smith24"

    def test_extracts_plain_key(self):
        bib = "@inproceedings{smith2024,\n  title = {x}\n}"
        assert cite_key(bib) == "smith2024"

    def test_returns_none_for_invalid(self):
        assert cite_key("not bibtex") is None

    def test_returns_none_for_empty(self):
        assert cite_key("") is None
        assert cite_key(None) is None
