"""Tests for the offline BibTeX generator."""

import pytest

from src.bibtex_local import paper_to_bibtex
from src.models import Paper


class TestPaperToBibtex:
    def test_returns_none_without_dblp_key(self):
        paper = Paper(paper_id="1", title="Foo", year=2024)
        assert paper_to_bibtex(paper) is None

    def test_renders_inproceedings_for_conference(self):
        paper = Paper(
            paper_id="1", key="conf/sp/Smith24",
            title="On Foo.", year=2024,
            authors="Alice Smith, Bob Jones",
            event="IEEE S&P", pages="100-115",
            ee="https://doi.org/10.1109/SP.2024.001",
        )
        out = paper_to_bibtex(paper)
        assert out.startswith("@inproceedings{DBLP:conf/sp/Smith24,")
        assert "  booktitle    = {IEEE S&P}," in out
        assert "  pages        = {100--115}," in out
        assert "title        = {On Foo}" in out  # trailing dot stripped

    def test_renders_article_for_journal_venue(self):
        paper = Paper(
            paper_id="1", key="journals/csur/Smith24",
            title="A Survey of Foo", year=2024,
            authors="Alice Smith",
            event="ACM Computing Surveys",
        )
        out = paper_to_bibtex(paper)
        assert out.startswith("@article{DBLP:journals/csur/Smith24,")
        assert "journal      = {ACM Computing Surveys}," in out

    def test_authors_split_by_comma(self):
        paper = Paper(
            paper_id="1", key="x/y",
            title="T", year=2024,
            authors="Alice, Bob, Carol",
        )
        out = paper_to_bibtex(paper)
        assert "Alice and\n                  Bob and\n                  Carol" in out

    def test_doi_field_populated_from_ee(self):
        paper = Paper(
            paper_id="1", key="x/y", title="T", year=2024,
            ee="https://doi.org/10.1145/3000000",
        )
        out = paper_to_bibtex(paper)
        assert "doi          = {10.1145/3000000}" in out
