"""Tests for Pydantic models and SearchFilters."""

import pytest
from pydantic import ValidationError

from src.config import ConfigManager
from src.models import Configuration, Paper, PaperClass, SearchFilters


class TestSearchFilters:
    def test_empty_has_no_filter(self):
        assert not SearchFilters().has_any_filter()

    def test_one_filter_set(self):
        assert SearchFilters(title_contains="LLM").has_any_filter()

    def test_multiple_filters(self):
        f = SearchFilters(event="ACM CCS", year=2023)
        assert f.has_any_filter()


class TestPaperValidation:
    def test_valid_year(self):
        p = Paper(paper_id="1", title="T", year=2023)
        assert p.year == 2023

    def test_invalid_year_raises(self):
        with pytest.raises(ValidationError):
            Paper(paper_id="1", title="T", year=1800)

    def test_alias_population(self):
        p = Paper(**{"ID": "99", "title": "T", "year": 2021})
        assert p.paper_id == "99"


class TestPaperComputedProperties:
    def test_doi_from_doi_org_url(self):
        p = Paper(paper_id="1", title="T", year=2023, ee="https://doi.org/10.1145/3548606.3559413")
        assert p.doi == "10.1145/3548606.3559413"

    def test_doi_from_bare_doi(self):
        p = Paper(paper_id="1", title="T", year=2023, ee="10.1145/3548606.3559413")
        assert p.doi == "10.1145/3548606.3559413"

    def test_doi_none_when_no_ee(self):
        assert Paper(paper_id="1", title="T", year=2023).doi is None

    def test_doi_none_for_non_doi_url(self):
        p = Paper(paper_id="1", title="T", year=2023, ee="https://www.usenix.org/conference/foo")
        assert p.doi is None

    def test_first_author(self):
        p = Paper(paper_id="1", title="T", year=2023, authors="Alice Smith, Bob Jones, Carol")
        assert p.first_author == "Alice Smith"

    def test_first_author_none_when_no_authors(self):
        assert Paper(paper_id="1", title="T", year=2023).first_author is None

    def test_abstract_words_zero_when_empty(self):
        assert Paper(paper_id="1", title="T", year=2023).abstract_words == 0

    def test_abstract_words_counts_correctly(self):
        p = Paper(paper_id="1", title="T", year=2023, abstract="This is a five-word abstract here")
        assert p.abstract_words == 6

    def test_paper_class_sok(self):
        p = Paper(paper_id="1", title="SoK: Cryptographic Confidentiality", year=2023)
        assert p.paper_class == PaperClass.SOK

    def test_paper_class_survey(self):
        p = Paper(paper_id="1", title="A Survey of Adversarial Attacks", year=2023)
        assert p.paper_class == PaperClass.SURVEY

    def test_paper_class_poster(self):
        p = Paper(paper_id="1", title="Poster: Side-Channel Mitigations", year=2023)
        assert p.paper_class == PaperClass.POSTER

    def test_paper_class_journal(self):
        p = Paper(paper_id="1", title="Distributed Systems Survey", year=2023,
                  event="ACM Computing Surveys")
        assert p.paper_class == PaperClass.SURVEY  # title wins

    def test_paper_class_journal_when_title_neutral(self):
        p = Paper(paper_id="1", title="On the Hardness of Lattice Problems", year=2023,
                  event="ACM Computing Surveys")
        assert p.paper_class == PaperClass.JOURNAL

    def test_paper_class_default_article(self):
        p = Paper(paper_id="1", title="Provably Secure Encryption", year=2023, event="ACM CCS")
        assert p.paper_class == PaperClass.ARTICLE


class TestConfiguration:
    def test_default_interval_is_list(self):
        c = Configuration()
        assert isinstance(c.default_interval, list)
        assert len(c.default_interval) == 2

    def test_yaml_roundtrip(self, tmp_path):
        cfg = Configuration()
        mgr = ConfigManager(tmp_path / "config.yaml")
        mgr.save(cfg)
        loaded = mgr.load()
        assert loaded.events == cfg.events
        assert loaded.default_interval == cfg.default_interval
