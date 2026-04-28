"""Tests for Pydantic models and SearchFilters."""

import pytest
from pydantic import ValidationError

from src.config import ConfigManager
from src.models import Configuration, Paper, SearchFilters


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
