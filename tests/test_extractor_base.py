"""Tests for AbstractExtractor shared utilities."""

import pytest

from src.extractors.base import AbstractExtractor


class _DummyExtractor(AbstractExtractor):
    async def extract(self, paper_url, paper_id, collector):
        return None


@pytest.fixture
def extractor():
    return _DummyExtractor()


class TestCleanAbstract:
    def test_none_returns_none(self, extractor):
        assert extractor._clean_abstract(None) is None

    def test_empty_returns_none(self, extractor):
        assert extractor._clean_abstract("   ") is None

    def test_strips_usenix_footer(self, extractor):
        text = "Great research. USENIX is committed to Open Access for the benefit of security."
        result = extractor._clean_abstract(text)
        assert "USENIX is committed" not in result
        assert "Great research" in result

    def test_strips_abstract_prefix(self, extractor):
        result = extractor._clean_abstract("Abstract: We propose a novel method.")
        assert result == "We propose a novel method."

    def test_normalises_whitespace(self, extractor):
        result = extractor._clean_abstract("too   many    spaces")
        assert result == "too many spaces"

    def test_strips_author_line(self, extractor):
        text = "Alice Bob (MIT): This paper presents..."
        result = extractor._clean_abstract(text)
        assert "Alice Bob" not in result


class TestIsValidAbstract:
    def test_none_is_invalid(self, extractor):
        assert not extractor._is_valid_abstract(None)

    def test_short_text_invalid(self, extractor):
        assert not extractor._is_valid_abstract("Too short")

    def test_false_positive_invalid(self, extractor):
        assert not extractor._is_valid_abstract("true")
        assert not extractor._is_valid_abstract("false")
        assert not extractor._is_valid_abstract("null")

    def test_long_text_valid(self, extractor):
        text = "a" * 150
        assert extractor._is_valid_abstract(text)

    def test_exactly_min_length_valid(self, extractor):
        text = "x" * 100
        assert extractor._is_valid_abstract(text)

    def test_one_below_min_invalid(self, extractor):
        text = "x" * 99
        assert not extractor._is_valid_abstract(text)
