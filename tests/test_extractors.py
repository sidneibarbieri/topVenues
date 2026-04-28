"""
Extractor tests using realistic content samples.

- IEEE: tests the regex/JSON parsing methods directly (no subprocess needed).
- USENIX, NDSS, ACM: mock _run_xidel to return realistic xidel output,
  then verify the full extract() path — routing, cleaning, validation.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.extractors.acm import ACMExtractor
from src.extractors.ieee import IEEEExtractor
from src.extractors.ndss import NDSSExtractor
from src.extractors.usenix import USENIXExtractor
from src.models import Configuration

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REAL_ABSTRACT = (
    "We present a formal analysis of group signature schemes supporting "
    "dynamic membership. Our model captures the notion of membership privacy, "
    "which requires that an adversary cannot determine which members signed a "
    "given message. We prove that several existing constructions satisfy our "
    "definitions and discuss the tradeoffs between security and efficiency."
)


def _make_collector(blocked: bool = False, failure_count: int = 0) -> MagicMock:
    """Return a minimal Collector mock for extractor tests."""
    collector = MagicMock()
    collector.config = Configuration()
    collector.config.user_agents = ["Mozilla/5.0 TestAgent"]
    collector.is_acm_blocked.return_value = blocked
    collector.get_acm_failure_count.return_value = failure_count
    collector.reset_acm_failure_count = MagicMock()
    collector.increment_acm_failure_count = MagicMock()
    return collector


# ---------------------------------------------------------------------------
# IEEE extractor — parsing methods (no network, no subprocess)
# ---------------------------------------------------------------------------

class TestIEEEExtractFromRegex:
    def setup_method(self):
        self.ext = IEEEExtractor()

    def test_finds_abstract_field(self):
        script = 'xplGlobal.document.metadata={"abstract":"Side-channel attacks exploit leaked information.","title":"Test"};'
        result = self.ext._extract_from_regex(script)
        assert result == "Side-channel attacks exploit leaked information."

    def test_unescapes_quotes(self):
        script = 'xplGlobal.document.metadata={"abstract":"Alice said \\"hello\\"."};'
        result = self.ext._extract_from_regex(script)
        assert result == 'Alice said "hello".'

    def test_returns_none_when_no_abstract_key(self):
        script = 'xplGlobal.document.metadata={"title":"No abstract here"};'
        assert self.ext._extract_from_regex(script) is None

    def test_returns_none_on_empty_script(self):
        assert self.ext._extract_from_regex("") is None


class TestIEEEExtractFromJson:
    def setup_method(self):
        self.ext = IEEEExtractor()

    def test_parses_valid_metadata_object(self):
        metadata = {"abstract": REAL_ABSTRACT, "title": "Formal Analysis"}
        script = f"xplGlobal.document.metadata = {json.dumps(metadata)};"
        result = self.ext._extract_from_json(script)
        assert result == REAL_ABSTRACT

    def test_returns_none_when_no_abstract_key(self):
        metadata = {"title": "Only title"}
        script = f"xplGlobal.document.metadata = {json.dumps(metadata)};"
        assert self.ext._extract_from_json(script) is None

    def test_returns_none_when_pattern_missing(self):
        assert self.ext._extract_from_json("var x = 1;") is None

    def test_returns_none_on_broken_json(self):
        script = "xplGlobal.document.metadata = {broken json};"
        assert self.ext._extract_from_json(script) is None


class TestIEEEExtractFullPath:
    """End-to-end: mock _get_metadata_script and verify the extract() path."""

    @pytest.mark.asyncio
    async def test_regex_path_succeeds(self):
        ext = IEEEExtractor()
        collector = _make_collector()
        script = f'xplGlobal.document.metadata={{"abstract":"{REAL_ABSTRACT}"}};'
        with patch.object(ext, "_get_metadata_script", new=AsyncMock(return_value=script)):
            result = await ext.extract("https://ieeexplore.ieee.org/document/1234", "id1", collector)
        assert result is not None
        assert len(result) > 100

    @pytest.mark.asyncio
    async def test_falls_back_to_json_path(self):
        ext = IEEEExtractor()
        collector = _make_collector()
        # Regex path won't match (no quotes around value), but JSON path will
        metadata = {"abstract": REAL_ABSTRACT}
        script = f"xplGlobal.document.metadata = {json.dumps(metadata)};"
        with patch.object(ext, "_get_metadata_script", new=AsyncMock(return_value=script)):
            result = await ext.extract("https://ieeexplore.ieee.org/document/1234", "id1", collector)
        assert result is not None

    @pytest.mark.asyncio
    async def test_returns_none_when_script_absent(self):
        ext = IEEEExtractor()
        collector = _make_collector()
        with patch.object(ext, "_get_metadata_script", new=AsyncMock(return_value=None)):
            result = await ext.extract("https://ieeexplore.ieee.org/document/1234", "id1", collector)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_abstract_too_short(self):
        ext = IEEEExtractor()
        collector = _make_collector()
        script = 'xplGlobal.document.metadata={"abstract":"Too short."};'
        with patch.object(ext, "_get_metadata_script", new=AsyncMock(return_value=script)):
            result = await ext.extract("https://ieeexplore.ieee.org/document/1234", "id1", collector)
        assert result is None


# ---------------------------------------------------------------------------
# USENIX extractor — mock _run_xidel
# ---------------------------------------------------------------------------

class TestUSENIXExtractor:
    @pytest.mark.asyncio
    async def test_returns_abstract_on_first_xpath_match(self):
        ext = USENIXExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=REAL_ABSTRACT)):
            result = await ext.extract(
                "https://www.usenix.org/conference/usenixsecurity19/presentation/test",
                "uid1",
                collector,
            )
        assert result is not None
        assert len(result) >= 100

    @pytest.mark.asyncio
    async def test_tries_next_xpath_when_first_returns_short_text(self):
        ext = USENIXExtractor()
        collector = _make_collector()
        # First XPath returns garbage; second returns real content
        call_results = iter(["too short", REAL_ABSTRACT])
        with patch.object(ext, "_run_xidel", new=AsyncMock(side_effect=call_results)):
            result = await ext.extract(
                "https://www.usenix.org/conference/usenixsecurity19/presentation/test",
                "uid1",
                collector,
            )
        assert result is not None

    @pytest.mark.asyncio
    async def test_returns_none_when_all_xpaths_fail(self):
        ext = USENIXExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=None)):
            result = await ext.extract(
                "https://www.usenix.org/conference/usenixsecurity19/presentation/test",
                "uid1",
                collector,
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_strips_abstract_prefix(self):
        ext = USENIXExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value="Abstract: " + REAL_ABSTRACT)):
            result = await ext.extract(
                "https://www.usenix.org/conference/usenixsecurity19/presentation/test",
                "uid1",
                collector,
            )
        assert result is not None
        assert not result.lower().startswith("abstract")

    @pytest.mark.asyncio
    async def test_strips_usenix_open_access_footer(self):
        ext = USENIXExtractor()
        collector = _make_collector()
        text_with_footer = REAL_ABSTRACT + " USENIX is committed to Open Access to research."
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=text_with_footer)):
            result = await ext.extract(
                "https://www.usenix.org/conference/usenixsecurity19/presentation/test",
                "uid1",
                collector,
            )
        assert result is not None
        assert "USENIX is committed" not in result


# ---------------------------------------------------------------------------
# NDSS extractor — mock _run_xidel
# ---------------------------------------------------------------------------

class TestNDSSExtractor:
    @pytest.mark.asyncio
    async def test_returns_abstract_on_first_xpath_match(self):
        ext = NDSSExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=REAL_ABSTRACT)):
            result = await ext.extract(
                "https://www.ndss-symposium.org/ndss-paper/test/",
                "nid1",
                collector,
            )
        assert result is not None

    @pytest.mark.asyncio
    async def test_falls_through_to_second_xpath(self):
        ext = NDSSExtractor()
        collector = _make_collector()
        call_results = iter([None, REAL_ABSTRACT])
        with patch.object(ext, "_run_xidel", new=AsyncMock(side_effect=call_results)):
            result = await ext.extract(
                "https://www.ndss-symposium.org/ndss-paper/test/",
                "nid1",
                collector,
            )
        assert result is not None

    @pytest.mark.asyncio
    async def test_returns_none_when_all_fail(self):
        ext = NDSSExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=None)):
            result = await ext.extract(
                "https://www.ndss-symposium.org/ndss-paper/test/",
                "nid1",
                collector,
            )
        assert result is None


# ---------------------------------------------------------------------------
# ACM extractor — mock _run_xidel, test rate-limit logic
# ---------------------------------------------------------------------------

class TestACMExtractor:
    @pytest.mark.asyncio
    async def test_returns_abstract_and_resets_failure_count(self):
        ext = ACMExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=REAL_ABSTRACT)):
            result = await ext.extract(
                "https://dl.acm.org/doi/10.1145/3319535.3354257",
                "aid1",
                collector,
            )
        assert result is not None
        collector.reset_acm_failure_count.assert_called_once()
        collector.increment_acm_failure_count.assert_not_called()

    @pytest.mark.asyncio
    async def test_increments_failure_count_when_xidel_returns_none(self):
        ext = ACMExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=None)):
            result = await ext.extract(
                "https://dl.acm.org/doi/10.1145/3319535.3354257",
                "aid1",
                collector,
            )
        assert result is None
        collector.increment_acm_failure_count.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_none_immediately_when_blocked(self):
        ext = ACMExtractor()
        collector = _make_collector(blocked=True)
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=REAL_ABSTRACT)) as mock_xidel:
            result = await ext.extract(
                "https://dl.acm.org/doi/10.1145/3319535.3354257",
                "aid1",
                collector,
            )
        assert result is None
        mock_xidel.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_none_when_failure_threshold_reached(self):
        ext = ACMExtractor()
        collector = _make_collector(failure_count=3)  # equals acm_failure_threshold
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=REAL_ABSTRACT)) as mock_xidel:
            result = await ext.extract(
                "https://dl.acm.org/doi/10.1145/3319535.3354257",
                "aid1",
                collector,
            )
        assert result is None
        mock_xidel.assert_not_called()

    @pytest.mark.asyncio
    async def test_strips_abstract_prefix_from_acm_content(self):
        ext = ACMExtractor()
        collector = _make_collector()
        content_with_prefix = "Abstract " + REAL_ABSTRACT
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value=content_with_prefix)):
            result = await ext.extract(
                "https://dl.acm.org/doi/10.1145/3319535.3354257",
                "aid1",
                collector,
            )
        assert result is not None
        assert not result.lower().startswith("abstract")

    @pytest.mark.asyncio
    async def test_increments_failure_when_result_too_short(self):
        ext = ACMExtractor()
        collector = _make_collector()
        with patch.object(ext, "_run_xidel", new=AsyncMock(return_value="too short")):
            result = await ext.extract(
                "https://dl.acm.org/doi/10.1145/3319535.3354257",
                "aid1",
                collector,
            )
        assert result is None
        collector.increment_acm_failure_count.assert_called_once()
