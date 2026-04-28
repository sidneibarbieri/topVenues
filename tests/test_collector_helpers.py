"""Tests for Collector helper functions."""

from src.collector import _extract_doi


class TestExtractDoi:
    def test_doi_org_url(self):
        assert _extract_doi("https://doi.org/10.1145/3319535.3354257") == "10.1145/3319535.3354257"

    def test_bare_doi(self):
        assert _extract_doi("10.1109/SP.2023.00042") == "10.1109/SP.2023.00042"

    def test_http_doi_url(self):
        assert _extract_doi("http://doi.org/10.1145/abc") == "10.1145/abc"

    def test_non_doi_url_returns_none(self):
        assert _extract_doi("https://www.usenix.org/conference/usenixsecurity23/paper") is None

    def test_none_returns_none(self):
        assert _extract_doi(None) is None

    def test_empty_string_returns_none(self):
        assert _extract_doi("") is None


class TestExtractorRouting:
    """Verify get_extractor_for_event returns the correct extractor type."""

    def test_usenix(self):
        from src.extractors import USENIXExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("USENIX Security"), USENIXExtractor)

    def test_ndss(self):
        from src.extractors import NDSSExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("NDSS"), NDSSExtractor)

    def test_ieee_sp(self):
        from src.extractors import IEEEExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("IEEE S&P"), IEEEExtractor)

    def test_ieee_euro_sp(self):
        from src.extractors import IEEEExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("IEEE EURO S&P"), IEEEExtractor)

    def test_acm_ccs(self):
        from src.extractors import ACMExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("ACM CCS"), ACMExtractor)

    def test_acm_computing_surveys(self):
        # ACM CSUR is on dl.acm.org → must use ACMExtractor
        from src.extractors import ACMExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("ACM Computing Surveys"), ACMExtractor)

    def test_hotnets(self):
        from src.extractors import ACMExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("HotNets"), ACMExtractor)

    def test_unknown_falls_back_to_acm(self):
        from src.extractors import ACMExtractor, get_extractor_for_event
        assert isinstance(get_extractor_for_event("UnknownVenue"), ACMExtractor)
