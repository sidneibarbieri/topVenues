"""Tests for DataConsolidator."""

import json
from pathlib import Path

import pytest

from src.consolidator import DataConsolidator


@pytest.fixture
def consolidator(tmp_path):
    return DataConsolidator(tmp_path / "json", tmp_path / "data")


class TestNormalizeEvent:
    def test_ccs(self, consolidator):
        assert consolidator._normalize_event("CCS") == "ACM CCS"

    def test_acm_ccs(self, consolidator):
        assert consolidator._normalize_event("acm ccs") == "ACM CCS"

    def test_asiaccs(self, consolidator):
        assert consolidator._normalize_event("asiaccs") == "ACM ASIA CCS"

    def test_asia_ccs_with_hyphen(self, consolidator):
        assert consolidator._normalize_event("asia-ccs") == "ACM ASIA CCS"

    def test_ndss(self, consolidator):
        assert consolidator._normalize_event("NDSS") == "NDSS"

    def test_usenix(self, consolidator):
        assert consolidator._normalize_event("USENIX Security") == "USENIX Security"

    def test_ieee_sp(self, consolidator):
        assert consolidator._normalize_event("SP") == "IEEE S&P"

    def test_eurosp(self, consolidator):
        assert consolidator._normalize_event("EuroS&P") == "IEEE EURO S&P"

    def test_hotnets(self, consolidator):
        assert consolidator._normalize_event("hotnets") == "HotNets"

    def test_sacmat(self, consolidator):
        assert consolidator._normalize_event("sacmat") == "ACM SACMAT"

    def test_acm_csur(self, consolidator):
        assert consolidator._normalize_event("csur") == "ACM Computing Surveys"

    def test_ieee_comst(self, consolidator):
        assert consolidator._normalize_event("comst") == "IEEE Communications Surveys & Tutorials"

    def test_unknown_passes_through(self, consolidator):
        assert consolidator._normalize_event("SomethingElse") == "SomethingElse"

    def test_empty_string(self, consolidator):
        assert consolidator._normalize_event("") == ""


class TestExtractAuthors:
    def test_single_author_dict(self, consolidator):
        result = consolidator._extract_authors({"author": {"text": "Alice"}})
        assert result == "Alice"

    def test_multiple_authors(self, consolidator):
        result = consolidator._extract_authors({
            "author": [{"text": "Alice"}, {"text": "Bob"}]
        })
        assert result == "Alice, Bob"

    def test_empty(self, consolidator):
        assert consolidator._extract_authors({}) is None

    def test_none(self, consolidator):
        assert consolidator._extract_authors(None) is None


class TestNormalizeEventJournalAbbreviations:
    """_normalize_event must handle raw DBLP journal abbreviation strings."""

    def test_dblp_csur_abbreviation(self, consolidator):
        # DBLP returns "ACM Comput. Surv." as the venue string
        assert consolidator._normalize_event("ACM Comput. Surv.") == "ACM Computing Surveys"

    def test_dblp_comst_abbreviation(self, consolidator):
        # DBLP returns "IEEE Commun. Surv. Tutorials" as the venue string
        result = consolidator._normalize_event("IEEE Commun. Surv. Tutorials")
        assert result == "IEEE Communications Surveys & Tutorials"

    def test_dblp_ftsec_abbreviation(self, consolidator):
        # DBLP returns "Found. Trends Priv. Secur." as the venue string
        result = consolidator._normalize_event("Found. Trends Priv. Secur.")
        assert result == "Foundations and Trends in Privacy and Security"

    def test_eurosp_ampersand(self, consolidator):
        # DBLP venue "EuroS&P" → lowercases to "euros&p", not "eurosp"
        assert consolidator._normalize_event("EuroS&P") == "IEEE EURO S&P"


class TestProcessJsonFile:
    def _make_json(self, tmp_path: Path, hits: list[dict]) -> Path:
        data = {"result": {"hits": {"hit": hits}}}
        f = tmp_path / "test.json"
        f.write_text(json.dumps(data), encoding="utf-8")
        return f

    def test_valid_paper_parsed(self, tmp_path, consolidator):
        hit = {
            "@id": "42",
            "@score": "1",
            "info": {
                "title": "Zero-Day Exploits",
                "year": "2023",
                "venue": "CCS",
                "type": "article",
                "authors": {"author": [{"text": "Alice"}, {"text": "Bob"}]},
            },
        }
        path = self._make_json(tmp_path, [hit])
        papers = consolidator._process_json_file(path)
        assert len(papers) == 1
        assert papers[0].title == "Zero-Day Exploits"
        assert papers[0].event == "ACM CCS"
        assert papers[0].authors == "Alice, Bob"

    def test_missing_required_fields_skipped(self, tmp_path, consolidator):
        hit = {"@id": "99", "info": {"title": "No Year"}}
        path = self._make_json(tmp_path, [hit])
        papers = consolidator._process_json_file(path)
        assert papers == []

    def test_invalid_json_raises(self, tmp_path, consolidator):
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        with pytest.raises(json.JSONDecodeError):
            consolidator._process_json_file(bad)
