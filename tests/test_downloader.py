"""Tests for JSONDownloader helpers."""

import json

import pytest

from src.downloader import JSONDownloader
from src.models import Configuration


@pytest.fixture
def downloader(tmp_path):
    return JSONDownloader(Configuration(), tmp_path / "log")


class TestGetEventUrls:
    def test_asiaccs_two_urls(self, downloader):
        urls = downloader._get_event_urls("asiaccs", 2022)
        assert len(urls) == 2
        assert any("asiaccs2022" in u for u in urls)

    def test_sacmat(self, downloader):
        urls = downloader._get_event_urls("sacmat", 2023)
        assert urls == ["https://dblp.org/db/conf/sacmat/sacmat2023.html"]

    def test_acsac(self, downloader):
        urls = downloader._get_event_urls("acsac", 2023)
        assert urls == ["https://dblp.org/db/conf/acsac/acsac2023.html"]

    def test_acm_csur(self, downloader):
        # Journals use volume-based URLs; 2022 maps to vol 54
        urls = downloader._get_event_urls("acm_csur", 2022)
        assert urls == ["https://dblp.org/db/journals/csur/csur54.html"]

    def test_ieee_comst(self, downloader):
        # 2022 maps to vol 24; DBLP path is comsur (not comst)
        urls = downloader._get_event_urls("ieee_comst", 2022)
        assert urls == ["https://dblp.org/db/journals/comsur/comsur24.html"]

    def test_fnt_privsec(self, downloader):
        # 2022 maps to vol 5; DBLP path is ftsec (not fntsec)
        urls = downloader._get_event_urls("fnt_privsec", 2022)
        assert urls == ["https://dblp.org/db/journals/ftsec/ftsec5.html"]

    def test_acm_csur_unknown_year(self, downloader):
        # Years outside the volume map return an empty list
        assert downloader._get_event_urls("acm_csur", 2010) == []

    def test_ieee_comst_unknown_year(self, downloader):
        assert downloader._get_event_urls("ieee_comst", 2010) == []

    def test_fnt_privsec_unknown_year(self, downloader):
        assert downloader._get_event_urls("fnt_privsec", 2010) == []

    def test_default_pattern(self, downloader):
        urls = downloader._get_event_urls("ccs", 2023)
        assert urls == ["https://dblp.org/db/conf/ccs/ccs2023.html"]


class TestValidateJson:
    def test_valid_structure(self, downloader, tmp_path):
        data = {"result": {"hits": {"hit": [{"info": {"title": "Demo"}}]}}}
        f = tmp_path / "ok.json"
        f.write_text(json.dumps(data), encoding="utf-8")
        assert downloader._validate_json(f)

    def test_empty_hit_list_is_not_materialized_data(self, downloader, tmp_path):
        data = {"result": {"hits": {"hit": []}}}
        f = tmp_path / "empty.json"
        f.write_text(json.dumps(data), encoding="utf-8")
        assert not downloader._validate_json(f)

    def test_missing_hits_key(self, downloader, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text(json.dumps({"result": {}}), encoding="utf-8")
        assert not downloader._validate_json(f)

    def test_invalid_json(self, downloader, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("not json", encoding="utf-8")
        assert not downloader._validate_json(f)

    def test_nonexistent_file(self, downloader, tmp_path):
        assert not downloader._validate_json(tmp_path / "ghost.json")


class TestRetryPolicy:
    @pytest.mark.parametrize("status_code", [400, 401, 403, 404])
    def test_permanent_client_errors_are_not_retried(self, downloader, status_code):
        assert downloader._is_permanent_client_error(status_code)

    @pytest.mark.parametrize("status_code", [200, 429, 500, 502, 503])
    def test_retryable_or_success_statuses_are_not_permanent(self, downloader, status_code):
        assert not downloader._is_permanent_client_error(status_code)


class TestDblpTocApi:
    def test_toc_key_from_standard_dblp_url(self, downloader):
        assert (
            downloader._toc_key_from_dblp_url("https://dblp.org/db/conf/aaai/aaai2025.html")
            == "db/conf/aaai/aaai2025.bht"
        )

    def test_toc_key_rejects_non_dblp_url(self, downloader):
        assert downloader._toc_key_from_dblp_url("https://example.com/aaai2025.html") is None

    def test_toc_api_url_uses_pagination_offset(self, downloader):
        url = downloader._toc_api_url("db/conf/aaai/aaai2025.bht", 1000)
        assert "format=json" in url
        assert "h=1000" in url
        assert "f=1000" in url
        assert "toc%3Adb/conf/aaai/aaai2025.bht%3A" in url
