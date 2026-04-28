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
        data = {"result": {"hits": {"hit": []}}}
        f = tmp_path / "ok.json"
        f.write_text(json.dumps(data), encoding="utf-8")
        assert downloader._validate_json(f)

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
