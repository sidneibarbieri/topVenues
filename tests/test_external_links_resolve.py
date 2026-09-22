"""Every way out of the interface must lead somewhere real.

The arXiv button sent `au:"Name"` to arXiv's web form under `searchtype=all`.
That prefix is API syntax; the form searched it as literal text and returned no
results for every author. The button worked, the page loaded, and the answer
was empty -- which no exception-based check can catch.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest

from src.research_intelligence import arxiv_author_search_url


@pytest.fixture
def query():
    def _query(author: str) -> dict[str, list[str]]:
        return parse_qs(urlparse(arxiv_author_search_url(author)).query)

    return _query


def test_the_author_goes_in_the_author_field(query):
    assert query("Yasemin Acar")["searchtype"] == ["author"]


def test_the_query_carries_the_plain_name(query):
    """`au:` is API syntax. The web form searches it as text."""
    value = query("Yasemin Acar")["query"][0]
    assert value == "Yasemin Acar"
    assert "au:" not in value


def test_a_dblp_homonym_suffix_is_dropped(query):
    """arXiv has no notion of DBLP's numeric disambiguator."""
    assert query("Qi Li 0002")["query"] == ["Qi Li"]


def test_the_url_points_at_arxiv_search():
    parsed = urlparse(arxiv_author_search_url("Ada Lovelace"))
    assert (parsed.scheme, parsed.netloc, parsed.path) == ("https", "arxiv.org", "/search/")


def test_every_download_and_link_on_every_page_is_live():
    """A disabled download or an empty link target is a dead end for a reviewer.

    These are not `st.button`, so an audit that walks buttons alone misses them
    entirely -- which is how the arXiv link shipped broken.
    """
    import logging

    from streamlit.testing.v1 import AppTest

    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(
        logging.ERROR
    )

    dead = []
    for page in ("Overview", "Search", "Insights", "Evidence", "Dataset lifecycle"):
        app = AppTest.from_file("web/app.py", default_timeout=300)
        app.run()
        app.radio(key="page").set_value(page).run()
        for download in app.get("download_button"):
            if download.proto.disabled or not download.proto.url:
                dead.append((page, "download", download.proto.label))
        for link in app.get("link_button"):
            if not link.proto.url.startswith("https://"):
                dead.append((page, "link", link.proto.label, link.proto.url))

    assert not dead, dead
