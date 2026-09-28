"""Tests for the source-bounded IEEE S&P award collector."""

from __future__ import annotations

from scripts.collect_ieee_sp_awards import SOURCE_URLS, collect_page


def test_collector_reads_only_declared_paper_award_sections() -> None:
    html = """
    <h2>Best Paper Award</h2>
    <p><strong>Paper One</strong><br>Authors</p>
    <h2>Test of Time Award</h2>
    <p><strong>Old Paper</strong><br>Authors</p>
    <h2>Best Practical Paper Award</h2>
    <p><strong>Paper Two</strong><br>Authors</p>
    """

    records = collect_page(2020, SOURCE_URLS[2020], html)

    assert [(record.award, record.title) for record in records] == [
        ("Best Paper Award", "Paper One"),
        ("Best Practical Paper Award", "Paper Two"),
    ]


def test_every_source_is_an_official_ieee_security_page() -> None:
    assert set(SOURCE_URLS) == set(range(2019, 2025))
    assert all(url.startswith("https://www.ieee-security.org/") for url in SOURCE_URLS.values())
