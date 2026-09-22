"""The interface's metric options must match the metrics the analytics accept.

Adding an option to the selectbox without wiring it raises a KeyError only when
a user picks it, which is the worst place to find out.
"""

from __future__ import annotations

import re
from pathlib import Path

from src.analytics import AUTHOR_RANKING_METRICS

APP = Path(__file__).resolve().parent.parent / "web" / "app.py"


def _metric_mapping() -> dict[str, str]:
    source = APP.read_text(encoding="utf-8")
    block = re.search(r"(\{[^{}]*?\})\[author_metric\]", source, re.S)
    assert block, "author metric mapping not found in web/app.py"
    return dict(re.findall(r'"([^"]+)":\s*"([^"]+)"', block.group(1)))


def _selectbox_options() -> list[str]:
    source = APP.read_text(encoding="utf-8")
    block = re.search(r'"Ranking metric",\s*\n\s*\[([^\]]+)\]', source)
    assert block, "ranking metric options not found in web/app.py"
    return re.findall(r'"([^"]+)"', block.group(1))


def test_every_offered_option_is_wired():
    mapping = _metric_mapping()
    for option in _selectbox_options():
        assert option in mapping, f"{option!r} is offered but not mapped to a metric"


def test_every_wired_metric_is_one_the_analytics_accept():
    for metric in _metric_mapping().values():
        assert metric in AUTHOR_RANKING_METRICS


def test_the_concentration_metric_reaches_the_interface():
    assert "Top-4 concentration" in _selectbox_options()
