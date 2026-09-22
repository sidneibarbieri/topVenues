"""The interface's metric options must match the metrics the analytics accept.

The options and their metric names live in one mapping, RANKING_METRICS, so an
option cannot be offered without a metric. What can still drift is the metric
name itself, which the analytics reject only when a reader picks it.
"""

from __future__ import annotations

import ast
from pathlib import Path

from src.analytics import AUTHOR_RANKING_METRICS

APP = Path(__file__).resolve().parent.parent / "web" / "app.py"


def _literal(node: ast.expr) -> str:
    """A string literal, or one wrapped in marked() for translation."""
    if isinstance(node, ast.Call):
        node = node.args[0]
    assert isinstance(node, ast.Constant)
    return node.value


def _ranking_metrics() -> dict[str, str]:
    tree = ast.parse(APP.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "RANKING_METRICS"
            for target in node.targets
        ):
            assert isinstance(node.value, ast.Dict)
            return {
                _literal(key): _literal(value)
                for key, value in zip(node.value.keys, node.value.values, strict=True)
            }
    raise AssertionError("RANKING_METRICS not found in web/app.py")


def test_every_offered_metric_is_one_the_analytics_accept():
    for metric in _ranking_metrics().values():
        assert metric in AUTHOR_RANKING_METRICS


def test_the_concentration_metric_reaches_the_interface():
    assert _ranking_metrics().get("Top-4 concentration") == "top4_concentration"


def test_the_selectbox_offers_exactly_the_wired_metrics():
    assert "tuple(RANKING_METRICS)" in APP.read_text(encoding="utf-8")
