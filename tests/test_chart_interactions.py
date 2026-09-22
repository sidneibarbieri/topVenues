"""Tests for stable extraction of Streamlit chart selection payloads."""

from src.chart_interactions import selected_chart_value


def test_reads_documented_point_selection_shape() -> None:
    event = {"selection": {"venue_selection": [{"Venue": "ACM CCS"}]}}
    assert selected_chart_value(event, "venue_selection", "Venue") == "ACM CCS"


def test_reads_field_array_selection_shape() -> None:
    event = {"selection": {"year_selection": {"Year": [2025]}}}
    assert selected_chart_value(event, "year_selection", "Year") == 2025


def test_returns_none_for_empty_or_unrelated_selection() -> None:
    assert selected_chart_value({"selection": {"point": {}}}, "point", "Venue") is None
