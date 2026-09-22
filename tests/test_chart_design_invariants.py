"""Lock the chart design decisions that integrations have silently dropped.

Visual regressions do not fail a test suite by themselves, so each of these
decisions was lost at least once during a branch integration and only found by
re-rendering the interface. These assertions make the loss fail in CI instead.
"""

from __future__ import annotations

import pandas as pd

from web import charts
from web.theme import LIGHT


def _sample(rows: int) -> pd.DataFrame:
    return pd.DataFrame(
        {"Venue": [f"venue {index}" for index in range(rows)], "Papers": range(rows, 0, -1)}
    )


def test_bars_declare_one_thickness_for_the_whole_application():
    """A 20-bar chart beside an 8-bar chart must not draw different bars."""
    assert isinstance(charts.BAR_THICKNESS, int)
    chart = charts.bar_chart(
        _sample(6), "Venue", "Papers", charts.alt.selection_point(), LIGHT.chart
    )
    bar_layer = chart.to_dict()["layer"][0]
    assert bar_layer["mark"]["size"] == charts.BAR_THICKNESS


def test_horizontal_height_grows_with_the_bar_count():
    """Otherwise thickness silently shrinks as categories are added."""
    small = charts.bar_chart(
        _sample(4), "Venue", "Papers", charts.alt.selection_point(), LIGHT.chart
    )
    large = charts.bar_chart(
        _sample(20), "Venue", "Papers", charts.alt.selection_point(), LIGHT.chart
    )
    assert large.to_dict()["height"] > small.to_dict()["height"]


def test_every_bar_carries_its_value():
    """A dashboard read at a glance must not require hovering to learn a number."""
    chart = charts.bar_chart(
        _sample(5), "Venue", "Papers", charts.alt.selection_point(), LIGHT.chart
    )
    layers = chart.to_dict()["layer"]
    assert any(layer["mark"]["type"] == "text" for layer in layers)


def test_the_palette_is_declared_in_one_place():
    """Hex literals at call sites are how the palette drifted before."""
    import re
    from pathlib import Path

    web = Path(charts.__file__).parent
    for name in ("app.py", "charts.py", "styles.css"):
        source = (web / name).read_text(encoding="utf-8")
        assert not re.search(r"#[0-9A-Fa-f]{6}\b", source), f"web/{name} hardcodes a colour"


def test_unselected_bars_read_as_clearly_dimmed():
    """A subtle difference makes the click-to-filter affordance invisible."""
    assert charts.UNSELECTED_OPACITY <= 0.5


def test_every_headline_card_is_computed_from_the_corpus():
    """A hardcoded card cannot drift with the data, so it silently goes stale.

    The fourth card used to read a literal "Offline", which also broke the row's
    reading: three measured values beside one status word.
    """
    from web.app import _artifact_claims

    stats = {
        "total_papers": 100,
        "with_abstracts": 90,
        "with_bibtex": 100,
        "by_event": {"ACM CCS": 60, "NDSS": 40},
        "by_year": {2019: 50, 2026: 50},
    }
    assert [card.value for card in _artifact_claims(stats)] == [
        "100",
        "90.0%",
        "100.0%",
        "2019–2026",
    ]


def test_both_headline_rows_use_the_same_card_type():
    """Two card components drifted apart in palette, precision and markup.

    The Search row tinted its cards amber and rose, which read as warning and
    error on a 94% and a 100% figure, and printed two decimals where the
    Overview row printed one.
    """
    from web.app import HeadlineCard, _artifact_claims, _corpus_cards

    stats = {
        "total_papers": 200,
        "with_abstracts": 188,
        "with_bibtex": 200,
        "by_event": {"ACM CCS": 200},
        "by_year": {2020: 200},
    }
    rows = (_artifact_claims(stats), _corpus_cards(stats, filtered_count=50))

    for row in rows:
        assert all(isinstance(card, HeadlineCard) for card in row)
    assert _corpus_cards(stats)[1].note == "94.0% coverage"
    assert _corpus_cards(stats, filtered_count=50)[3].note == "25.0% of the corpus"


def test_a_line_prints_its_values_like_every_bar_does():
    """The module's promise is that no reader hovers to learn a number.

    The topic-share chart was the one series without printed values, and it is
    the normalized one: the number a reader would actually quote.
    """
    import altair as alt
    import pandas as pd

    from web import charts

    data = pd.DataFrame([{"Year": 2024, "Share (%)": 1.8}, {"Year": 2025, "Share (%)": 9.5}])
    chart = charts.line_chart(
        data,
        "Year",
        "Share (%)",
        alt.selection_point("s", fields=["Year"]),
        LIGHT.chart,
        value_format=".1f",
    )

    text_layers = [layer for layer in chart.to_dict()["layer"] if layer["mark"]["type"] == "text"]
    assert text_layers, "the line chart prints no values"
    assert text_layers[0]["encoding"]["text"]["format"] == ".1f"


def test_a_horizontal_chart_is_as_tall_as_its_bars():
    """A height floor stretches the gaps, not the bars.

    Six bars in a 320px frame drew on a 43px rhythm while a 20-bar chart drew
    on 32px, and left 63px of empty frame below the last bar.
    """
    import altair as alt
    import pandas as pd

    from web import charts

    data = pd.DataFrame([{"Class": name, "Papers": 10} for name in "abcdef"])
    chart = charts.bar_chart(
        data, "Class", "Papers", alt.selection_point("s", fields=["Class"]), LIGHT.chart, height=320
    )

    assert chart.to_dict()["height"] == len(data) * (charts.BAR_THICKNESS + charts.BAR_GAP)


def test_series_differ_by_dash_as_well_as_hue():
    """No chart may rely on colour alone (docs/brand/BRAND.md)."""
    assert len(charts.SERIES_DASH) == len(LIGHT.chart.series)
    assert len(set(charts.SERIES_DASH)) == len(charts.SERIES_DASH)
    assert charts.series_legend().to_dict()["symbolType"] == "stroke"


def test_the_shared_visual_language_survives_every_configure_call():
    """Altair's `configure` replaces the config; placed last, it erased the rest."""
    chart = charts.apply_theme(
        charts.bar_chart(_sample(3), "Venue", "Papers", charts.alt.selection_point(), LIGHT.chart),
        LIGHT.chart,
    )
    config = chart.to_dict()["config"]
    assert config["background"] == "transparent"
    assert config["axis"]["domain"] is False and config["axis"]["ticks"] is False
    assert config["axis"]["labelColor"] == LIGHT.chart.muted
    assert config["view"]["strokeOpacity"] == 0
    assert config["legend"]["labelColor"] == LIGHT.chart.muted


def test_a_coverage_track_sorts_by_the_value_field():
    """A channel sort ("-x") has no field in the track layer and Vega draws nothing."""
    chart = charts.bar_chart(
        _sample(3), "Venue", "Papers", charts.alt.selection_point(), LIGHT.chart, whole=100
    )
    layers = chart.to_dict()["layer"]
    assert layers[0]["mark"]["color"] == LIGHT.chart.track
    for layer in layers:
        assert layer["encoding"]["y"]["sort"] == {"field": "Papers", "order": "descending"}
