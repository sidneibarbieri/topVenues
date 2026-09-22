"""A bar chart must keep a linear scale, because a bar starts at zero.

On a logarithmic scale there is no zero to start from, so Vega emitted every
bar as a zero-width path: the value labels rendered and the bars did not. The
"Papers by class" chart shipped that way, showing six floating numbers over an
empty frame.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import pytest

from web import charts


@pytest.fixture
def counts():
    return pd.DataFrame(
        [
            {"Class": "Article", "Papers": 11609},
            {"Class": "Workshop", "Papers": 86},
        ]
    )


def _bar_layer(chart: alt.Chart) -> dict:
    spec = chart.to_dict()
    return next(layer for layer in spec["layer"] if layer["mark"]["type"] == "bar")


def test_a_bar_chart_keeps_a_linear_value_scale(counts):
    chart = charts.bar_chart(counts, "Class", "Papers", alt.selection_point("s", fields=["Class"]))
    scale = _bar_layer(chart)["encoding"]["x"].get("scale", {})
    assert scale.get("type", "linear") == "linear"


def test_the_label_can_carry_more_than_the_raw_value(counts):
    """A sub-pixel bar needs its share printed; length cannot express it."""
    counts["Label"] = ["11,609 (99.3%)", "86 (0.7%)"]
    chart = charts.bar_chart(
        counts,
        "Class",
        "Papers",
        alt.selection_point("s", fields=["Class"]),
        label_field="Label",
    )
    spec = chart.to_dict()
    text_layer = next(layer for layer in spec["layer"] if layer["mark"]["type"] == "text")
    assert text_layer["encoding"]["text"]["field"] == "Label"


def test_the_class_chart_labels_pair_a_count_with_a_share():
    from web.app import _count_with_share

    labels = _count_with_share(pd.Series([11609, 1692, 86]))

    assert labels[0].startswith("11,609 (")
    assert labels[-1] == "86 (0.6%)"
