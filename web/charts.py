"""Chart construction for the TopVenues web interface.

Presentation lives here so the page modules stay about data and flow. Every
chart in the application is built through these helpers, which makes the visual
language a single decision rather than a per-call-site one.

The design is deliberately restrained: one accent colour, horizontal rules
only, no borders, and the value printed next to each bar so a reader never has
to hover to learn a number.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

# Data colours, not brand colours (docs/brand/BRAND.md): Corpus Blue identifies
# TopVenues and never encodes a value. One data hue, one ink, one muted tone;
# extra colour here would carry no meaning.
ACCENT = "#44607F"
# Coverage answers "where can this corpus not speak?", so it reads apart from
# the volume charts on purpose.
COVERAGE = "#4F7D4A"
# Multi-series charts need one hue per series to stay readable, and a second
# channel (line dash) so that no chart relies on colour alone. The two extra
# hues are Okabe-Ito colours, distinguishable under common colour-vision
# deficiencies. Single-series charts do not use them: there, a second colour
# would mean nothing.
SERIES = ("#44607F", "#D55E00", "#009E73")
SERIES_DASH = ((1, 0), (6, 3), (2, 3))
INK = "#10233F"
MUTED = "#667085"
RULE = "#E9EEF4"
FONT = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"

# Unselected bars must read as clearly dimmed, not merely lighter.
SELECTED_OPACITY = 1.0
UNSELECTED_OPACITY = 0.32

# One bar thickness across the application. Letting height drive thickness made
# a 20-bar chart draw 19px bars beside an 8-bar chart drawing 41px ones, and the
# two read as different design systems when placed side by side.
BAR_THICKNESS = 22
BAR_GAP = 10

LABEL_SIZE = 12
TITLE_SIZE = 12
VALUE_LABEL_SIZE = 11


def series_legend() -> alt.Legend:
    """Legend entries drawn as the line itself, so dash identifies a series too."""
    return alt.Legend(symbolType="stroke", symbolStrokeWidth=2.3, symbolSize=320)


def apply_theme(chart: alt.Chart) -> alt.Chart:
    """Apply the shared visual language to a finished chart."""
    return (
        chart.configure_axis(
            domain=False,
            ticks=False,
            grid=False,
            labelColor=MUTED,
            titleColor=MUTED,
            labelFont=FONT,
            titleFont=FONT,
            labelFontSize=LABEL_SIZE,
            titleFontSize=TITLE_SIZE,
            titleFontWeight="normal",
            labelPadding=6,
            titlePadding=10,
        )
        .configure_view(strokeOpacity=0)
        .configure(background="transparent")
        .configure_axisY(grid=False)
        .configure_axisX(grid=True, gridColor=RULE, gridDash=[2, 3])
    )


def bar_chart(
    data: pd.DataFrame,
    category: str,
    value: str,
    selection: alt.Parameter,
    *,
    horizontal: bool = True,
    sort: str | list | None = "-x",
    category_title: str | None = None,
    value_title: str | None = None,
    value_format: str = ",",
    height: int = 320,
    color: str = ACCENT,
    value_scale: alt.Scale | None = None,
    label_field: str | None = None,
) -> alt.Chart:
    """A selectable bar chart with the value printed beside every bar.

    `label_field` names a column to print instead of the raw value, for when a
    bar is worth more than one number, such as a count beside its share.

    `height` applies to vertical charts. A horizontal chart derives its height
    from how many bars it has, so bars keep one rhythm across the application.

    Both marks share one base encoding. Encoding the layers separately makes
    Altair resolve two axes for the same channel, and the second one paints
    over the category names.

    A bar encodes magnitude as length from zero, so `value_scale` must stay
    linear. A logarithmic scale has no zero to start from, and Vega draws every
    bar zero pixels wide: the labels appear, the bars do not.
    """
    category_axis = alt.Axis(labelAngle=0, labelLimit=200)
    if horizontal:
        base = alt.Chart(data).encode(
            y=alt.Y(f"{category}:N", sort=sort, title=category_title, axis=category_axis),
            x=alt.X(
                f"{value}:Q",
                title=value_title,
                axis=alt.Axis(tickCount=4),
                scale=value_scale or alt.Undefined,
            ),
        )
    else:
        base = alt.Chart(data).encode(
            x=alt.X(f"{category}:O", sort=sort, title=category_title, axis=category_axis),
            y=alt.Y(
                f"{value}:Q",
                title=value_title,
                axis=alt.Axis(tickCount=4),
                scale=value_scale or alt.Undefined,
            ),
        )

    bars = (
        base.mark_bar(color=color, cornerRadiusEnd=2, size=BAR_THICKNESS)
        .encode(
            tooltip=[
                alt.Tooltip(f"{category}:N", title=category_title or category),
                alt.Tooltip(f"{value}:Q", title=value_title or value, format=value_format),
            ],
            opacity=alt.condition(
                selection, alt.value(SELECTED_OPACITY), alt.value(UNSELECTED_OPACITY)
            ),
        )
        .add_params(selection)
    )

    labels = base.mark_text(
        align="left" if horizontal else "center",
        baseline="middle",
        dx=6 if horizontal else 0,
        dy=0 if horizontal else -9,
        color=MUTED,
        font=FONT,
        fontSize=VALUE_LABEL_SIZE,
    ).encode(
        text=alt.Text(f"{label_field}:N")
        if label_field
        else alt.Text(f"{value}:Q", format=value_format)
    )

    # The value label sits outside the longest bar, so the plot needs room on
    # that side or the largest number is clipped at the frame.
    padding = {"right": 96 if label_field else 44} if horizontal else {"top": 18}
    # A horizontal chart's height is the bar count, not the caller's number. A
    # floor taller than the bars stretches the gaps between them instead: six
    # bars in a 320px frame drew on a 43px rhythm beside a 32px one elsewhere,
    # and left 63px of empty frame that reads as a chart failing to draw.
    if horizontal:
        height = len(data) * (BAR_THICKNESS + BAR_GAP)
    return (bars + labels).properties(height=height, padding=padding)


def line_chart(
    data: pd.DataFrame,
    x_field: str,
    y_field: str,
    selection: alt.Parameter,
    *,
    x_title: str | None = None,
    y_title: str | None = None,
    value_format: str = ",",
    height: int = 300,
) -> alt.Chart:
    """A selectable chronological chart with an emphasised current point."""
    base = alt.Chart(data).encode(
        x=alt.X(f"{x_field}:O", sort="ascending", title=x_title, axis=alt.Axis(labelAngle=0)),
        y=alt.Y(f"{y_field}:Q", title=y_title, scale=alt.Scale(zero=True)),
    )
    line = base.mark_line(color=ACCENT, strokeWidth=2.2)
    points = (
        base.mark_point(filled=True, color=ACCENT, size=70)
        .encode(
            tooltip=[
                alt.Tooltip(f"{x_field}:O", title=x_title or x_field),
                alt.Tooltip(f"{y_field}:Q", title=y_title or y_field, format=value_format),
            ],
            opacity=alt.condition(
                selection, alt.value(SELECTED_OPACITY), alt.value(UNSELECTED_OPACITY)
            ),
        )
        .add_params(selection)
    )
    # The module prints every bar's value; a line left the reader hovering for
    # the one series that is normalized, which is the one worth reading exactly.
    labels = base.mark_text(
        align="center",
        baseline="bottom",
        dy=-10,
        color=MUTED,
        font=FONT,
        fontSize=VALUE_LABEL_SIZE,
    ).encode(text=alt.Text(f"{y_field}:Q", format=value_format))
    return (line + points + labels).properties(height=height, padding={"top": 18})
