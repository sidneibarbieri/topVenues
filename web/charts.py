"""Chart construction for the TopVenues web interface.

Presentation lives here so the page modules stay about data and flow. Every
chart in the application is built through these helpers, which makes the visual
language a single decision rather than a per-call-site one.

The design is deliberately restrained: one data hue per series, vertical rules
only, no borders, and the value printed next to each bar so a reader never has
to hover to learn a number. Colours come from the active theme's ChartPalette
(web/theme.py), so every chart reads the same in light and dark.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

from web.theme import ChartPalette

# A second channel beside colour, so no multi-series chart relies on hue alone.
SERIES_DASH = ((1, 0), (6, 3), (2, 3))
FONT = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"

# Unselected bars must read as clearly dimmed, not merely lighter.
SELECTED_OPACITY = 1.0
UNSELECTED_OPACITY = 0.32

# One bar thickness across the application. Letting height drive thickness made
# a 20-bar chart draw 19px bars beside an 8-bar chart drawing 41px ones, and the
# two read as different design systems when placed side by side.
BAR_THICKNESS = 16
BAR_GAP = 12
BAR_CORNER = 4

LABEL_SIZE = 12
TITLE_SIZE = 12
VALUE_LABEL_SIZE = 11
# Long venue names such as "IEEE Communications Surveys & Tutorials" must print
# whole; a truncated category name is a label the reader has to guess.
CATEGORY_LABEL_LIMIT = 300


def series_legend() -> alt.Legend:
    """Legend entries drawn as the line itself, so dash identifies a series too."""
    return alt.Legend(symbolType="stroke", symbolStrokeWidth=2.3, symbolSize=320)


def apply_theme(
    chart: alt.Chart, palette: ChartPalette, number_locale: dict | None = None
) -> alt.Chart:
    """Apply the shared visual language to a finished chart.

    `configure` replaces the whole config object in Altair 6, so it runs first;
    placed last, it silently discarded every axis, legend and view setting.

    `number_locale` sets the digit separators of ticks and labels, so a chart
    reads 20.305 beside a Portuguese interface that says 20.305.
    """
    locale = {"number": number_locale} if number_locale else alt.Undefined
    return (
        chart.configure(background="transparent", locale=locale)
        .configure_axis(
            domain=False,
            ticks=False,
            grid=False,
            labelColor=palette.muted,
            titleColor=palette.muted,
            labelFont=FONT,
            titleFont=FONT,
            labelFontSize=LABEL_SIZE,
            titleFontSize=TITLE_SIZE,
            titleFontWeight="normal",
            labelPadding=6,
            titlePadding=10,
        )
        .configure_legend(labelColor=palette.muted, titleColor=palette.muted, labelFont=FONT)
        .configure_view(strokeOpacity=0)
        .configure_axisY(grid=False)
        .configure_axisX(grid=True, gridColor=palette.rule, gridDash=[2, 3])
    )


def _bar_fill(palette: ChartPalette, partial_field: str | None) -> alt.Color | alt.Value:
    """One data hue; a partial category, such as an unfinished year, reads lighter."""
    if partial_field is None:
        return alt.value(palette.primary)
    return alt.condition(
        f"datum['{partial_field}']", alt.value(palette.partial), alt.value(palette.primary)
    )


def _category_sort(
    sort: str | list | None, value: str
) -> str | list | alt.EncodingSortField | None:
    """Sort categories by the value field, not by a channel.

    A channel sort such as "-x" breaks a layered chart whose track layer pins x
    to a constant: Vega-Lite finds no field there to sort by and draws nothing.
    """
    channel_orders = {"-x": "descending", "-y": "descending", "x": "ascending", "y": "ascending"}
    if isinstance(sort, str) and sort in channel_orders:
        return alt.EncodingSortField(field=value, order=channel_orders[sort])
    return sort


def bar_chart(
    data: pd.DataFrame,
    category: str,
    value: str,
    selection: alt.Parameter,
    palette: ChartPalette,
    *,
    horizontal: bool = True,
    sort: str | list | None = "-x",
    category_title: str | None = None,
    value_title: str | None = None,
    value_format: str = ",",
    height: int = 320,
    value_scale: alt.Scale | None = None,
    label_field: str | None = None,
    whole: float | None = None,
    partial_field: str | None = None,
) -> alt.Chart:
    """A selectable bar chart with the value printed beside every bar.

    `label_field` names a column to print instead of the raw value, for when a
    bar is worth more than one number, such as a count beside its share.

    `whole` draws a recessive track from zero to that value behind every bar,
    for part-of-a-whole measures such as coverage: the empty stretch of track
    is the gap, visible without reading a number.

    `partial_field` names a boolean column that marks a category as incomplete,
    such as the current publication year; those bars take the lighter step.

    `height` applies to vertical charts. A horizontal chart derives its height
    from how many bars it has, so bars keep one rhythm across the application.

    Both marks share one base encoding. Encoding the layers separately makes
    Altair resolve two axes for the same channel, and the second one paints
    over the category names.

    A bar encodes magnitude as length from zero, so `value_scale` must stay
    linear. A logarithmic scale has no zero to start from, and Vega draws every
    bar zero pixels wide: the labels appear, the bars do not.
    """
    sort = _category_sort(sort, value)
    category_axis = alt.Axis(labelAngle=0, labelLimit=CATEGORY_LABEL_LIMIT)
    value_axis = alt.Axis(tickCount=4)
    scale = value_scale or (alt.Scale(domain=[0, whole]) if whole else alt.Undefined)
    if horizontal:
        base = alt.Chart(data).encode(
            y=alt.Y(f"{category}:N", sort=sort, title=category_title, axis=category_axis),
            x=alt.X(f"{value}:Q", title=value_title, axis=value_axis, scale=scale),
        )
    else:
        base = alt.Chart(data).encode(
            x=alt.X(f"{category}:O", sort=sort, title=category_title, axis=category_axis),
            y=alt.Y(f"{value}:Q", title=value_title, axis=value_axis, scale=scale),
        )

    bars = (
        base.mark_bar(cornerRadiusEnd=BAR_CORNER, size=BAR_THICKNESS)
        .encode(
            color=_bar_fill(palette, partial_field),
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
        color=palette.muted,
        font=FONT,
        fontSize=VALUE_LABEL_SIZE,
    ).encode(
        text=alt.Text(f"{label_field}:N")
        if label_field
        else alt.Text(f"{value}:Q", format=value_format)
    )

    if whole:
        # With a track, values line up in one column past its end.
        labels = labels.encode(
            **({"x": alt.datum(whole)} if horizontal else {"y": alt.datum(whole)})
        )

    layers = [bars, labels]
    if whole:
        track_value = alt.datum(whole)
        track_encoding = {"x": track_value} if horizontal else {"y": track_value}
        track = base.mark_bar(
            cornerRadiusEnd=BAR_CORNER, size=BAR_THICKNESS, color=palette.track
        ).encode(**track_encoding)
        layers.insert(0, track)

    # The value label sits outside the longest bar, so the plot needs room on
    # that side or the largest number is clipped at the frame.
    # A vertical chart's label is centred on its bar, so a wide one, such as a
    # count marked "partial", needs room past the last bar as well.
    if horizontal:
        padding = {"right": 96 if label_field else 44}
    else:
        padding = {"top": 18, "right": 40 if label_field else 0}
    # A horizontal chart's height is the bar count, not the caller's number. A
    # floor taller than the bars stretches the gaps between them instead: six
    # bars in a 320px frame drew on a 43px rhythm beside a 32px one elsewhere,
    # and left 63px of empty frame that reads as a chart failing to draw.
    if horizontal:
        height = len(data) * (BAR_THICKNESS + BAR_GAP)
    return alt.layer(*layers).properties(height=height, padding=padding)


def line_chart(
    data: pd.DataFrame,
    x_field: str,
    y_field: str,
    selection: alt.Parameter,
    palette: ChartPalette,
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
    line = base.mark_line(color=palette.primary, strokeWidth=2)
    points = (
        base.mark_point(filled=True, color=palette.primary, size=64)
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
        color=palette.muted,
        font=FONT,
        fontSize=VALUE_LABEL_SIZE,
    ).encode(text=alt.Text(f"{y_field}:Q", format=value_format))
    return (line + points + labels).properties(height=height, padding={"top": 18})
