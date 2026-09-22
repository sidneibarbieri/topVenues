import altair as alt
import pandas as pd

from web import charts
from web.theme import LIGHT


def test_bar_chart_accepts_explicit_log_scale() -> None:
    data = pd.DataFrame([{"Class": "Article", "Papers": 1000}, {"Class": "Poster", "Papers": 10}])
    selection = alt.selection_point(fields=["Class"], empty=True)

    chart = charts.bar_chart(
        data,
        "Class",
        "Papers",
        selection,
        LIGHT.chart,
        value_scale=alt.Scale(type="log", domainMin=1),
    )

    specification = chart.to_dict()
    assert specification["layer"][0]["encoding"]["x"]["scale"] == {
        "domainMin": 1,
        "type": "log",
    }
