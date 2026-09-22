"""The light and dark themes stay readable and agree with Streamlit's theme.

Both themes are first-class: each one's text and chart colours are checked
against that theme's own background, and the Streamlit configuration must name
the same colours as web/theme.py, which styles everything Streamlit does not.
"""

import re
import tomllib
from pathlib import Path

import pytest

from web.theme import DARK, LIGHT, Theme

ROOT = Path(__file__).resolve().parents[1]
THEMES = (LIGHT, DARK)


def _luminance(hex_colour: str) -> float:
    channels = [int(hex_colour[index : index + 2], 16) / 255 for index in (1, 3, 5)]
    linear = [
        value / 12.92 if value <= 0.04045 else ((value + 0.055) / 1.055) ** 2.4
        for value in channels
    ]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def _contrast(foreground: str, background: str) -> float:
    lighter, darker = sorted((_luminance(foreground), _luminance(background)), reverse=True)
    return (lighter + 0.05) / (darker + 0.05)


@pytest.mark.parametrize("theme", THEMES, ids=lambda theme: theme.name)
def test_text_meets_wcag_aa_on_its_own_background(theme: Theme):
    background = theme.interface.background
    for role in ("ink", "slate", "accent"):
        colour = getattr(theme.interface, role)
        assert _contrast(colour, background) >= 4.5, f"{theme.name} {role} {colour}"


@pytest.mark.parametrize("theme", THEMES, ids=lambda theme: theme.name)
def test_every_series_reads_against_its_own_background(theme: Theme):
    for colour in theme.chart.series:
        assert _contrast(colour, theme.interface.background) >= 3.0, f"{theme.name} {colour}"


def test_both_themes_carry_the_same_number_of_series():
    assert len(LIGHT.chart.series) == len(DARK.chart.series)


@pytest.mark.parametrize("theme", THEMES, ids=lambda theme: theme.name)
def test_streamlit_names_the_same_colours(theme: Theme):
    config = tomllib.loads((ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8"))
    section = config["theme"][theme.name]
    assert section["primaryColor"] == theme.interface.accent
    assert section["backgroundColor"] == theme.interface.background
    assert section["secondaryBackgroundColor"] == theme.interface.surface
    assert section["textColor"] == theme.interface.ink
    assert section["borderColor"] == theme.interface.border


@pytest.mark.parametrize("theme", THEMES, ids=lambda theme: theme.name)
def test_each_theme_ships_its_brand_files(theme: Theme):
    brand = ROOT / "docs" / "brand"
    assert (brand / theme.wordmark).is_file()
    assert (brand / theme.mark).is_file()


def test_the_stylesheet_uses_only_declared_custom_properties():
    stylesheet = (ROOT / "web" / "styles.css").read_text(encoding="utf-8")
    used = set(re.findall(r"var\((--[a-z-]+)\)", stylesheet))
    assert used <= set(LIGHT.interface.custom_properties())
