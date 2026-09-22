"""Light and dark design tokens for the interface and its charts.

Every colour the interface draws comes from here (docs/brand/BRAND.md). The
chart series were validated for colour-vision separation and contrast against
each theme's surface, so a chart reads the same in either theme.
"""

from typing import Annotated, Literal

import streamlit as st
from pydantic import BaseModel, ConfigDict, StringConstraints

HexColor = Annotated[str, StringConstraints(pattern=r"^#[0-9A-F]{6}$")]


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class InterfaceTokens(Frozen):
    """The CSS custom properties of web/styles.css."""

    ink: HexColor
    accent: HexColor
    slate: HexColor
    mist: HexColor
    surface: HexColor
    border: HexColor
    background: HexColor
    card: HexColor
    header_scrim: str
    tag_background: HexColor
    tag_text: HexColor
    tag_emphasis_background: HexColor
    tag_emphasis_text: HexColor

    def custom_properties(self) -> dict[str, str]:
        return {
            "--ink": self.ink,
            "--accent": self.accent,
            "--slate": self.slate,
            "--mist": self.mist,
            "--surface": self.surface,
            "--border": self.border,
            "--bg": self.background,
            "--card": self.card,
            "--header-scrim": self.header_scrim,
            "--tag-bg": self.tag_background,
            "--tag-text": self.tag_text,
            "--tag-emphasis-bg": self.tag_emphasis_background,
            "--tag-emphasis-text": self.tag_emphasis_text,
        }

    def as_css(self) -> str:
        body = " ".join(f"{name}: {value};" for name, value in self.custom_properties().items())
        return f":root {{ {body} }}"


class ChartPalette(Frozen):
    """Chart colours: series identity, text, and the recessive grid and track."""

    series: tuple[HexColor, HexColor, HexColor]
    partial: HexColor
    track: HexColor
    ink: HexColor
    muted: HexColor
    rule: HexColor

    @property
    def primary(self) -> HexColor:
        return self.series[0]


class Theme(Frozen):
    name: Literal["light", "dark"]
    interface: InterfaceTokens
    chart: ChartPalette
    wordmark: str
    mark: str


LIGHT = Theme(
    name="light",
    interface=InterfaceTokens(
        ink="#10233F",
        accent="#2867B2",
        slate="#667085",
        mist="#E9EEF4",
        surface="#F5F7FA",
        border="#D3DBE5",
        background="#FFFFFF",
        card="#FFFFFF",
        header_scrim="rgba(255, 255, 255, .88)",
        tag_background="#E9EEF4",
        tag_text="#1F2937",
        tag_emphasis_background="#DCE8F6",
        tag_emphasis_text="#1C4F8F",
    ),
    chart=ChartPalette(
        series=("#2867B2", "#EB6834", "#199E70"),
        partial="#89ABD5",
        track="#E9EEF4",
        ink="#10233F",
        muted="#667085",
        rule="#E9EEF4",
    ),
    wordmark="topvenues-wordmark.svg",
    mark="topvenues-mark.svg",
)

DARK = Theme(
    name="dark",
    interface=InterfaceTokens(
        ink="#E8EDF4",
        accent="#4DA3FF",
        slate="#9AA7B8",
        mist="#1A2B44",
        surface="#122238",
        border="#243A57",
        background="#0B1627",
        card="#102036",
        header_scrim="rgba(11, 22, 39, .88)",
        tag_background="#1A2B44",
        tag_text="#C9D6E6",
        tag_emphasis_background="#16345C",
        tag_emphasis_text="#8CC4FF",
    ),
    chart=ChartPalette(
        series=("#4D8FE0", "#D95926", "#199E70"),
        partial="#2C5283",
        track="#1A2B44",
        ink="#E8EDF4",
        muted="#9AA7B8",
        rule="#1F3350",
    ),
    wordmark="topvenues-wordmark-reversed.svg",
    mark="topvenues-mark-reversed.svg",
)


# Picking Light or Dark in the menu repaints Streamlit's widgets without a rerun,
# so st.context.theme stays stale and our colours would lag behind. The probe
# reads the page's own background in the browser and reports every change,
# which reruns the script with the theme the reader actually sees.
THEME_PROBE_JS = """
export default function (component) {
  const { setStateValue } = component;
  let reported = null;
  const report = () => {
    const channels = getComputedStyle(document.body).backgroundColor.match(/\\d+/g).map(Number);
    const luminance = (0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2]) / 255;
    const mode = luminance < 0.5 ? "dark" : "light";
    if (mode !== reported) {
      reported = mode;
      setStateValue("mode", mode);
    }
  };
  report();
  const timer = setInterval(report, 400);
  return () => clearInterval(timer);
}
"""
THEME_PROBE = st.components.v2.component("topvenues_theme_probe", js=THEME_PROBE_JS)
THEME_PROBE_KEY = "theme-probe"


def active_theme() -> Theme:
    """The theme the reader's browser is showing, as the probe reports it."""
    reported = THEME_PROBE(key=THEME_PROBE_KEY, on_mode_change=lambda: None)
    mode = getattr(reported, "mode", None) or st.context.theme.type
    return DARK if mode == "dark" else LIGHT
