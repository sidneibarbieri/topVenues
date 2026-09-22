"""Recreate the documentation screenshots from a running interface.

Start the interface first (`python -m streamlit run web/app.py`), then run
this script. It drives the same scenes the README shows: the overview,
a ranked search for "LLM" in the Security top-4 scope, that topic's trend, one
author's trajectory in Researcher Radar, and the evidence page.

`--language pt` captures the same scenes in Portuguese, into a pt-BR folder, by
looking the labels up in the interface's own catalog.

Requires Playwright with Chrome: `pip install playwright`.
"""

import argparse
import json
from pathlib import Path

from playwright.sync_api import Locator, Page, sync_playwright

REPOSITORY = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPOSITORY / "docs" / "assets" / "screenshots"
LOCALES = REPOSITORY / "web" / "locales"
OUTPUT_FOLDERS = {"en": "", "pt": "pt-BR"}
VIEWPORT = {"width": 1280, "height": 720}
TALL_VIEWPORT = {"width": 1280, "height": 2600}
TOPIC = "LLM"
SCOPE = "Security top-4"
PREFERRED_AUTHOR = "Yang Zhang 0016"
SETTLE_MS = 3500


class Labels:
    """The interface's labels in the language being captured.

    The interface opens in the browser's language, so the scenes pin one and
    click its labels, rather than inherit the machine's locale.
    """

    def __init__(self, language: str) -> None:
        self.language = language
        catalog = LOCALES / f"{language}.json"
        self.catalog = json.loads(catalog.read_text(encoding="utf-8")) if catalog.exists() else {}

    def __call__(self, text: str) -> str:
        return self.catalog.get(text, text)

    def prefix(self, text: str) -> str:
        """The fixed start of a label that carries a field, such as a count.

        The dash stays: "Artigos por ano" alone also names a section heading.
        """
        return self(text).split("{")[0].rstrip()


def open_page(page: Page, label: Labels, name: str) -> None:
    sidebar = page.locator('section[data-testid="stSidebar"]')
    sidebar.get_by_text(label(name), exact=True).first.click()
    page.wait_for_timeout(SETTLE_MS)


def select_box(page: Page, label: str, position: int = 0) -> Locator:
    boxes = page.locator('[data-testid="stSelectbox"]').filter(has_text=label)
    return boxes.nth(position).locator("[role=combobox]")


def choose(page: Page, box: Locator, option: str) -> None:
    box.click()
    page.get_by_role("option", name=option, exact=True).first.click()
    page.wait_for_timeout(SETTLE_MS)


def type_and_submit(page: Page, field: Locator, text: str) -> None:
    field.click()
    field.fill(text)
    field.press("Enter")
    page.wait_for_timeout(SETTLE_MS)


def bounding_union(locators: list[Locator], padding: int = 4) -> dict[str, float]:
    boxes = [locator.bounding_box() for locator in locators]
    left = min(box["x"] for box in boxes) - padding
    top = min(box["y"] for box in boxes) - padding
    right = max(box["x"] + box["width"] for box in boxes) + padding
    bottom = max(box["y"] + box["height"] for box in boxes) + padding
    return {"x": left, "y": top, "width": right - left, "height": bottom - top}


def elements_below(
    collection: Locator, anchor_y: float, within: float | None = None
) -> list[Locator]:
    found = []
    for index in range(collection.count()):
        element = collection.nth(index)
        box = element.bounding_box()
        if box is None or box["y"] <= anchor_y:
            continue
        if within is None or box["y"] - anchor_y < within:
            found.append(element)
    return found


def capture_overview(page: Page, label: Labels, output: Path) -> None:
    page.screenshot(path=output / "overview.png")


def capture_search(page: Page, label: Labels, output: Path) -> None:
    open_page(page, label, "Search")
    type_and_submit(page, page.get_by_label(label("Ranked search (BM25)"), exact=True), TOPIC)
    choose(page, select_box(page, label("Venue tier scope")), label(SCOPE))
    select_box(page, label("Venue tier scope")).scroll_into_view_if_needed()
    page.mouse.move(640, 10)
    page.wait_for_timeout(1500)
    page.screenshot(path=output / "search-top4-llm.png")


def capture_topic_trend(page: Page, label: Labels, output: Path) -> None:
    open_page(page, label, "Insights")
    heading = page.get_by_role("heading", name=label("Topic trend"))
    heading.scroll_into_view_if_needed()
    type_and_submit(page, page.get_by_label(label("Topic"), exact=True).first, TOPIC)
    choose(page, select_box(page, label("Venue tier scope")), label(SCOPE))
    volume_title = page.get_by_text(label.prefix("Papers per year — {total} total")).first
    share_title = page.get_by_text(label("Share of the year's corpus (%)"), exact=True).first
    volume_title.evaluate("element => element.scrollIntoView({block: 'center'})")
    page.wait_for_timeout(2500)
    charts = elements_below(
        page.locator('[data-testid="stVegaLiteChart"]'), heading.bounding_box()["y"]
    )[:2]
    clip = bounding_union([volume_title, share_title, *charts])
    page.screenshot(path=output / "insights-llm-top4.png", clip=clip)


def capture_researcher_radar(page: Page, label: Labels, output: Path) -> None:
    page.get_by_role("heading", name=label("Researcher Radar")).scroll_into_view_if_needed()
    topic_field = page.get_by_label(label("Topic (title/abstract contains)"), exact=True)
    type_and_submit(page, topic_field, TOPIC)
    choose(page, select_box(page, label("Venue tier scope"), 1), label(SCOPE))
    author_box = select_box(page, label("Inspect an author's corpus records"))
    author_box.scroll_into_view_if_needed()
    author_box.click()
    page.wait_for_timeout(800)
    options = page.get_by_role("option")
    names = [options.nth(index).inner_text() for index in range(min(options.count(), 12))]
    author = next((name for name in names if name.startswith(PREFERRED_AUTHOR)), names[0])
    page.get_by_role("option", name=author, exact=True).first.click()
    page.wait_for_timeout(SETTLE_MS)
    heading = page.get_by_text(label.prefix("Publication trajectory — {author}")).first
    heading.evaluate("element => element.scrollIntoView({block: 'center'})")
    page.wait_for_timeout(4000)
    anchor = heading.bounding_box()["y"]
    trajectory = elements_below(
        page.locator('[data-testid="stVegaLiteChart"]'), anchor - 1, within=120
    )[:1]
    collaborators = elements_below(
        page.locator('[data-testid="stDataFrame"]'), anchor - 1, within=120
    )[:1]
    evidence = page.get_by_text(label("Trajectory evidence"), exact=True).first
    clip = bounding_union([heading, *trajectory, *collaborators, evidence])
    page.screenshot(path=output / "researcher-radar-llm-top4.png", clip=clip)


def capture_evidence(page: Page, label: Labels, output: Path) -> None:
    open_page(page, label, "Evidence")
    page.evaluate(
        "() => document.querySelectorAll('*').forEach(element => { element.scrollTop = 0; })"
    )
    page.mouse.move(640, 10)
    page.wait_for_timeout(1500)
    page.screenshot(path=output / "evidence.png")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8501/")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--scheme", choices=("light", "dark"), default="light")
    parser.add_argument("--language", choices=tuple(OUTPUT_FOLDERS), default="en")
    arguments = parser.parse_args()
    label = Labels(arguments.language)
    output = arguments.output / OUTPUT_FOLDERS[arguments.language]
    output.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel="chrome")
        page = browser.new_page(
            viewport=VIEWPORT, device_scale_factor=1, color_scheme=arguments.scheme
        )
        page.goto(f"{arguments.url}?lang={arguments.language}", wait_until="networkidle")
        page.wait_for_selector(".app-header", timeout=180_000)
        page.wait_for_timeout(2500)
        capture_overview(page, label, output)
        capture_search(page, label, output)
        page.set_viewport_size(TALL_VIEWPORT)
        capture_topic_trend(page, label, output)
        capture_researcher_radar(page, label, output)
        page.set_viewport_size(VIEWPORT)
        capture_evidence(page, label, output)
        browser.close()
    print(f"screenshots written to {output}")


if __name__ == "__main__":
    main()
