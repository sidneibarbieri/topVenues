"""Recreate the documentation screenshots from a running interface.

Start the interface first (`python -m src.cli --profile security-20-v4 web`),
then run this script. It drives the same scenes the README shows: the overview,
a ranked search for "LLM" in the Security top-4 scope, that topic's trend, one
author's trajectory in Researcher Radar, and the evidence page.

Requires Playwright with Chrome: `pip install playwright`.
"""

import argparse
from pathlib import Path

from playwright.sync_api import Locator, Page, sync_playwright

DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "docs" / "assets" / "screenshots"
VIEWPORT = {"width": 1280, "height": 720}
TALL_VIEWPORT = {"width": 1280, "height": 2600}
TOPIC = "LLM"
SCOPE = "Security top-4"
PREFERRED_AUTHOR = "Yang Zhang 0016"
SETTLE_MS = 3500


def open_page(page: Page, name: str) -> None:
    page.locator('section[data-testid="stSidebar"]').get_by_text(name, exact=True).first.click()
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


def capture_overview(page: Page, output: Path) -> None:
    page.screenshot(path=output / "overview.png")


def capture_search(page: Page, output: Path) -> None:
    open_page(page, "Search")
    type_and_submit(page, page.get_by_label("Ranked search (BM25)", exact=True), TOPIC)
    choose(page, select_box(page, "Venue tier scope"), SCOPE)
    select_box(page, "Venue tier scope").scroll_into_view_if_needed()
    page.mouse.move(640, 10)
    page.wait_for_timeout(1500)
    page.screenshot(path=output / "search-top4-llm.png")


def capture_topic_trend(page: Page, output: Path) -> None:
    open_page(page, "Insights")
    heading = page.get_by_role("heading", name="Topic trend")
    heading.scroll_into_view_if_needed()
    type_and_submit(page, page.get_by_label("Topic", exact=True).first, TOPIC)
    choose(page, select_box(page, "Venue tier scope"), SCOPE)
    volume_title = page.get_by_text("Papers per year", exact=False).first
    share_title = page.get_by_text("Share of the year", exact=False).first
    volume_title.evaluate("element => element.scrollIntoView({block: 'center'})")
    page.wait_for_timeout(2500)
    charts = elements_below(
        page.locator('[data-testid="stVegaLiteChart"]'), heading.bounding_box()["y"]
    )[:2]
    clip = bounding_union([volume_title, share_title, *charts])
    page.screenshot(path=output / "insights-llm-top4.png", clip=clip)


def capture_researcher_radar(page: Page, output: Path) -> None:
    page.get_by_role("heading", name="Researcher Radar").scroll_into_view_if_needed()
    type_and_submit(page, page.get_by_label("Topic (title/abstract contains)", exact=True), TOPIC)
    choose(page, select_box(page, "Venue tier scope", 1), SCOPE)
    author_box = select_box(page, "Inspect an author's corpus records")
    author_box.scroll_into_view_if_needed()
    author_box.click()
    page.wait_for_timeout(800)
    options = page.get_by_role("option")
    names = [options.nth(index).inner_text() for index in range(min(options.count(), 12))]
    author = next((name for name in names if name.startswith(PREFERRED_AUTHOR)), names[0])
    page.get_by_role("option", name=author, exact=True).first.click()
    page.wait_for_timeout(SETTLE_MS)
    heading = page.get_by_text("Publication trajectory", exact=False).first
    heading.evaluate("element => element.scrollIntoView({block: 'center'})")
    page.wait_for_timeout(4000)
    anchor = heading.bounding_box()["y"]
    trajectory = elements_below(
        page.locator('[data-testid="stVegaLiteChart"]'), anchor - 1, within=120
    )[:1]
    collaborators = elements_below(
        page.locator('[data-testid="stDataFrame"]'), anchor - 1, within=120
    )[:1]
    evidence = page.get_by_text("Trajectory evidence", exact=True).first
    clip = bounding_union([heading, *trajectory, *collaborators, evidence])
    page.screenshot(path=output / "researcher-radar-llm-top4.png", clip=clip)


def capture_evidence(page: Page, output: Path) -> None:
    open_page(page, "Evidence")
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
    arguments = parser.parse_args()
    arguments.output.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel="chrome")
        page = browser.new_page(
            viewport=VIEWPORT, device_scale_factor=1, color_scheme=arguments.scheme
        )
        page.goto(arguments.url, wait_until="networkidle")
        page.wait_for_selector(".app-header", timeout=180_000)
        page.wait_for_timeout(2500)
        capture_overview(page, arguments.output)
        capture_search(page, arguments.output)
        page.set_viewport_size(TALL_VIEWPORT)
        capture_topic_trend(page, arguments.output)
        capture_researcher_radar(page, arguments.output)
        page.set_viewport_size(VIEWPORT)
        capture_evidence(page, arguments.output)
        browser.close()
    print(f"screenshots written to {arguments.output}")


if __name__ == "__main__":
    main()
