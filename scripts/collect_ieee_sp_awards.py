#!/usr/bin/env python3
"""Collect IEEE S&P paper awards from official, year-specific pages."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx
from bs4 import BeautifulSoup, Tag

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_JSON = REPO_ROOT / "data" / "awards" / "ieee_sp_2019_2024_paper_awards.json"
OUTPUT_TSV = REPO_ROOT / "data" / "awards" / "ieee_sp_2019_2024_paper_awards.tsv"
SOURCE_URLS = {
    2019: "https://www.ieee-security.org/TC/SP2019/awards.html",
    2020: "https://www.ieee-security.org/TC/SP2020/awards.html",
    2021: "https://www.ieee-security.org/TC/SP2021/awards.html",
    2022: "https://www.ieee-security.org/TC/SP2022/awards.html",
    2023: "https://www.ieee-security.org/TC/SP2023/program-awards.html",
    2024: "https://www.ieee-security.org/TC/SP2024/awards.html",
}
AWARD_HEADINGS = {
    "best paper award": "Best Paper Award",
    "best practical paper award": "Best Practical Paper Award",
    "best student paper award": "Best Student Paper Award",
    "distinguished paper award": "Distinguished Paper Award",
    "distinguished paper awards": "Distinguished Paper Award",
    "distinguished practical paper award": "Distinguished Practical Paper Award",
}
FIELDS = ("venue", "year", "award", "title", "url", "source_url")


@dataclass(frozen=True)
class CollectedAward:
    venue: str
    year: int
    award: str
    title: str
    url: str
    source_url: str


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _heading_level(heading: Tag) -> int:
    return int(heading.name[1])


def _award_titles(heading: Tag) -> list[str]:
    """Read bold paper titles until the next heading at the same level or above."""
    titles: list[str] = []
    heading_level = _heading_level(heading)
    for element in heading.find_all_next():
        if element is heading:
            continue
        if element.name in {"h1", "h2", "h3"} and _heading_level(element) <= heading_level:
            break
        if element.name not in {"p", "div"}:
            continue
        if element.name == "div" and "list-group-item" not in element.get("class", []):
            continue
        title_element = element.find(["strong", "b"], recursive=True)
        if title_element is None:
            continue
        title = _clean(title_element.get_text(" ", strip=True))
        if title and title not in titles:
            titles.append(title)
    return titles


def collect_page(year: int, source_url: str, html: str) -> list[CollectedAward]:
    """Extract the declared paper-award sections from one official page."""
    soup = BeautifulSoup(html, "html.parser")
    records: list[CollectedAward] = []
    for heading in soup.find_all(["h1", "h2", "h3"]):
        heading_key = _clean(heading.get_text(" ", strip=True)).casefold()
        award = AWARD_HEADINGS.get(heading_key)
        if award is None:
            continue
        for title in _award_titles(heading):
            records.append(
                CollectedAward(
                    venue="IEEE S&P",
                    year=year,
                    award=award,
                    title=title,
                    url=source_url,
                    source_url=source_url,
                )
            )
    if not records:
        raise RuntimeError(f"No IEEE S&P paper awards found for {year}: {source_url}")
    return records


def collect_all(client: httpx.Client) -> list[CollectedAward]:
    """Fetch every configured official page and return a stable record list."""
    records: list[CollectedAward] = []
    for year, source_url in SOURCE_URLS.items():
        response = client.get(source_url)
        response.raise_for_status()
        records.extend(collect_page(year, source_url, response.text))
    return sorted(records, key=lambda record: (record.year, record.award, record.title))


def write_records(records: list[CollectedAward]) -> None:
    OUTPUT_JSON.write_text(
        json.dumps([asdict(record) for record in records], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with OUTPUT_TSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(asdict(record) for record in records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()
    with httpx.Client(follow_redirects=True, timeout=args.timeout) as client:
        records = collect_all(client)
    write_records(records)
    print(f"Wrote {len(records)} IEEE S&P paper-award records to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
