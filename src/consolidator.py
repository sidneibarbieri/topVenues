"""Merges DBLP JSON files into master dataset."""

import json
import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from .event_normalizer import EventNormalizer
from .models import Paper

logger = logging.getLogger(__name__)


class DataConsolidator:
    def __init__(
        self,
        json_dir: Path,
        data_dir: Path,
    ):
        self.json_dir = Path(json_dir)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.normalizer = EventNormalizer()

    def consolidate(
        self,
        progress_callback: Callable | None = None,
    ) -> list[Paper]:
        """Merge JSON files into deduplicated Paper objects."""
        json_files = list(self.json_dir.glob("*.json"))
        if not json_files:
            return []

        papers: list[Paper] = []
        for idx, json_file in enumerate(json_files, start=1):
            papers.extend(self._process_json_file(json_file))
            if progress_callback:
                progress_callback(idx, len(json_files), json_file.name)

        papers = [p for p in papers if p.paper_type.value != "editorship"]
        return self._deduplicate(papers)

    @staticmethod
    def _deduplicate(papers: list[Paper]) -> list[Paper]:
        seen: set[str] = set()
        unique: list[Paper] = []
        for paper in papers:
            if paper.paper_id in seen:
                continue
            seen.add(paper.paper_id)
            unique.append(paper)
        return unique

    _PAPER_TYPE_MAP: dict[str, str] = {
        "article": "article",
        "conference and workshop papers": "article",
        "inproceedings": "article",
        "proceedings": "proceedings",
        "editorship": "editorship",
    }

    def _process_json_file(self, json_file: Path) -> list[Paper]:
        with open(json_file, encoding="utf-8") as fh:
            data = json.load(fh)

        hits = data.get("result", {}).get("hits", {}).get("hit", [])
        papers: list[Paper] = []

        for hit in hits:
            info = hit.get("info", {})
            paper_id = hit.get("@id")
            title = info.get("title")
            year = info.get("year")

            if not (paper_id and title and year):
                continue

            raw_type = str(info.get("type", "article")).lower()
            paper_type = self._PAPER_TYPE_MAP.get(raw_type, "unknown")

            paper = Paper(
                score=hit.get("@score"),
                paper_id=paper_id,
                authors=self._extract_authors(info.get("authors", {})),
                title=title,
                venue=info.get("venue"),
                pages=info.get("pages"),
                year=year,
                paper_type=paper_type,
                access=info.get("access"),
                key=info.get("key"),
                ee=info.get("ee"),
                url=info.get("url"),
                event=self.normalizer.normalize(info.get("venue", "")),
                abstract=None,
            )
            papers.append(paper)

        return papers

    def _normalize_event(self, venue: str) -> str:
        return self.normalizer.normalize(venue)

    def _extract_authors(self, authors_data: dict) -> str | None:
        if not authors_data:
            return None
        author_list = authors_data.get("author", [])
        if not author_list:
            return None
        if isinstance(author_list, dict):
            return author_list.get("text", "") or None

        names: list[str] = []
        for author in author_list:
            if isinstance(author, dict):
                name = author.get("text", "")
                if isinstance(name, list):
                    names.extend(name)
                else:
                    names.append(name)
        return ", ".join(names) or None

    def save_dataset(
        self,
        papers: list[Paper],
        master_file: Path | None = None,
        csv_file: Path | None = None,
    ) -> tuple[Path, Path]:
        if master_file is None:
            master_file = self.data_dir / "master_dataset.pkl"
        if csv_file is None:
            csv_file = self.data_dir / "master_dataset.csv"

        df = pd.DataFrame([p.model_dump(by_alias=True) for p in papers])
        df.to_pickle(master_file)
        df.to_csv(csv_file, index=False, encoding="utf-8")
        return master_file, csv_file

