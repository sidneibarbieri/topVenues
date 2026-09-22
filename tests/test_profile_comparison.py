from pathlib import Path

from src.database import DatabaseManager
from src.models import Paper
from src.profile_comparison import compare_databases


def _database(path: Path, paper_ids: list[str]) -> Path:
    manager = DatabaseManager(path)
    manager.upsert_papers(
        [
            Paper(
                paper_id=paper_id,
                title=paper_id,
                year=2025,
                event="ACM CCS",
                abstract="abstract" if paper_id != "missing" else None,
                bibtex="@inproceedings{x}" if paper_id != "missing" else None,
            )
            for paper_id in paper_ids
        ]
    )
    return manager.db_path


def test_profile_comparison_reports_exact_set_changes(tmp_path: Path) -> None:
    previous = _database(tmp_path / "previous.db", ["a", "b"])
    successor = _database(tmp_path / "successor.db", ["b", "c", "missing"])
    comparison = compare_databases(previous, successor)
    assert comparison.added_ids == ["c", "missing"]
    assert comparison.removed_ids == ["a"]
    assert comparison.retained == 1
    assert comparison.successor.abstracts == 2
