from pathlib import Path

from src.database import DatabaseManager
from src.models import Paper
from src.research_intelligence import (
    ResearchWatchlist,
    arxiv_author_search_url,
    collaboration_network,
    emerging_researchers,
    researcher_trajectory,
    unseen_watchlist_matches,
    watchlist_matching_ids,
)


def _database(tmp_path: Path) -> Path:
    manager = DatabaseManager(tmp_path / "papers.db")
    manager.upsert_papers(
        [
            Paper(paper_id="1", title="Old", year=2019, event="ACM CCS", authors="Alice, Bob"),
            Paper(paper_id="2", title="New", year=2024, event="NDSS", authors="Alice, Carol"),
            Paper(paper_id="3", title="Newer", year=2025, event="NDSS", authors="Dan, Alice"),
            Paper(
                paper_id="4", title="Newest", year=2026, event="IEEE S&P", authors="Alice, Carol"
            ),
        ]
    )
    return manager.db_path


def test_emerging_signal_is_transparent_annual_rate_change(tmp_path: Path) -> None:
    result = emerging_researchers(_database(tmp_path), minimum_recent_papers=2)
    alice = next(item for item in result if item.author == "Alice")
    assert alice.recent_papers == 3
    assert alice.prior_papers == 1
    assert alice.recent_rate == 1.0
    assert alice.prior_rate == 0.2
    assert alice.rate_change == 0.8


def test_trajectory_and_collaboration_keep_supporting_counts(tmp_path: Path) -> None:
    db_path = _database(tmp_path)
    trajectory = researcher_trajectory(db_path, "Alice")
    assert [point.year for point in trajectory] == [2019, 2024, 2025, 2026]
    assert trajectory[-1].first_author_papers == 1
    collaborators = collaboration_network(db_path, "Alice")
    assert collaborators[0].collaborator == "Carol"
    assert collaborators[0].joint_papers == 2


def test_watchlist_is_portable_and_arxiv_link_is_encoded() -> None:
    watchlist = ResearchWatchlist(profile_id="security-20-v3", name="Fuzzing", authors=["Alice"])
    assert '"profile_id":"security-20-v3"' in watchlist.model_dump_json()
    url = arxiv_author_search_url("Alice Smith")
    assert url.startswith("https://arxiv.org/search/?")
    assert "Alice+Smith" in url
    assert "0002" not in arxiv_author_search_url("Qi Li 0002")


def test_watchlist_baseline_supports_future_delta(tmp_path: Path) -> None:
    db_path = _database(tmp_path)
    watchlist = ResearchWatchlist(
        profile_id="old", name="Alice", authors=["Alice"], known_paper_ids=["1", "2"]
    )
    assert watchlist_matching_ids(db_path, watchlist) == ["1", "2", "3", "4"]
    assert unseen_watchlist_matches(db_path, watchlist) == ["3", "4"]
