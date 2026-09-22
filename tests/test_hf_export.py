"""Tests for the Hugging Face dataset export."""

import pandas as pd
import pytest

from src.database import DatabaseManager
from src.hf_export import _EXPORT_COLUMNS, export_hf_dataset
from src.models import Paper


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(tmp_path / "papers.db")
    manager.upsert_papers(
        [
            Paper(
                paper_id="1",
                title="Fuzzing the Kernel",
                year=2024,
                event="ACM CCS",
                abstract="We fuzz kernels.",
                bibtex="@inproceedings{x}",
            ),
            Paper(
                paper_id="2",
                title="Congestion Control",
                year=2023,
                event="ACM SIGCOMM",
                bibtex="@inproceedings{y}",
            ),
            Paper(paper_id="3", title="Deep Nets", year=2022, event="NeurIPS"),
        ]
    )
    return manager


def test_export_writes_parquet_and_card(db, tmp_path):
    out = tmp_path / "hf"
    stats = export_hf_dataset(db.db_path, out)

    assert len(stats["parquet_paths"]) == 1
    assert stats["parquet_paths"][0].name == "train-00000-of-00001.parquet"
    df = pd.read_parquet(stats["parquet_paths"][0])
    assert list(df.columns) == _EXPORT_COLUMNS
    assert len(df) == 3
    assert set(df["area"]) == {"security", "networks", "ai"}

    card = (out / "README.md").read_text(encoding="utf-8")
    assert card.startswith("---\n")
    assert "path: train-*.parquet" in card
    assert "load_dataset" in card
    assert "| ACM CCS | security | 1 | 1 |" in card
    assert "assets/topvenues-abstract-search.png" in card
    assert "assets/topvenues-abstract-search.pdf" in card
    assert (out / "assets" / "topvenues-abstract-search.png").is_file()
    assert (out / "assets" / "topvenues-abstract-search.pdf").is_file()


def test_stats_reflect_two_layer_policy(db, tmp_path):
    stats = export_hf_dataset(db.db_path, tmp_path / "hf")
    assert stats["total"] == 3
    assert stats["security_total"] == 1
    assert stats["security_abstracts"] == 1
    assert stats["n_abstracts"] == 1
    assert stats["security_pct"] == 100.0


def test_repo_id_lands_in_usage_example(db, tmp_path):
    out = tmp_path / "hf"
    export_hf_dataset(db.db_path, out, repo_id="someorg/somecorpus")
    card = (out / "README.md").read_text(encoding="utf-8")
    assert 'load_dataset("someorg/somecorpus", split="train")' in card


def test_profile_export_records_snapshot_identity(db, tmp_path):
    base = tmp_path / "artifact"
    manifest_dir = base / "data" / "profiles" / "security-20"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "manifest.json").write_text(
        '{"snapshot": {"gzip_sha256": "abc", "papers": 3}}', encoding="utf-8"
    )
    out = tmp_path / "hf"
    export_hf_dataset(
        db.db_path,
        out,
        profile_id="security-20",
        release_tag="test-release",
        base_dir=base,
    )
    card = (out / "README.md").read_text(encoding="utf-8")
    assert "**Profile:** `security-20`" in card
    assert "accepted paper's frozen `security-20` profile" in card
    assert "`abc`" in card
    assert "`test-release`" in card


def test_large_corpus_is_sharded(tmp_path):
    from src.database import DatabaseManager
    from src.hf_export import export_hf_dataset
    from src.models import Paper

    manager = DatabaseManager(tmp_path / "papers.db")
    manager.upsert_papers(
        [
            Paper(paper_id=str(index), title=f"Paper {index}", year=2024, event="ACM CCS")
            for index in range(20_001)
        ]
    )

    stats = export_hf_dataset(manager.db_path, tmp_path / "hf")

    names = [path.name for path in stats["parquet_paths"]]
    assert names == ["train-00000-of-00002.parquet", "train-00001-of-00002.parquet"]
    total_rows = sum(len(pd.read_parquet(path)) for path in stats["parquet_paths"])
    assert total_rows == 20_001


def test_export_removes_stale_shards(db, tmp_path):
    out = tmp_path / "hf"
    stale = out / "data" / "train-00000-of-00099.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"old shard")

    export_hf_dataset(db.db_path, out)

    assert not stale.exists()
