"""Tests for CLI export behavior."""

import pytest
from click.testing import CliRunner

from src.cli import cli
from src.database import DatabaseManager
from src.models import Paper


@pytest.fixture(autouse=True)
def _isolate_profile_environment(monkeypatch):
    """Keep an outer TOPVENUES_PROFILE from changing temporary CLI fixtures."""
    monkeypatch.delenv("TOPVENUES_PROFILE", raising=False)


def _configure(base_dir, data_dir="data/dataset"):
    (base_dir / "config.yaml").write_text(
        "\n".join(
            (
                "immutable_snapshot: false",
                f"data_dir: {data_dir}",
                "events: [ccs]",
                "years: [2024, 2025]",
            )
        ),
        encoding="utf-8",
    )


def _seed(base_dir):
    _configure(base_dir)
    db = DatabaseManager(base_dir / "data" / "dataset" / "papers.db")
    papers = [
        Paper(
            paper_id="1",
            title="Intrusion Detection with Logs",
            authors="Alice",
            year=2024,
            event="ACM CCS",
            abstract="This paper studies intrusion detection using security logs.",
            bibtex="@inproceedings{demo2024, title={Intrusion Detection with Logs}, year={2024}}",
        ),
        Paper(
            paper_id="2",
            title="Cryptographic Protocols",
            authors="Bob",
            year=2024,
            event="IEEE S&P",
            abstract="This paper studies cryptographic protocols.",
            bibtex="@inproceedings{crypto2024, title={Cryptographic Protocols}, year={2024}}",
        ),
        Paper(
            paper_id="3",
            title="Intrusion Detection in Enterprise Networks",
            authors="Carol",
            year=2025,
            event="IEEE CNS",
            abstract="This paper studies intrusion detection in a strong venue.",
            bibtex="@inproceedings{cns2025, title={Intrusion Detection}, year={2025}}",
        ),
    ]
    db.upsert_papers(papers)
    for paper in papers:
        db.update_bibtex(paper.paper_id, paper.bibtex or "")


def test_export_bibtex_filtered_to_stdout(tmp_path):
    _seed(tmp_path)
    result = CliRunner().invoke(
        cli,
        ["--base-dir", str(tmp_path), "export", "--format", "bibtex", "-T", "intrusion"],
    )

    assert result.exit_code == 0
    assert "demo2024" in result.output
    assert "crypto2024" not in result.output


def test_export_json_filtered_to_file(tmp_path):
    _seed(tmp_path)
    output = tmp_path / "out" / "papers.json"
    result = CliRunner().invoke(
        cli,
        [
            "--base-dir",
            str(tmp_path),
            "export",
            "--format",
            "json",
            "-e",
            "IEEE S&P",
            "-o",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert output.exists()
    text = output.read_text(encoding="utf-8")
    assert "Cryptographic Protocols" in text
    assert "Intrusion Detection" not in text


def test_export_can_restrict_to_security_big_four(tmp_path):
    _seed(tmp_path)
    result = CliRunner().invoke(
        cli,
        [
            "--base-dir",
            str(tmp_path),
            "export",
            "--format",
            "json",
            "--tech",
            "intrusion",
            "--tier-scope",
            "Security top-4",
        ],
    )

    assert result.exit_code == 0
    assert "Intrusion Detection with Logs" in result.output
    assert "Enterprise Networks" not in result.output


def test_backfill_abstracts_cli_invokes_collector(tmp_path, monkeypatch):
    _configure(tmp_path)
    called = {}

    async def fake_backfill(self, event=None, limit=None, concurrency=4):
        called["event"] = event
        called["limit"] = limit
        called["concurrency"] = concurrency
        return {"scanned": 3, "updated": 2, "missing": 1, "no_doi": 0}

    monkeypatch.setattr("src.collector.Collector.run_backfill_abstracts", fake_backfill)

    result = CliRunner().invoke(
        cli,
        [
            "--base-dir",
            str(tmp_path),
            "backfill-abstracts",
            "--event",
            "ACSAC",
            "--limit",
            "3",
            "--concurrency",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert called == {"event": "ACSAC", "limit": 3, "concurrency": 2}
    assert "Updated" in result.output


def test_download_cli_can_scope_events(tmp_path, monkeypatch):
    _configure(tmp_path)
    called = {}

    async def fake_download(self):
        called["events"] = list(self.config.events)
        called["years"] = list(self.config.years)

    monkeypatch.setattr("src.collector.Collector.run_download", fake_download)

    result = CliRunner().invoke(
        cli,
        [
            "--base-dir",
            str(tmp_path),
            "download",
            "--event",
            "acsac",
            "--event",
            "satml",
            "--year",
            "2025",
        ],
    )

    assert result.exit_code == 0
    assert called == {"events": ["acsac", "satml"], "years": [2025]}


def test_materialization_status_reports_json_years(tmp_path, monkeypatch):
    _configure(tmp_path)
    json_dir = tmp_path / "data" / "json"
    json_dir.mkdir(parents=True)
    (json_dir / "data_ccs2024.json").write_text("{}", encoding="utf-8")

    class FakeConfig:
        events = ["ccs"]

        def effective_years(self):
            return [2024, 2025]

    class FakeCollector:
        def __init__(self, base_dir):
            self.config = FakeConfig()
            self.json_dir = json_dir

    monkeypatch.setattr("src.cli.Collector", FakeCollector)

    result = CliRunner().invoke(
        cli,
        ["--base-dir", str(tmp_path), "materialization-status"],
    )

    assert result.exit_code == 0
    assert "ccs" in result.output
    assert "2024" in result.output
    assert "2025" in result.output


def test_profile_flag_selects_isolated_workspace(tmp_path):
    config_dir = tmp_path / "profiles" / "security-20"
    config_dir.mkdir(parents=True)
    (config_dir / "config.yaml").write_text(
        "\n".join(
            (
                "profile_id: security-20",
                "immutable_snapshot: false",
                "data_dir: data/workspaces/security-20/dataset",
                "events: [ccs]",
            )
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli,
        ["--base-dir", str(tmp_path), "--profile", "security-20", "stats"],
    )

    assert result.exit_code == 0
    assert (tmp_path / "data/workspaces/security-20/dataset/papers.db").is_file()
    assert not (tmp_path / "data/dataset/papers.db").exists()
