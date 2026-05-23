"""Tests for CLI export behavior."""

from click.testing import CliRunner

from src.cli import cli
from src.database import DatabaseManager
from src.models import Paper


def _seed(base_dir):
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
