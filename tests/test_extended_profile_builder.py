"""The extension builder only ever adds, and says why it leaves a record out.

A successor built from a newer DBLP dump would silently undo earlier identity
decisions if it re-imported everything: merged DOI aliases would come back and
out-of-window years would re-enter. These cases pin the rules that prevent it.
"""

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _builder():
    spec = importlib.util.spec_from_file_location(
        "build_extended_profile", ROOT / "scripts" / "build_extended_profile.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BUILDER = _builder()
YEARS = set(range(2019, 2027))


@pytest.mark.parametrize(
    ("link", "expected"),
    [
        ("https://doi.org/10.1109/SP46215.2023.10179305", "doi:10.1109/sp46215.2023.10179305"),
        ("http://dx.doi.org/10.1145/3395351.3399341/", "doi:10.1145/3395351.3399341"),
        ("http://www.usenix.org/conference/woot26/x", "https://www.usenix.org/conference/woot26/x"),
        (None, None),
    ],
)
def test_resources_compare_in_one_canonical_form(link, expected):
    assert BUILDER.canonical_resource(link) == expected


def test_the_identity_log_yields_every_merged_alias():
    merged = BUILDER.merged_resources(BUILDER.IDENTITY_LOG)
    decisions = json.loads(BUILDER.IDENTITY_LOG.read_text(encoding="utf-8"))["decisions"]
    aliases = [decision for decision in decisions if decision["decision"] == "merge_alias"]
    assert len(merged) == len(aliases)
    assert "doi:10.1109/sp46215.2023.10179305" in merged


def _record(key: str, year: int = 2026, resource: str | None = None):
    return BUILDER.StagedRecord(key=key, event="IEEE S&P", year=year, resource=resource)


def test_a_new_edition_record_is_added():
    record = _record("conf/sp/New26", resource="doi:10.1109/sp.2026.1")
    assert BUILDER.classify(record, set(), set(), set(), YEARS) is None


def test_a_source_record_is_never_added_again():
    record = _record("conf/sp/Old23", 2023, "doi:10.1109/sp.2023.1")
    assert BUILDER.classify(record, {"conf/sp/Old23"}, set(), set(), YEARS) is not None


def test_a_record_sharing_a_source_resource_stays_out():
    record = _record("conf/sp/Rekeyed23", 2023, "doi:10.1109/sp.2023.1")
    reason = BUILDER.classify(record, set(), {"doi:10.1109/sp.2023.1"}, set(), YEARS)
    assert reason == "resource already in the source"


def test_a_merged_alias_does_not_come_back():
    record = _record("conf/sp/JiangZXSLY23", 2023, "doi:10.1109/sp46215.2023.10179305")
    merged = {"doi:10.1109/sp46215.2023.10179305"}
    assert (
        BUILDER.classify(record, set(), set(), merged, YEARS)
        == "merged as a DOI alias by the identity log"
    )


def test_a_year_outside_the_declared_window_stays_out():
    record = _record("journals/ftsec/Old17", 2017)
    assert (
        BUILDER.classify(record, set(), set(), set(), YEARS) == "outside the declared year window"
    )


def _frozen_profile(root: Path, abstract: str | None) -> Path:
    """A one-record frozen profile laid out as the repository keeps one."""
    import sqlite3

    database = root / "papers.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE papers (paper_id TEXT PRIMARY KEY, event TEXT, year INTEGER, "
            "abstract TEXT, bibtex TEXT)"
        )
        connection.execute(
            "INSERT INTO papers VALUES ('conf/sp/X26', 'IEEE S&P', 2026, ?, '@x{}')", (abstract,)
        )
    declared = "data/profiles/demo/papers.db.gz"
    archive = root / declared
    archive.parent.mkdir(parents=True)
    BUILDER.write_gzip(database, archive)
    snapshot = BUILDER.snapshot_declaration(database, archive, declared)
    (archive.parent / "manifest.json").write_text(json.dumps({"snapshot": snapshot}))
    log = root / "repairs.json"
    repair = {
        "paper_id": "conf/sp/X26",
        "abstract": "We fill a missing abstract.",
        "source_url": "https://doi.org/10.0/x",
        "reviewer": "A Reviewer",
        "decided_at": "2026-09-22",
        "reason": "abstract collection failed",
    }
    log.write_text(json.dumps({"profile_id": "demo", "repairs": [repair]}))
    return log


def test_a_repair_fills_a_missing_abstract_and_restates_the_snapshot(tmp_path):
    log = _frozen_profile(tmp_path, abstract=None)
    BUILDER.repair_abstracts(log, tmp_path)
    manifest = json.loads((tmp_path / "data/profiles/demo/manifest.json").read_text())
    archive = tmp_path / manifest["snapshot"]["path"]
    assert manifest["snapshot"]["abstracts"] == 1
    assert manifest["snapshot"]["gzip_sha256"] == BUILDER.sha256(archive)
    assert manifest["repair_log"]["abstracts_repaired"] == 1
    assert (tmp_path / manifest["repair_log"]["path"]).is_file()


def test_a_repair_never_overwrites_an_existing_abstract(tmp_path):
    log = _frozen_profile(tmp_path, abstract="Text the source already carries.")
    with pytest.raises(SystemExit, match="already has an abstract"):
        BUILDER.repair_abstracts(log, tmp_path)
