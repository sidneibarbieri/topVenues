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
