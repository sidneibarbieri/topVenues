"""Profile isolation and immutable-manifest checks."""

import sqlite3

import pytest

import src.sqlite_connection as sqlite_connection_module
from src.profiles import (
    DEFAULT_PROFILE_ID,
    PROJECT_ROOT,
    load_profile,
    select_profile_id,
    verified_profile_snapshot,
)


def test_default_profile_is_the_tool_denominator(monkeypatch) -> None:
    monkeypatch.delenv("TOPVENUES_PROFILE", raising=False)
    assert select_profile_id() == DEFAULT_PROFILE_ID == "security-20-v4"


def test_environment_can_select_an_explicit_profile(monkeypatch) -> None:
    monkeypatch.setenv("TOPVENUES_PROFILE", "security-20")
    assert select_profile_id() == "security-20"


def test_unknown_profile_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("TOPVENUES_PROFILE", "latest")
    with pytest.raises(ValueError, match="unknown profile"):
        select_profile_id()


def test_archived_security_snapshot_keeps_manifest_and_distribution_identity() -> None:
    profile = load_profile("security-20", PROJECT_ROOT)
    assert profile.manifest["snapshot"]["papers"] == 20305
    assert profile.manifest["snapshot"]["venues"] == 20
    assert profile.manifest["distribution"]["bundled"] is False
    assert profile.manifest["distribution"]["url"].endswith(
        "/v1.0.1/data/profiles/security-20/papers.db.gz"
    )


def test_current_snapshot_matches_strict_declared_window() -> None:
    with verified_profile_snapshot("security-20-v3", PROJECT_ROOT) as verified:
        snapshot = verified.profile.manifest["snapshot"]
        assert snapshot["papers"] == 14859
        assert snapshot["observed_year_min"] == 2019
        assert snapshot["observed_year_max"] == 2026


def test_verified_snapshot_closes_database_before_temp_cleanup(monkeypatch) -> None:
    connections: list[sqlite3.Connection] = []
    original_connect = sqlite_connection_module.sqlite3.connect

    def tracked_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(sqlite_connection_module.sqlite3, "connect", tracked_connect)
    with verified_profile_snapshot("security-20-v3", PROJECT_ROOT):
        pass

    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connections[0].execute("SELECT 1")
