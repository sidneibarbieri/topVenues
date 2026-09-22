"""Named corpus profiles and immutable snapshot verification.

The profile configuration selects a disposable workspace.  Scientific
analyses never trust that workspace: they verify the profile manifest and
materialize the immutable snapshot into a temporary read-only database.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .models import Configuration
from .sqlite_connection import managed_sqlite_connection

DEFAULT_PROFILE_ID = "security-20-v4"
PROFILE_ENV_VAR = "TOPVENUES_PROFILE"
PROFILE_IDS = (
    "security-20",
    "security-20-v2",
    "security-20-v3",
    DEFAULT_PROFILE_ID,
)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Profile:
    """Resolved paths and declarations for one closed corpus profile."""

    profile_id: str
    root: Path
    config_path: Path
    manifest_path: Path
    snapshot_path: Path
    workspace_data_dir: Path
    configuration: Configuration
    manifest: dict[str, Any]


@dataclass(frozen=True)
class VerifiedSnapshot:
    """A verified profile and its temporary materialized SQLite database."""

    profile: Profile
    database_path: Path


def select_profile_id(requested: str | None = None) -> str:
    """Resolve an explicit profile, then the environment, then the tool default."""
    profile_id = requested or os.getenv(PROFILE_ENV_VAR) or DEFAULT_PROFILE_ID
    if profile_id not in PROFILE_IDS:
        choices = ", ".join(PROFILE_IDS)
        raise ValueError(f"unknown profile {profile_id!r}; choose one of: {choices}")
    return profile_id


def profile_config_path(profile_id: str, root: Path = PROJECT_ROOT) -> Path:
    """Return the configuration path for a validated named profile."""
    selected = select_profile_id(profile_id)
    return Path(root) / "profiles" / selected / "config.yaml"


def profile_manifest_path(profile_id: str, root: Path = PROJECT_ROOT) -> Path:
    """Return the immutable snapshot manifest path for a named profile."""
    selected = select_profile_id(profile_id)
    return Path(root) / "data" / "profiles" / selected / "manifest.json"


def _read_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"expected a mapping in {path}")
    return payload


def _within_root(root: Path, declared: str, label: str) -> Path:
    relative = Path(declared)
    if relative.is_absolute():
        raise RuntimeError(f"{label} must be relative to the artifact root: {declared}")
    resolved_root = root.resolve()
    resolved = (resolved_root / relative).resolve()
    if not resolved.is_relative_to(resolved_root):
        raise RuntimeError(f"{label} escapes the artifact root: {declared}")
    return resolved


def load_profile(profile_id: str | None = None, root: Path = PROJECT_ROOT) -> Profile:
    """Load and cross-check a named profile's config and manifest declarations."""
    selected = select_profile_id(profile_id)
    resolved_root = Path(root).resolve()
    config_path = profile_config_path(selected, resolved_root)
    manifest_path = profile_manifest_path(selected, resolved_root)
    config_payload = _read_mapping(config_path)
    manifest = _read_mapping(manifest_path)

    if config_payload.get("profile_id") != selected:
        raise RuntimeError(f"profile_id mismatch in {config_path}")
    if config_payload.get("immutable_snapshot") is not True:
        raise RuntimeError(f"profile {selected} does not declare an immutable snapshot")
    if manifest.get("profile_id") != selected:
        raise RuntimeError(f"profile_id mismatch in {manifest_path}")

    snapshot_manifest = manifest.get("snapshot")
    configuration_manifest = manifest.get("configuration")
    if not isinstance(snapshot_manifest, dict):
        raise RuntimeError(f"missing snapshot declaration in {manifest_path}")
    if not isinstance(configuration_manifest, dict):
        raise RuntimeError(f"missing configuration declaration in {manifest_path}")

    config_snapshot = config_payload.get("snapshot_path")
    manifest_snapshot = snapshot_manifest.get("path")
    if config_snapshot != manifest_snapshot:
        raise RuntimeError(
            f"snapshot path mismatch for {selected}: config={config_snapshot!r}, "
            f"manifest={manifest_snapshot!r}"
        )
    if config_payload.get("events") != configuration_manifest.get("event_keys"):
        raise RuntimeError(f"event-key mismatch between config and manifest for {selected}")
    if config_payload.get("years") != configuration_manifest.get("declared_years"):
        raise RuntimeError(f"year-scope mismatch between config and manifest for {selected}")
    if config_payload.get("data_dir") != configuration_manifest.get("workspace_data_dir"):
        raise RuntimeError(f"workspace mismatch between config and manifest for {selected}")
    declared_events = config_payload.get("events")
    if not isinstance(declared_events, list) or len(declared_events) != snapshot_manifest.get(
        "venues"
    ):
        raise RuntimeError(f"venue-count mismatch between config and snapshot for {selected}")

    configuration = Configuration(**config_payload)
    return Profile(
        profile_id=selected,
        root=resolved_root,
        config_path=config_path,
        manifest_path=manifest_path,
        snapshot_path=_within_root(resolved_root, str(config_snapshot), "snapshot_path"),
        workspace_data_dir=_within_root(resolved_root, str(configuration.data_dir), "data_dir"),
        configuration=configuration,
        manifest=manifest,
    )


def sha256_file(path: Path) -> str:
    """Compute a streaming SHA-256 digest."""
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _verify_file(path: Path, declaration: dict[str, Any], *, prefix: str = "") -> None:
    if not path.is_file():
        raise FileNotFoundError(path)
    bytes_key = f"{prefix}bytes" if prefix else "bytes"
    digest_key = f"{prefix}sha256" if prefix else "sha256"
    expected_bytes = declaration.get(bytes_key)
    expected_digest = declaration.get(digest_key)
    actual_bytes = path.stat().st_size
    if actual_bytes != expected_bytes:
        raise RuntimeError(
            f"size mismatch for {path}: expected {expected_bytes}, got {actual_bytes}"
        )
    actual_digest = sha256_file(path)
    if actual_digest != expected_digest:
        raise RuntimeError(
            f"SHA-256 mismatch for {path}: expected {expected_digest}, got {actual_digest}"
        )


def _database_observation(connection: sqlite3.Connection) -> dict[str, Any]:
    totals = connection.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN abstract IS NOT NULL AND TRIM(abstract) <> '' THEN 1 ELSE 0 END),
               SUM(CASE WHEN bibtex IS NOT NULL AND TRIM(bibtex) <> '' THEN 1 ELSE 0 END),
               COUNT(DISTINCT event), MIN(year), MAX(year)
        FROM papers
        """
    ).fetchone()
    event_rows = connection.execute(
        """
        SELECT event, COUNT(*),
               SUM(CASE WHEN abstract IS NOT NULL AND TRIM(abstract) <> '' THEN 1 ELSE 0 END),
               SUM(CASE WHEN bibtex IS NOT NULL AND TRIM(bibtex) <> '' THEN 1 ELSE 0 END),
               MIN(year), MAX(year)
        FROM papers
        GROUP BY event
        ORDER BY event
        """
    ).fetchall()
    return {
        "papers": totals[0],
        "abstracts": totals[1],
        "bibtex": totals[2],
        "venues": totals[3],
        "observed_year_min": totals[4],
        "observed_year_max": totals[5],
        "event_counts": [
            {
                "event": row[0],
                "papers": row[1],
                "abstracts": row[2],
                "bibtex": row[3],
                "year_min": row[4],
                "year_max": row[5],
            }
            for row in event_rows
        ],
    }


@contextmanager
def verified_profile_snapshot(
    profile_id: str | None = None,
    root: Path = PROJECT_ROOT,
) -> Iterator[VerifiedSnapshot]:
    """Yield a temporary SQLite copy after validating the complete manifest."""
    profile = load_profile(profile_id, root)
    snapshot_declaration = profile.manifest["snapshot"]
    _verify_file(profile.snapshot_path, snapshot_declaration, prefix="gzip_")
    with tempfile.TemporaryDirectory(prefix=f"topvenues-{profile.profile_id}-") as directory:
        database_path = Path(directory) / "papers.db"
        sqlite_digest = hashlib.sha256()
        with gzip.open(profile.snapshot_path, "rb") as source, database_path.open("wb") as target:
            while chunk := source.read(1024 * 1024):
                sqlite_digest.update(chunk)
                target.write(chunk)

        expected_sqlite_bytes = snapshot_declaration.get("sqlite_bytes")
        if database_path.stat().st_size != expected_sqlite_bytes:
            raise RuntimeError(
                f"SQLite size mismatch for {profile.profile_id}: expected "
                f"{expected_sqlite_bytes}, got {database_path.stat().st_size}"
            )
        expected_sqlite_digest = snapshot_declaration.get("sqlite_sha256")
        actual_sqlite_digest = sqlite_digest.hexdigest()
        if actual_sqlite_digest != expected_sqlite_digest:
            raise RuntimeError(
                f"SQLite SHA-256 mismatch for {profile.profile_id}: expected "
                f"{expected_sqlite_digest}, got {actual_sqlite_digest}"
            )

        uri = f"file:{database_path.resolve()}?mode=ro&immutable=1"
        with managed_sqlite_connection(uri, uri=True) as connection:
            observed = _database_observation(connection)
        expected = {key: snapshot_declaration.get(key) for key in observed}
        if observed != expected:
            raise RuntimeError(f"database contents do not match manifest for {profile.profile_id}")

        yield VerifiedSnapshot(profile=profile, database_path=database_path)


def copy_verified_snapshot_to_workspace(profile: Profile, database_path: Path) -> Path:
    """Replace only the disposable workspace database with a verified copy."""
    target = profile.workspace_data_dir / "papers.db"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(database_path, target)
    return target
