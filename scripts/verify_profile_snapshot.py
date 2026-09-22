#!/usr/bin/env python3
"""Verify that a named corpus profile matches its immutable snapshot manifest."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profiles import PROFILE_IDS, verified_profile_snapshot  # noqa: E402
from src.sqlite_connection import managed_sqlite_connection  # noqa: E402

SOURCE_METADATA = {
    "security-20": {
        "release_status": "sf-submission",
        "paper_denominator": False,
        "origin": "security-oriented Salão de Ferramentas release",
        "source_release_tag": "sbseg2026-sf-submission-r1",
    },
    "security-20-v2": {
        "release_status": "post-publication-successor",
        "paper_denominator": False,
        "origin": "deduplicated successor of the security-oriented Salão de Ferramentas release",
        "source_release_tag": "v1.1.0",
    },
    "security-20-v3": {
        "release_status": "post-publication-adjudicated-successor",
        "paper_denominator": False,
        "origin": "strict-window, identity-adjudicated successor of security-20-v2",
        "source_release_tag": "v1.2.1",
    },
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def materialize(snapshot: Path, destination: Path) -> str:
    digest = hashlib.sha256()
    with gzip.open(snapshot, "rb") as source, destination.open("wb") as target:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            target.write(chunk)
    return digest.hexdigest()


def build_manifest(profile_id: str) -> dict[str, object]:
    config_path = ROOT / "profiles" / profile_id / "config.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if config.get("profile_id") != profile_id:
        raise RuntimeError(f"profile_id mismatch in {config_path}")
    if config.get("immutable_snapshot") is not True:
        raise RuntimeError(f"profile {profile_id} is not declared immutable")

    snapshot_relative = Path(config["snapshot_path"])
    snapshot = ROOT / snapshot_relative
    if not snapshot.is_file():
        raise FileNotFoundError(snapshot)

    with tempfile.TemporaryDirectory(prefix=f"topvenues-{profile_id}-") as directory:
        database = Path(directory) / "papers.db"
        sqlite_sha256 = materialize(snapshot, database)
        uri = f"file:{database.resolve()}?mode=ro&immutable=1"
        with managed_sqlite_connection(uri, uri=True) as connection:
            totals = connection.execute(
                """
                SELECT COUNT(*),
                       SUM(CASE WHEN abstract IS NOT NULL AND TRIM(abstract) <> ''
                                THEN 1 ELSE 0 END),
                       SUM(CASE WHEN bibtex IS NOT NULL AND TRIM(bibtex) <> ''
                                THEN 1 ELSE 0 END),
                       COUNT(DISTINCT event), MIN(year), MAX(year)
                FROM papers
                """
            ).fetchone()
            event_rows = connection.execute(
                """
                SELECT event, COUNT(*),
                       SUM(CASE WHEN abstract IS NOT NULL AND TRIM(abstract) <> ''
                                THEN 1 ELSE 0 END),
                       SUM(CASE WHEN bibtex IS NOT NULL AND TRIM(bibtex) <> ''
                                THEN 1 ELSE 0 END),
                       MIN(year), MAX(year)
                FROM papers
                GROUP BY event
                ORDER BY event
                """
            ).fetchall()
            duplicate_resource_groups = connection.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT LOWER(TRIM(ee))
                    FROM papers
                    WHERE ee IS NOT NULL AND TRIM(ee) <> ''
                    GROUP BY LOWER(TRIM(ee))
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
            unresolved_bibliographic_conflicts = connection.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT LOWER(TRIM(title)), LOWER(TRIM(COALESCE(authors, ''))),
                           year, LOWER(TRIM(COALESCE(pages, '')))
                    FROM papers
                    GROUP BY 1, 2, 3, 4
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        sqlite_bytes = database.stat().st_size

    return {
        "schema_version": "1.0.0",
        "profile_id": profile_id,
        **SOURCE_METADATA[profile_id],
        "configuration": {
            "path": config_path.relative_to(ROOT).as_posix(),
            "event_keys": config["events"],
            "declared_years": config["years"],
            "workspace_data_dir": config["data_dir"],
        },
        "snapshot": {
            "path": snapshot_relative.as_posix(),
            "gzip_bytes": snapshot.stat().st_size,
            "gzip_sha256": sha256_file(snapshot),
            "sqlite_bytes": sqlite_bytes,
            "sqlite_sha256": sqlite_sha256,
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
        },
        "identity_policy": {
            "merged_on": (
                "exact canonical DOI or stable landing page, plus Crossref-confirmed DOI aliases"
                if profile_id == "security-20-v3"
                else "exact canonical DOI or stable landing page"
            ),
            "remaining_duplicate_resource_groups": duplicate_resource_groups,
            "unresolved_same_metadata_groups": unresolved_bibliographic_conflicts,
            "adjudication_log": (
                "data/adjudication/security-20-v3-identity.json"
                if profile_id == "security-20-v3"
                else None
            ),
            "note": (
                "Records with different canonical resources remain distinct even when "
                "title, authors, year, and pages coincide."
            ),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, choices=PROFILE_IDS)
    parser.add_argument("--write-manifest", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--machine",
        action="store_true",
        help="Print snapshot path, workspace dir, counts, and SHA as one TSV row.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = ROOT / "data" / "profiles" / args.profile / "manifest.json"

    if args.write_manifest:
        actual = build_manifest(args.profile)
        if manifest_path.exists() and not args.force:
            raise FileExistsError(f"refusing to overwrite {manifest_path}; pass --force")
        manifest_path.write_text(
            json.dumps(actual, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    else:
        with verified_profile_snapshot(args.profile, ROOT) as verified:
            actual = verified.profile.manifest

    snapshot = actual["snapshot"]
    configuration = actual["configuration"]
    if args.machine:
        print(
            "\t".join(
                str(value)
                for value in (
                    snapshot["path"],
                    configuration["workspace_data_dir"],
                    snapshot["papers"],
                    snapshot["abstracts"],
                    snapshot["bibtex"],
                    snapshot["gzip_sha256"],
                )
            )
        )
    else:
        action = "wrote" if args.write_manifest else "verified"
        print(
            f"{action} {args.profile}: {snapshot['papers']:,} papers, "
            f"{snapshot['venues']} venues, sha256={snapshot['gzip_sha256']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
