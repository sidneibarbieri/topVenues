#!/usr/bin/env python3
"""Build a successor that adds what DBLP published after the source froze.

Additive by construction. Every source record is copied unchanged. A record
enters only when its DBLP key and its canonical resource are both absent from
the source, its year is inside the declared window, and no identity decision
already merged it away. Everything else stays out, with its reason logged.

Three steps, so the enrichment runs through the same commands as any corpus:

    python scripts/build_extended_profile.py stage --dump dblp-2026-09-01.xml.gz --staging STAGE
    python -m src.cli --base-dir STAGE backfill-abstracts
    python -m src.cli --base-dir STAGE extract
    python -m src.cli --base-dir STAGE bibtex-from-dump --dump-dir DUMPDIR
    python scripts/build_extended_profile.py freeze --staging STAGE --dump-release 10.4230/dblp.xml.2026-09-01

`stage` materializes and consolidates the declared scope from a DBLP dump into
STAGE and keeps only the additive records. `freeze` copies the verified source,
inserts them, and writes the snapshot, its manifest and the extension log.
"""

import argparse
import asyncio
import gzip
import hashlib
import json
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import date
from pathlib import Path

from pydantic import BaseModel, ConfigDict

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.collector import Collector  # noqa: E402
from src.dblp_dump_materializer import DblpDumpMaterializer  # noqa: E402
from src.profiles import load_profile, verified_profile_snapshot  # noqa: E402

DEFAULT_SOURCE = "security-20-v4"
DEFAULT_TARGET = "security-20-v5"
IDENTITY_LOG = ROOT / "data" / "adjudication" / "security-20-v3-identity.json"
EXTENSION_FILE = "extension.json"
COPIED_COLUMNS = (
    "score", "paper_id", "authors", "title", "venue", "pages", "year", "paper_type",
    "access", "key", "ee", "url", "event", "abstract", "bibtex",
)  # fmt: skip
DOI_PREFIXES = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
)


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Succession(Frozen):
    """The frozen profile a successor extends, and the successor's identifier."""

    source: str
    target: str


class StagedRecord(Frozen):
    key: str
    event: str
    year: int
    resource: str | None


class ExcludedRecord(StagedRecord):
    reason: str


class ExtensionLog(Frozen):
    schema_version: str = "1.0.0"
    source_profile: str
    target_profile: str
    dump: str
    staged_on: str
    policy: str
    added: tuple[StagedRecord, ...]
    excluded: tuple[ExcludedRecord, ...]


def canonical_resource(link: str | None) -> str | None:
    """A DOI or landing page reduced to one comparable form."""
    if not link:
        return None
    value = link.strip().lower().rstrip("/")
    for prefix in DOI_PREFIXES:
        if value.startswith(prefix):
            return "doi:" + value.removeprefix(prefix)
    return value.replace("http://", "https://", 1)


def merged_resources(identity_log: Path) -> set[str]:
    """DOIs an identity decision merged into another record's DOI."""
    decisions = json.loads(identity_log.read_text(encoding="utf-8"))["decisions"]
    merged = set()
    for decision in decisions:
        if decision["decision"] != "merge_alias":
            continue
        match = re.search(r"/works/(10\.[^\s\"]+)$", decision["evidence"])
        if match:
            merged.add("doi:" + match.group(1).lower())
    return merged


def source_identity(database: Path) -> tuple[set[str], set[str]]:
    with sqlite3.connect(database) as connection:
        rows = connection.execute("SELECT key, ee FROM papers").fetchall()
    keys = {key for key, _ in rows}
    resources = {canonical_resource(link) for _, link in rows if link}
    return keys, resources


def classify(
    record: StagedRecord,
    source_keys: set[str],
    source_resources: set[str],
    merged: set[str],
    declared_years: set[int],
) -> str | None:
    """Why a staged record stays out, or None when it is a genuine addition."""
    if record.key in source_keys:
        return "already in the source"
    if record.resource in source_resources:
        return "resource already in the source"
    if record.resource in merged:
        return "merged as a DOI alias by the identity log"
    if record.year not in declared_years:
        return "outside the declared year window"
    return None


def write_staging_config(source_config: Path, staging: Path, succession: Succession) -> None:
    text = source_config.read_text(encoding="utf-8")
    text = re.sub(
        r"^profile_id: .*$",
        f"profile_id: {succession.target}-staging",
        text,
        flags=re.M,
    )
    text = re.sub(r"^immutable_snapshot: .*$", "immutable_snapshot: false", text, flags=re.M)
    text = re.sub(r"^snapshot_path: .*\n", "", text, flags=re.M)
    text = re.sub(r"data/workspaces/[^/]+/", "workspace/", text)
    (staging / "config.yaml").write_text(text, encoding="utf-8")


def staging_collector(staging: Path) -> Collector:
    """The staging workspace, read through its own configuration, never the default one."""
    return Collector(base_dir=staging, config_path=staging / "config.yaml")


def staged_records(database: Path) -> list[StagedRecord]:
    with sqlite3.connect(database) as connection:
        rows = connection.execute("SELECT key, event, year, ee FROM papers").fetchall()
    return [
        StagedRecord(key=key, event=event, year=int(year), resource=canonical_resource(link))
        for key, event, year, link in rows
    ]


def stage(succession: Succession, dump: Path, staging: Path, identity_log: Path) -> None:
    source = load_profile(succession.source, ROOT)
    staging.mkdir(parents=True, exist_ok=True)
    write_staging_config(source.config_path, staging, succession)
    collector = staging_collector(staging)
    DblpDumpMaterializer(
        collector.config, collector.json_dir, collector.log_dir, dump
    ).materialize()
    asyncio.run(collector.run_consolidate())

    declared_years = set(collector.config.effective_years())
    merged = merged_resources(identity_log)
    with verified_profile_snapshot(succession.source, ROOT) as verified:
        source_keys, source_resources = source_identity(verified.database_path)

    added, excluded = [], []
    for record in staged_records(collector.db.db_path):
        reason = classify(record, source_keys, source_resources, merged, declared_years)
        if reason is None:
            added.append(record)
        elif reason not in {"already in the source", "resource already in the source"}:
            excluded.append(ExcludedRecord(**record.model_dump(), reason=reason))

    with sqlite3.connect(collector.db.db_path) as connection:
        connection.execute("CREATE TEMP TABLE kept(key TEXT PRIMARY KEY)")
        connection.executemany("INSERT INTO kept VALUES (?)", [(record.key,) for record in added])
        connection.execute("DELETE FROM papers WHERE key NOT IN (SELECT key FROM kept)")

    log = ExtensionLog(
        source_profile=succession.source,
        target_profile=succession.target,
        dump=dump.name,
        staged_on=date.today().isoformat(),
        policy=(
            "Additive: every source record is copied unchanged; a record enters only when its "
            "DBLP key and canonical resource are absent from the source, its year is declared, "
            "and no identity decision merged it. Added records use the DBLP key as paper_id, "
            "because the dump carries no numeric DBLP identifier."
        ),
        added=tuple(sorted(added, key=lambda record: (record.event, record.year, record.key))),
        excluded=tuple(excluded),
    )
    (staging / EXTENSION_FILE).write_text(log.model_dump_json(indent=1) + "\n", encoding="utf-8")
    print(f"staged {len(added)} additions, excluded {len(excluded)}; enrich STAGE next")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_gzip(source: Path, target: Path) -> None:
    """A byte-stable gzip, so the digest depends only on the content."""
    with (
        source.open("rb") as input_file,
        target.open("wb") as output_file,
        gzip.GzipFile(fileobj=output_file, mode="wb", compresslevel=9, mtime=0) as archive,
    ):
        shutil.copyfileobj(input_file, archive, length=1024 * 1024)


def insert_additions(target: Path, staged: Path, expected_keys: set[str]) -> None:
    columns = ", ".join(COPIED_COLUMNS)
    with sqlite3.connect(staged) as source_connection:
        rows = source_connection.execute(f"SELECT {columns} FROM papers").fetchall()
    staged_keys = {row[COPIED_COLUMNS.index("key")] for row in rows}
    if staged_keys != expected_keys:
        raise SystemExit("the staged database no longer matches its extension log")
    placeholders = ", ".join("?" for _ in COPIED_COLUMNS)
    with sqlite3.connect(target) as connection:
        connection.executemany(f"INSERT INTO papers ({columns}) VALUES ({placeholders})", rows)


def event_counts(connection: sqlite3.Connection) -> list[dict]:
    rows = connection.execute(
        """
        SELECT event, COUNT(*),
               SUM(abstract IS NOT NULL AND TRIM(abstract) <> ''),
               SUM(bibtex IS NOT NULL AND TRIM(bibtex) <> ''),
               MIN(year), MAX(year)
        FROM papers GROUP BY event ORDER BY event
        """
    ).fetchall()
    return [
        {"event": event, "papers": papers, "abstracts": abstracts, "bibtex": bibtex,
         "year_min": first, "year_max": last}
        for event, papers, abstracts, bibtex, first, last in rows
    ]  # fmt: skip


def snapshot_declaration(database: Path, archive: Path, declared_path: str) -> dict:
    with sqlite3.connect(database) as connection:
        counts = event_counts(connection)
        years = connection.execute("SELECT MIN(year), MAX(year) FROM papers").fetchone()
    return {
        "path": declared_path,
        "gzip_bytes": archive.stat().st_size,
        "gzip_sha256": sha256(archive),
        "sqlite_bytes": database.stat().st_size,
        "sqlite_sha256": sha256(database),
        "papers": sum(count["papers"] for count in counts),
        "abstracts": sum(count["abstracts"] for count in counts),
        "bibtex": sum(count["bibtex"] for count in counts),
        "venues": len(counts),
        "observed_year_min": years[0],
        "observed_year_max": years[1],
        "event_counts": counts,
    }


def successor_manifest(
    source_manifest: dict, snapshot: dict, extension: ExtensionLog, dump_release: str
) -> dict:
    target, source = extension.target_profile, extension.source_profile
    manifest = json.loads(json.dumps(source_manifest))
    manifest["profile_id"] = target
    manifest["origin"] = f"additive successor of {source} from the DBLP XML release {dump_release}"
    manifest["configuration"]["path"] = f"profiles/{target}/config.yaml"
    manifest["configuration"]["workspace_data_dir"] = f"data/workspaces/{target}/dataset"
    manifest["snapshot"] = snapshot
    manifest["built_on"] = date.today().isoformat()
    manifest.pop("repair_log", None)
    manifest["extension_log"] = {
        "path": f"data/adjudication/{target}-extension.json",
        "records_added": len(extension.added),
        "records_excluded": len(extension.excluded),
        "dblp_release": dump_release,
        "note": "Additions only; every source record is copied unchanged.",
    }
    return manifest


def successor_config(source_config: Path, succession: Succession) -> str:
    return source_config.read_text(encoding="utf-8").replace(succession.source, succession.target)


def freeze(staging: Path, dump_release: str, output_root: Path) -> None:
    """Write the successor's snapshot, manifest, configuration and log under output_root."""
    extension = ExtensionLog.model_validate_json(
        (staging / EXTENSION_FILE).read_text(encoding="utf-8")
    )
    succession = Succession(source=extension.source_profile, target=extension.target_profile)
    source = load_profile(succession.source, ROOT)
    declared_archive = f"data/profiles/{succession.target}/papers.db.gz"
    archive = output_root / declared_archive
    archive.parent.mkdir(parents=True, exist_ok=True)
    with (
        verified_profile_snapshot(succession.source, ROOT) as verified,
        tempfile.TemporaryDirectory(prefix="topvenues-extend-") as workspace,
    ):
        working_copy = Path(workspace) / "papers.db"
        shutil.copyfile(verified.database_path, working_copy)
        added_keys = {record.key for record in extension.added}
        insert_additions(working_copy, staging_collector(staging).db.db_path, added_keys)
        write_gzip(working_copy, archive)
        snapshot = snapshot_declaration(working_copy, archive, declared_archive)

    manifest = successor_manifest(source.manifest, snapshot, extension, dump_release)
    (archive.parent / "manifest.json").write_text(
        json.dumps(manifest, indent=1) + "\n", encoding="utf-8"
    )
    config = output_root / manifest["configuration"]["path"]
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(successor_config(source.config_path, succession), encoding="utf-8")
    log = output_root / manifest["extension_log"]["path"]
    log.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(staging / EXTENSION_FILE, log)
    print(f"{succession.target}: {snapshot['papers']} records, {snapshot['abstracts']} abstracts")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    stage_command = commands.add_parser("stage")
    stage_command.add_argument("--source", default=DEFAULT_SOURCE)
    stage_command.add_argument("--target", default=DEFAULT_TARGET)
    stage_command.add_argument("--dump", type=Path, required=True)
    stage_command.add_argument("--staging", type=Path, required=True)
    stage_command.add_argument("--identity-log", type=Path, default=IDENTITY_LOG)
    freeze_command = commands.add_parser("freeze")
    freeze_command.add_argument("--staging", type=Path, required=True)
    freeze_command.add_argument("--dump-release", required=True)
    freeze_command.add_argument(
        "--output-root",
        type=Path,
        default=ROOT,
        help="repository root, or a review folder",
    )
    arguments = parser.parse_args()
    if arguments.command == "stage":
        succession = Succession(source=arguments.source, target=arguments.target)
        stage(
            succession,
            arguments.dump.resolve(),
            arguments.staging.resolve(),
            arguments.identity_log,
        )
    else:
        freeze(
            arguments.staging.resolve(),
            arguments.dump_release,
            arguments.output_root.resolve(),
        )


if __name__ == "__main__":
    main()
