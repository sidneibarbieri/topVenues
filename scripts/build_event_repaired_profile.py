#!/usr/bin/env python3
"""Build the event-repaired successor of a frozen profile.

DBLP files a co-located workshop as "WOOT @ USENIX Security Symposium", and the
event normalizer matched the host before the workshop: 49 WOOT papers were
counted as USENIX Security in every security-20 profile. The normalizer now
reads the event before the "@".

``declare`` re-derives every record's event from its venue with the current
normalizer and writes each disagreement to a log; ``build`` applies that log to
the verified source and freezes the successor. Only the ``event`` column
changes, and only for the records the log names, so every other field and the
record count are preserved by construction.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
import tempfile
from datetime import date
from pathlib import Path

from pydantic import BaseModel, ConfigDict

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.build_extended_profile import snapshot_declaration, write_gzip  # noqa: E402
from src.event_normalizer import EventNormalizer  # noqa: E402
from src.profiles import load_profile, verified_profile_snapshot  # noqa: E402
from src.sqlite_connection import managed_sqlite_connection  # noqa: E402

SOURCE_PROFILE = "security-20-v5"
TARGET_PROFILE = "security-20-v6"
REPAIR_LOG = ROOT / "data" / "adjudication" / f"{TARGET_PROFILE}-events.json"


class EventRepair(BaseModel):
    model_config = ConfigDict(frozen=True)

    paper_id: str
    key: str
    venue: str
    stored_event: str
    corrected_event: str


class EventRepairLog(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_profile: str
    target_profile: str
    rule: str
    repairs: tuple[EventRepair, ...]


def derive_repairs(connection: sqlite3.Connection) -> tuple[EventRepair, ...]:
    normalizer = EventNormalizer()
    rows = connection.execute("SELECT paper_id, key, venue, event FROM papers ORDER BY paper_id")
    return tuple(
        EventRepair(
            paper_id=str(paper_id),
            key=key,
            venue=venue,
            stored_event=event,
            corrected_event=normalizer.normalize(venue),
        )
        for paper_id, key, venue, event in rows
        if normalizer.normalize(venue) != event
    )


def declare() -> None:
    with (
        verified_profile_snapshot(SOURCE_PROFILE, ROOT) as verified,
        managed_sqlite_connection(verified.database_path) as connection,
    ):
        repairs = derive_repairs(connection)
    log = EventRepairLog(
        source_profile=SOURCE_PROFILE,
        target_profile=TARGET_PROFILE,
        rule='A venue written "EVENT @ HOST" belongs to EVENT, not to HOST.',
        repairs=repairs,
    )
    REPAIR_LOG.write_text(log.model_dump_json(indent=1) + "\n", encoding="utf-8")
    print(f"declared {len(repairs)} event repairs in {REPAIR_LOG}")


def apply_repairs(connection: sqlite3.Connection, repairs: tuple[EventRepair, ...]) -> None:
    """Set each declared event, failing loudly if the stored value moved."""
    for repair in repairs:
        row = connection.execute(
            "SELECT event FROM papers WHERE paper_id = ?", (repair.paper_id,)
        ).fetchone()
        if row is None:
            raise SystemExit(f"{repair.paper_id} is not in {SOURCE_PROFILE}")
        if row[0] != repair.stored_event:
            raise SystemExit(f"{repair.paper_id}: stored event is {row[0]!r}; the log is stale")
        connection.execute(
            "UPDATE papers SET event = ? WHERE paper_id = ?",
            (repair.corrected_event, repair.paper_id),
        )


def successor_manifest(source_manifest: dict, snapshot: dict, log: EventRepairLog) -> dict:
    manifest = json.loads(json.dumps(source_manifest))
    manifest["profile_id"] = TARGET_PROFILE
    manifest["origin"] = f"event-repaired successor of {SOURCE_PROFILE}"
    manifest["configuration"]["path"] = f"profiles/{TARGET_PROFILE}/config.yaml"
    manifest["configuration"]["workspace_data_dir"] = f"data/workspaces/{TARGET_PROFILE}/dataset"
    manifest["snapshot"] = snapshot
    manifest["built_on"] = date.today().isoformat()
    manifest.pop("extension_log", None)
    manifest["repair_log"] = {
        "path": REPAIR_LOG.relative_to(ROOT).as_posix(),
        "events_repaired": len(log.repairs),
        "note": "Event labels only; no record added, removed or otherwise changed.",
    }
    return manifest


def build() -> None:
    log = EventRepairLog.model_validate_json(REPAIR_LOG.read_text(encoding="utf-8"))
    source = load_profile(SOURCE_PROFILE, ROOT)
    declared_archive = f"data/profiles/{TARGET_PROFILE}/papers.db.gz"
    archive = ROOT / declared_archive
    archive.parent.mkdir(parents=True, exist_ok=True)
    with (
        verified_profile_snapshot(SOURCE_PROFILE, ROOT) as verified,
        tempfile.TemporaryDirectory(prefix="topvenues-events-") as workspace,
    ):
        working_copy = Path(workspace) / "papers.db"
        shutil.copyfile(verified.database_path, working_copy)
        with managed_sqlite_connection(working_copy) as connection:
            apply_repairs(connection, log.repairs)
            connection.commit()
        write_gzip(working_copy, archive)
        snapshot = snapshot_declaration(working_copy, archive, declared_archive)

    manifest = successor_manifest(source.manifest, snapshot, log)
    (archive.parent / "manifest.json").write_text(
        json.dumps(manifest, indent=1) + "\n", encoding="utf-8"
    )
    config = ROOT / manifest["configuration"]["path"]
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        source.config_path.read_text(encoding="utf-8").replace(SOURCE_PROFILE, TARGET_PROFILE),
        encoding="utf-8",
    )
    print(f"{TARGET_PROFILE}: {snapshot['papers']} records, {len(log.repairs)} events repaired")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("command", choices=("declare", "build"))
    arguments = parser.parse_args()
    if arguments.command == "declare":
        declare()
    else:
        build()


if __name__ == "__main__":
    main()
