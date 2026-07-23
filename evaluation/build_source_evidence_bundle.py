#!/usr/bin/env python3
"""Build the minimal deterministic input bundle for the provenance audit."""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path

EXPECTED_ARCHIVE_SHA256 = "47d9ae8cb2d1d739435cdba9cdadd50da1f67622fea1533ec0d20ca22a2a5fe5"
EXPECTED_LOG_SHA256 = "fed4ef90decde44339ee34739282ac04c704b0d123dd5d822cb055068e35371c"
LOG_MEMBER = "data/log/abstract_log.csv"
EXPECTED_CACHE_FILES = 3270
FIXED_TIME = (1980, 1, 1, 0, 0, 0)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def write_member(bundle: zipfile.ZipFile, name: str, payload: bytes) -> None:
    info = zipfile.ZipInfo(name, FIXED_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    info.create_system = 3
    bundle.writestr(info, payload, compresslevel=9)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive",
        type=Path,
        default=Path.home() / "topVenues/data/archive/data_2026-05-04.zip",
    )
    parser.add_argument("--cache-dir", type=Path, default=Path.home() / "topVenues/data/cache")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).with_name("inputs") / "source_evidence.zip",
    )
    args = parser.parse_args()

    archive_sha = sha256_file(args.archive)
    if archive_sha != EXPECTED_ARCHIVE_SHA256:
        raise SystemExit(f"historical archive digest mismatch: {archive_sha}")
    with zipfile.ZipFile(args.archive) as archive:
        log_bytes = archive.read(LOG_MEMBER)
    if sha256_bytes(log_bytes) != EXPECTED_LOG_SHA256:
        raise SystemExit("abstract-log member digest mismatch")

    cache_paths = sorted(args.cache_dir.glob("*.json"), key=lambda path: path.name)
    if len(cache_paths) != EXPECTED_CACHE_FILES:
        raise SystemExit(f"expected {EXPECTED_CACHE_FILES} cache files, found {len(cache_paths)}")

    entries = []
    for path in cache_paths:
        payload = path.read_bytes()
        entries.append({"name": path.name, "bytes": len(payload), "sha256": sha256_bytes(payload)})
    manifest = {
        "schema_version": "topvenues-source-evidence-bundle/1.0.0",
        "source_archive_sha256": archive_sha,
        "log_member": LOG_MEMBER,
        "log_member_sha256": EXPECTED_LOG_SHA256,
        "cache_file_count": len(entries),
        "cache_entries": entries,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w") as bundle:
        write_member(bundle, LOG_MEMBER, log_bytes)
        for path in cache_paths:
            write_member(bundle, f"data/cache/{path.name}", path.read_bytes())
        write_member(
            bundle,
            "bundle_manifest.json",
            (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        )
    print(f"wrote {args.output} ({sha256_file(args.output)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
