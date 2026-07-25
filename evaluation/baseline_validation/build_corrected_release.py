#!/usr/bin/env python3
"""Derive the correction manifest and the corrected corpus from the manual labels.

The audited snapshot is immutable: its SHA-256 is cited in the paper, and every
reported count and measurement is reproducible from it. Corrections therefore
never overwrite it. They produce a second, separately named object with its own
digest, so a reader can reproduce the paper from one file and work from repaired
text in the other without the two ever being confused.

Both outputs are derived from `manual_labels.csv`. Nothing here is curated by
hand: a manifest maintained separately from the labels drifted once already and
silently omitted records that had verified corrections.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import shutil
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARTIFACT_ROOT = HERE.parent.parent
LABELS = HERE / "manual_labels.csv"
MANIFEST = HERE / "pending_corrections.csv"
FROZEN = ARTIFACT_ROOT / "data" / "dataset" / "papers.db.gz"
CORRECTED = ARTIFACT_ROOT / "data" / "dataset" / "papers-corrected.db.gz"

CITED_DIGEST = "0f4dbaa97d0cf39abd2340adb3280643df090b5de9cd1a29bff39a0b53ef64cd"

MANIFEST_FIELDS = [
    "paper_id",
    "venue",
    "defect",
    "current_abstract_chars",
    "corrected_abstract_chars",
    "evidence_url",
    "access_date",
    "applied_in",
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_defects() -> list[dict[str, str]]:
    with LABELS.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [row for row in rows if row["label"] != "valid"]


def materialize(source: Path, target: Path) -> None:
    with gzip.open(source, "rb") as compressed, target.open("wb") as plain:
        shutil.copyfileobj(compressed, plain)


def compress(source: Path, target: Path) -> None:
    # mtime=0 so the digest depends on the data alone and stays reproducible.
    with source.open("rb") as plain, gzip.GzipFile(target, "wb", mtime=0) as compressed:
        shutil.copyfileobj(plain, compressed)


def apply_corrections(db_path: Path, defects: list[dict[str, str]]) -> list[dict[str, str]]:
    connection = sqlite3.connect(db_path)
    manifest: list[dict[str, str]] = []
    for defect in defects:
        paper_id = defect["paper_id"]
        corrected = defect["corrected_abstract"].strip()
        row = connection.execute(
            "SELECT event, LENGTH(abstract) FROM papers WHERE paper_id = ?", (paper_id,)
        ).fetchone()
        if row is None:
            raise SystemExit(f"labeled record {paper_id} is absent from the snapshot")
        venue, current_chars = row
        if not corrected:
            raise SystemExit(f"record {paper_id} is labeled {defect['label']} with no corrected text")
        connection.execute(
            "UPDATE papers SET abstract = ? WHERE paper_id = ?", (corrected, paper_id)
        )
        manifest.append(
            {
                "paper_id": paper_id,
                "venue": venue,
                "defect": defect["label"],
                "current_abstract_chars": str(current_chars),
                "corrected_abstract_chars": str(len(corrected)),
                "evidence_url": defect["evidence_url"],
                "access_date": defect["access_date"],
                "applied_in": "papers-corrected.db.gz",
            }
        )
    connection.commit()
    connection.close()
    return manifest


def write_manifest(manifest: list[dict[str, str]]) -> None:
    with MANIFEST.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(manifest, key=lambda row: int(row["paper_id"])))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the manifest matches the labels without writing the corpus",
    )
    arguments = parser.parse_args()

    if digest(FROZEN) != CITED_DIGEST:
        raise SystemExit(
            f"{FROZEN.name} no longer matches the digest cited in the paper; refusing to proceed"
        )

    defects = read_defects()
    working = ARTIFACT_ROOT / "data" / "dataset" / "_corrected.db"
    materialize(FROZEN, working)
    manifest = apply_corrections(working, defects)

    if arguments.check:
        with MANIFEST.open(encoding="utf-8") as handle:
            on_disk = {row["paper_id"] for row in csv.DictReader(handle)}
        working.unlink()
        expected = {row["paper_id"] for row in manifest}
        if on_disk != expected:
            print(f"manifest covers {len(on_disk)} of {len(expected)} corrected records", file=sys.stderr)
            print(f"  missing: {sorted(expected - on_disk)}", file=sys.stderr)
            return 1
        print(f"manifest covers all {len(expected)} corrected records")
        return 0

    write_manifest(manifest)
    compress(working, CORRECTED)
    connection = sqlite3.connect(working)
    papers, abstracts = connection.execute(
        "SELECT COUNT(*), COUNT(abstract) FROM papers"
    ).fetchone()
    connection.close()
    working.unlink()

    print(f"records corrected:  {len(manifest)}")
    print(f"papers / abstracts: {papers} / {abstracts}")
    print(f"audited corpus      {FROZEN.name}  SHA-256 {digest(FROZEN)}")
    print(f"corrected corpus    {CORRECTED.name}  SHA-256 {digest(CORRECTED)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
