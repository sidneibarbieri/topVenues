#!/usr/bin/env python3
"""Generate the frozen, venue-stratified sample used by the live pilot.

This script samples only records with a non-empty abstract because the manual
sheet estimates extraction accuracy conditional on an abstract being present.
Corpus coverage (9,911/9,925) is an exact snapshot count and is not estimated
from this sample.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import math
import random
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path

SNAPSHOT_SHA256 = "0f4dbaa97d0cf39abd2340adb3280643df090b5de9cd1a29bff39a0b53ef64cd"
SEED = 20260721
SAMPLE_SIZE = 200


def default_snapshot() -> Path:
    """Locate the frozen snapshot in either paper-worktree or artifact layout."""
    script = Path(__file__).resolve()
    candidates = (
        Path.home() / "topVenues/data/dataset/papers.db.gz",
        script.parents[2] / "data/dataset/papers.db.gz",
        script.parents[3] / "artifact/data/dataset/papers.db.gz",
    )
    return next((path for path in candidates if path.is_file()), candidates[0])


DEFAULT_SNAPSHOT = default_snapshot()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def materialize_snapshot(snapshot: Path, destination: Path) -> None:
    if snapshot.suffix == ".gz":
        with gzip.open(snapshot, "rb") as source, destination.open("wb") as target:
            shutil.copyfileobj(source, target)
    else:
        shutil.copyfile(snapshot, destination)


def extract_doi(ee: str | None) -> str:
    match = re.search(r"doi\.org/(10\..+)$", ee or "", flags=re.IGNORECASE)
    return match.group(1).rstrip("/") if match else ""


def largest_remainder_allocation(counts: dict[str, int], size: int) -> dict[str, int]:
    """Proportional allocation with one record guaranteed for every venue."""
    total = sum(counts.values())
    quotas = {venue: size * count / total for venue, count in counts.items()}
    allocated = {venue: max(1, math.floor(quota)) for venue, quota in quotas.items()}

    while sum(allocated.values()) < size:
        venue = max(counts, key=lambda item: (quotas[item] - allocated[item], counts[item]))
        allocated[venue] += 1
    while sum(allocated.values()) > size:
        candidates = [venue for venue in counts if allocated[venue] > 1]
        venue = min(
            candidates,
            key=lambda item: (quotas[item] - allocated[item], -counts[item]),
        )
        allocated[venue] -= 1
    return allocated


def generate(snapshot: Path) -> list[dict[str, str | int]]:
    actual = file_sha256(snapshot)
    if actual != SNAPSHOT_SHA256:
        raise SystemExit(
            "Refusing to mix corpus versions. Expected submitted snapshot SHA-256 "
            f"{SNAPSHOT_SHA256}, got {actual} from {snapshot}."
        )

    with tempfile.TemporaryDirectory(prefix="topvenues-sample-") as directory:
        database = Path(directory) / "papers.db"
        materialize_snapshot(snapshot, database)
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        population = [
            dict(row)
            for row in connection.execute(
                """
                SELECT paper_id, key, event, year, title, ee, abstract
                FROM papers
                WHERE TRIM(COALESCE(abstract, '')) <> ''
                """
            )
        ]

    by_venue: dict[str, list[dict]] = {}
    for row in population:
        by_venue.setdefault(row["event"], []).append(row)
    allocation = largest_remainder_allocation(
        {venue: len(rows) for venue, rows in by_venue.items()}, SAMPLE_SIZE
    )

    randomizer = random.Random(SEED)
    sample: list[dict] = []
    for venue in sorted(by_venue):
        ordered = sorted(by_venue[venue], key=lambda row: row["paper_id"])
        sample.extend(randomizer.sample(ordered, allocation[venue]))

    result = []
    for row in sorted(sample, key=lambda item: item["paper_id"]):
        abstract = row.pop("abstract")
        result.append(
            {
                **row,
                "doi": extract_doi(row["ee"]),
                "abstract_sha256": hashlib.sha256(abstract.encode("utf-8")).hexdigest(),
                "venue_population_n": len(by_venue[row["event"]]),
                "venue_sample_n": allocation[row["event"]],
                "sampling_weight": f"{len(by_venue[row['event']]) / allocation[row['event']]:.12f}",
            }
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("sample.csv"))
    parser.add_argument(
        "--verify",
        action="store_true",
        help="compare generated rows with the committed sample instead of rewriting it",
    )
    args = parser.parse_args()
    rows = generate(args.snapshot)
    fieldnames = list(rows[0])

    if args.verify:
        with args.output.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            committed_fields = reader.fieldnames or []
            committed = list(reader)
        if committed_fields != fieldnames:
            raise SystemExit("Committed sample columns do not match seed/snapshot generation.")
        generated = [
            {field: "" if row[field] is None else str(row[field]) for field in fieldnames}
            for row in rows
        ]
        if generated != committed:
            mismatch = (
                next(
                    (
                        index,
                        field,
                        generated_row[field],
                        committed_row[field],
                    )
                    for index, (generated_row, committed_row) in enumerate(
                        zip(generated, committed, strict=False), start=2
                    )
                    for field in fieldnames
                    if generated_row[field] != committed_row.get(field, "")
                )
                if len(generated) == len(committed)
                else None
            )
            detail = (
                ""
                if mismatch is None
                else " First mismatch: line {} field {} generated={!r} committed={!r}.".format(
                    *mismatch
                )
            )
            raise SystemExit(
                f"Committed sample does not match the complete seed/snapshot generation.{detail}"
            )
        print(f"verified {len(rows)} sampled records against {args.output}")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} sampled records to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
