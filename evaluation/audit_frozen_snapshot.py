#!/usr/bin/env python3
"""Audit duplicate identities and recover abstract-source evidence offline.

This program never writes to the supplied SQLite snapshot, archive, or cache.
It emits deterministic CSV/JSON reports whose conclusions are bound to the
input bytes by SHA-256 digests.

Important distinction: an exact match in a historical log or API cache is
evidence consistent with a source; it is not proof of the source that wrote the
database field.  The frozen schema does not persist that attribution.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import re
import shutil
import sqlite3
import tempfile
import unicodedata
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

SCHEMA_VERSION = "1.0.0"
POLICY_VERSION = "topvenues-frozen-audit/1.0.0"
EXPECTED_FROZEN_GZIP_SHA256 = "0f4dbaa97d0cf39abd2340adb3280643df090b5de9cd1a29bff39a0b53ef64cd"
EXPECTED_FROZEN_SQLITE_SHA256 = "0f1d76413b480a0fdb368cc297d79c499621c41417f937ec268d7d8e6b8da1e5"
EXPECTED_RECORDS = 9_925
EXPECTED_NONEMPTY_ABSTRACTS = 9_911

LOG_MEMBER = "data/log/abstract_log.csv"
LOG_SOURCES = frozenset({"ACMExtractor", "IEEEExtractor", "NDSSExtractor", "USENIXExtractor"})
CACHE_SOURCES = frozenset({"crossref", "openalex", "semanticscholar"})

DOI_RE = re.compile(r"(10\.\d{4,9}/[^\s\"<>]+)", re.IGNORECASE)
TIMESTAMP_RE = re.compile(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_abstract(value: Any) -> str:
    """Apply only whitespace normalization before exact-content matching."""

    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_identity(value: Any) -> str:
    """Normalize bibliographic identity fields without fuzzy matching."""

    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"[\W_]+", " ", text, flags=re.UNICODE).strip()


def extract_doi(value: Any) -> str | None:
    match = DOI_RE.search(str(value or "").strip())
    if not match:
        return None
    return match.group(1).lower().rstrip(".,;:)]}")


def stable_join(values: Iterable[str]) -> str:
    return ";".join(sorted({value for value in values if value}))


def logical_input_path(role: str, path: Path, base: Path | None = None) -> str:
    if base is not None:
        try:
            return path.relative_to(base).as_posix()
        except ValueError:
            pass
    if role == "frozen_snapshot":
        return path.name
    return path.as_posix()


@contextmanager
def read_only_database(snapshot: Path) -> Iterator[sqlite3.Connection]:
    """Open SQLite in immutable read-only mode, materializing gzip to /tmp."""

    temporary: tempfile.TemporaryDirectory[str] | None = None
    db_path = snapshot
    try:
        if snapshot.suffix == ".gz":
            temporary = tempfile.TemporaryDirectory(prefix="topvenues-audit-")
            db_path = Path(temporary.name) / "papers.db"
            with gzip.open(snapshot, "rb") as source, db_path.open("wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)

        uri_path = quote(str(db_path.resolve()), safe="/")
        connection = sqlite3.connect(f"file:{uri_path}?mode=ro&immutable=1", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()
    finally:
        if temporary is not None:
            temporary.cleanup()


def load_papers(snapshot: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    required = {
        "paper_id",
        "key",
        "title",
        "authors",
        "year",
        "pages",
        "ee",
        "event",
        "abstract",
    }
    with read_only_database(snapshot) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(papers)").fetchall()}
        missing = required - columns
        if missing:
            raise RuntimeError(f"Snapshot schema is missing columns: {sorted(missing)}")

        query = """
            SELECT paper_id, key, title, authors, year, pages, ee, event,
                   abstract, created_at, updated_at
            FROM papers
            ORDER BY COALESCE(key, ''), paper_id
        """
        rows = [dict(row) for row in connection.execute(query).fetchall()]
        timestamp_row = connection.execute(
            """
            SELECT MIN(created_at), MAX(created_at),
                   MIN(updated_at), MAX(updated_at)
            FROM papers
            """
        ).fetchone()

    for row in rows:
        row["paper_id"] = str(row["paper_id"])
        row["normalized_abstract"] = normalize_abstract(row.get("abstract"))
        row["doi"] = extract_doi(row.get("ee"))

    timestamps = {
        "created_at_min_naive": timestamp_row[0],
        "created_at_max_naive": timestamp_row[1],
        "updated_at_min_naive": timestamp_row[2],
        "updated_at_max_naive": timestamp_row[3],
        "timezone_recorded": False,
    }
    return rows, timestamps


def audit_archive_log(
    archive: Path,
    papers_by_id: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], bytes]:
    evidence: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"sources": set(), "timestamps": set(), "exact_rows": 0}
    )
    statuses: Counter[str] = Counter()
    valid_timestamps: list[str] = []
    total_rows = 0
    malformed_rows = 0
    exact_rows = 0

    with zipfile.ZipFile(archive) as bundle:
        if LOG_MEMBER not in bundle.namelist():
            raise RuntimeError(f"Archive does not contain {LOG_MEMBER!r}")
        member_bytes = bundle.read(LOG_MEMBER)

    stream = io.TextIOWrapper(
        io.BytesIO(member_bytes), encoding="utf-8-sig", errors="replace", newline=""
    )
    for row in csv.DictReader(stream):
        total_rows += 1
        if None in row:
            malformed_rows += 1

        paper_id = str(row.get("ID", "")).strip()
        source = str(row.get("Source", "")).strip()
        status = str(row.get("Status", "")).strip()
        timestamp = str(row.get("Timestamp", "")).strip()
        statuses[status] += 1
        if TIMESTAMP_RE.fullmatch(timestamp):
            valid_timestamps.append(timestamp)

        paper = papers_by_id.get(paper_id)
        candidate = normalize_abstract(row.get("Abstract"))
        if (
            paper is None
            or status != "ok"
            or source not in LOG_SOURCES
            or not candidate
            or candidate != paper["normalized_abstract"]
        ):
            continue

        hit = evidence[paper_id]
        hit["sources"].add(source)
        if TIMESTAMP_RE.fullmatch(timestamp):
            hit["timestamps"].add(timestamp)
        hit["exact_rows"] += 1
        exact_rows += 1

    summary = {
        "member": LOG_MEMBER,
        "member_sha256": sha256_bytes(member_bytes),
        "total_csv_rows": total_rows,
        "malformed_csv_rows": malformed_rows,
        "status_counts": dict(sorted(statuses.items())),
        "exact_content_and_paper_id_rows": exact_rows,
        "exact_content_and_paper_id_records": len(evidence),
        "exact_record_counts_by_source": dict(
            sorted(
                Counter(source for hit in evidence.values() for source in hit["sources"]).items()
            )
        ),
        "valid_timestamp_min_naive": min(valid_timestamps) if valid_timestamps else None,
        "valid_timestamp_max_naive": max(valid_timestamps) if valid_timestamps else None,
        "timestamp_timezone_recorded": False,
    }
    return evidence, summary, member_bytes


def audit_api_cache(
    cache_source: Path,
    papers: list[dict[str, Any]],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, Any],
    list[dict[str, Any]],
]:
    by_doi: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for paper in papers:
        if paper["doi"]:
            by_doi[paper["doi"]].append(paper)

    evidence: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"sources": set(), "files": set(), "timestamps": set()}
    )
    input_rows: list[dict[str, Any]] = []
    source_file_counts: Counter[str] = Counter()
    parse_errors: list[str] = []
    unsupported_source_files = 0
    nonempty_values = 0
    exact_files = 0

    if cache_source.is_dir():
        cache_entries = [
            (path.name, path.read_bytes())
            for path in sorted(cache_source.glob("*.json"), key=lambda path: path.name)
        ]
    else:
        with zipfile.ZipFile(cache_source) as bundle:
            names = sorted(
                name
                for name in bundle.namelist()
                if name.startswith("data/cache/") and name.endswith(".json")
            )
            cache_entries = [(Path(name).name, bundle.read(name)) for name in names]

    for filename, raw in cache_entries:
        input_rows.append(
            {
                "role": "api_cache_entry",
                "relative_path": filename,
                "bytes": len(raw),
                "sha256": sha256_bytes(raw),
            }
        )
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            parse_errors.append(f"{filename}: {type(error).__name__}")
            continue

        cache_key = str(payload.get("key", ""))
        source, separator, key_tail = cache_key.partition("_")
        source = source.casefold()
        if not separator or source not in CACHE_SOURCES:
            unsupported_source_files += 1
            continue
        source_file_counts[source] += 1

        candidate = normalize_abstract(payload.get("value"))
        if not candidate:
            continue
        nonempty_values += 1
        doi = extract_doi(key_tail)
        if doi is None:
            continue

        matched_file = False
        for paper in by_doi.get(doi, []):
            if candidate != paper["normalized_abstract"]:
                continue
            matched_file = True
            hit = evidence[paper["paper_id"]]
            hit["sources"].add(source)
            hit["files"].add(filename)
            created_at = str(payload.get("created_at", "")).strip()
            if created_at:
                hit["timestamps"].add(created_at)
        if matched_file:
            exact_files += 1

    summary = {
        "json_files": len(cache_entries),
        "parse_error_files": len(parse_errors),
        "parse_errors": sorted(parse_errors),
        "unsupported_source_files": unsupported_source_files,
        "source_file_counts": dict(sorted(source_file_counts.items())),
        "nonempty_values": nonempty_values,
        "exact_doi_and_content_files": exact_files,
        "exact_doi_and_content_records": len(evidence),
        "exact_record_counts_by_source": dict(
            sorted(
                Counter(source for hit in evidence.values() for source in hit["sources"]).items()
            )
        ),
    }
    return evidence, summary, input_rows


def group_duplicates(
    papers: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    key_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    doi_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    title_author_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for paper in papers:
        if paper.get("key"):
            key_groups[str(paper["key"])].append(paper)
        if paper["doi"]:
            doi_groups[paper["doi"]].append(paper)
        identity = (
            normalize_identity(paper.get("title")),
            normalize_identity(paper.get("authors")),
        )
        if all(identity):
            title_author_groups[identity].append(paper)

    duplicate_key_groups = [group for group in key_groups.values() if len(group) > 1]
    duplicate_doi_groups = [group for group in doi_groups.values() if len(group) > 1]
    title_author_groups = {
        identity: group for identity, group in title_author_groups.items() if len(group) > 1
    }

    manifest: list[dict[str, Any]] = []
    exact_clusters = 0
    possible_version_clusters = 0
    for _identity, group in sorted(
        title_author_groups.items(), key=lambda item: (item[0][0], item[0][1])
    ):
        years = {str(paper.get("year") or "") for paper in group}
        pages = {normalize_identity(paper.get("pages")) for paper in group}
        if len(years) == 1 and len(pages) == 1:
            classification = "exact_bibliographic_duplicate_candidate"
            exact_clusters += 1
        else:
            classification = "possible_version_relation"
            possible_version_clusters += 1

        stable_members = sorted(str(paper.get("key") or paper["paper_id"]) for paper in group)
        cluster_id = (
            "ta-" + hashlib.sha256("\n".join(stable_members).encode("utf-8")).hexdigest()[:12]
        )
        for paper in sorted(group, key=lambda item: (str(item.get("key") or ""), item["paper_id"])):
            manifest.append(
                {
                    "cluster_id": cluster_id,
                    "classification": classification,
                    "cluster_size": len(group),
                    "paper_id": paper["paper_id"],
                    "dblp_key": paper.get("key") or "",
                    "title": paper.get("title") or "",
                    "authors": paper.get("authors") or "",
                    "year": paper.get("year") or "",
                    "pages": paper.get("pages") or "",
                    "doi": paper["doi"] or "",
                    "event": paper.get("event") or "",
                    "retained_in_frozen_snapshot": "yes",
                    "retention_reason": ("frozen DBLP-record denominator; no silent collapse"),
                }
            )

    summary = {
        "identity_unit_in_snapshot": "DBLP bibliographic record",
        "paper_id_distinct": len({paper["paper_id"] for paper in papers}),
        "dblp_key_nonempty": sum(bool(paper.get("key")) for paper in papers),
        "dblp_key_distinct": len({paper.get("key") for paper in papers if paper.get("key")}),
        "duplicate_dblp_key_groups": len(duplicate_key_groups),
        "duplicate_doi_groups": len(duplicate_doi_groups),
        "normalized_title_author_groups": len(title_author_groups),
        "exact_bibliographic_duplicate_candidate_clusters": exact_clusters,
        "exact_bibliographic_duplicate_candidate_records": sum(
            1
            for row in manifest
            if row["classification"] == "exact_bibliographic_duplicate_candidate"
        ),
        "possible_version_relation_clusters": possible_version_clusters,
        "possible_version_relation_records": sum(
            1 for row in manifest if row["classification"] == "possible_version_relation"
        ),
        "excess_rows_if_exact_candidates_were_collapsed": sum(
            len(group) - 1
            for group in title_author_groups.values()
            if len({str(paper.get("year") or "") for paper in group}) == 1
            and len({normalize_identity(paper.get("pages")) for paper in group}) == 1
        ),
        "mutation_performed": False,
        "journal_extension_policy": (
            "retain distinct DBLP publications; represent version relations explicitly"
        ),
        "fuzzy_or_semantic_version_matching_performed": False,
    }
    return manifest, summary


def provenance_manifest(
    papers: list[dict[str, Any]],
    log_evidence: dict[str, dict[str, Any]],
    cache_evidence: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    for paper in papers:
        paper_id = paper["paper_id"]
        abstract = paper["normalized_abstract"]
        log_hit = log_evidence.get(paper_id)
        cache_hit = cache_evidence.get(paper_id)

        if not abstract:
            status = "missing_abstract"
        elif log_hit and cache_hit:
            status = "exact_archive_log_and_api_cache_evidence"
        elif log_hit:
            status = "exact_archive_log_evidence"
        elif cache_hit:
            status = "exact_api_cache_evidence"
        else:
            status = "unresolved_no_retained_source_evidence"
        status_counts[status] += 1

        log_timestamps = sorted(log_hit["timestamps"]) if log_hit else []
        cache_timestamps = sorted(cache_hit["timestamps"]) if cache_hit else []
        manifest.append(
            {
                "paper_id": paper_id,
                "dblp_key": paper.get("key") or "",
                "has_abstract": "yes" if abstract else "no",
                "normalized_abstract_sha256": (
                    hashlib.sha256(abstract.encode("utf-8")).hexdigest() if abstract else ""
                ),
                "evidence_status": status,
                "archive_log_sources": (stable_join(log_hit["sources"]) if log_hit else ""),
                "archive_log_exact_rows": log_hit["exact_rows"] if log_hit else 0,
                "archive_log_first_timestamp_naive": (log_timestamps[0] if log_timestamps else ""),
                "archive_log_last_timestamp_naive": (log_timestamps[-1] if log_timestamps else ""),
                "api_cache_sources": (stable_join(cache_hit["sources"]) if cache_hit else ""),
                "api_cache_exact_files": (len(cache_hit["files"]) if cache_hit else 0),
                "api_cache_file_names": (stable_join(cache_hit["files"]) if cache_hit else ""),
                "api_cache_first_created_at_naive": (
                    cache_timestamps[0] if cache_timestamps else ""
                ),
                "api_cache_last_created_at_naive": (
                    cache_timestamps[-1] if cache_timestamps else ""
                ),
                "actual_insertion_source_known": "no",
                "pdf_origin_countable_from_supplied_evidence": "no",
            }
        )

    records_with_abstract = sum(bool(paper["normalized_abstract"]) for paper in papers)
    evidence_ids = set(log_evidence) | set(cache_evidence)
    unresolved = status_counts["unresolved_no_retained_source_evidence"]
    summary = {
        "records_with_nonempty_abstract": records_with_abstract,
        "records_missing_abstract": len(papers) - records_with_abstract,
        "records_with_exact_retained_source_evidence": len(evidence_ids),
        "records_with_exact_archive_log_evidence": len(log_evidence),
        "records_with_exact_api_cache_evidence": len(cache_evidence),
        "records_with_both_evidence_types": len(set(log_evidence) & set(cache_evidence)),
        "unresolved_nonempty_records": unresolved,
        "status_counts": dict(sorted(status_counts.items())),
        "actual_insertion_source_is_persisted_in_snapshot": False,
        "actual_pdf_derived_abstract_count": None,
        "records_with_exact_pdf_labeled_source_evidence": 0,
        "interpretation": (
            "Exact matches are retained evidence, not proof of the writer source. "
            "Unresolved records and PDF origin remain unknown."
        ),
    }
    return manifest, summary


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def manifest_digest(rows: list[dict[str, Any]]) -> str:
    payload = "".join(
        f"{row['role']}\0{row['relative_path']}\0{row['bytes']}\0{row['sha256']}\n" for row in rows
    ).encode("utf-8")
    return sha256_bytes(payload)


def parse_args() -> argparse.Namespace:
    script = Path(__file__).resolve()
    original_checkout = Path.home() / "topVenues"
    bundled_evidence = script.parent / "inputs/source_evidence.zip"
    snapshot_candidates = (
        original_checkout / "data/dataset/papers.db.gz",
        script.parents[1] / "data/dataset/papers.db.gz",
        script.parents[2] / "artifact/data/dataset/papers.db.gz",
    )
    default_snapshot = next(
        (path for path in snapshot_candidates if path.is_file()),
        snapshot_candidates[0],
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=default_snapshot,
        help="Frozen papers.db or papers.db.gz (opened read-only).",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        default=(
            bundled_evidence
            if bundled_evidence.is_file()
            else original_checkout / "data/archive/data_2026-05-04.zip"
        ),
        help="Historical archive containing data/log/abstract_log.csv.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=(
            bundled_evidence if bundled_evidence.is_file() else original_checkout / "data/cache"
        ),
        help="Historical API JSON cache directory or source-evidence bundle ZIP.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=script.parent / "output",
        help="Destination for deterministic manifests and summary.",
    )
    parser.add_argument(
        "--no-strict-snapshot",
        action="store_true",
        help="Allow a snapshot with a different SHA-256 or accepted counts.",
    )
    return parser.parse_args()


def validate_paths(args: argparse.Namespace) -> None:
    if not args.snapshot.is_file():
        raise FileNotFoundError(f"Snapshot not found: {args.snapshot}")
    if not args.archive.is_file():
        raise FileNotFoundError(f"Archive not found: {args.archive}")
    if not (args.cache_dir.is_dir() or args.cache_dir.is_file()):
        raise FileNotFoundError(f"Cache source not found: {args.cache_dir}")


def main() -> int:
    args = parse_args()
    validate_paths(args)

    snapshot_sha256 = sha256_file(args.snapshot)
    expected_snapshot_hashes = {
        EXPECTED_FROZEN_GZIP_SHA256,
        EXPECTED_FROZEN_SQLITE_SHA256,
    }
    if not args.no_strict_snapshot and snapshot_sha256 not in expected_snapshot_hashes:
        raise RuntimeError(
            "Snapshot SHA-256 differs from the submitted gzip/SQLite bytes. "
            "Use --no-strict-snapshot only for an intentional audit of another release."
        )

    papers, snapshot_timestamps = load_papers(args.snapshot)
    nonempty = sum(bool(paper["normalized_abstract"]) for paper in papers)
    if not args.no_strict_snapshot and (
        len(papers) != EXPECTED_RECORDS or nonempty != EXPECTED_NONEMPTY_ABSTRACTS
    ):
        raise RuntimeError(
            f"Unexpected accepted-snapshot counts: records={len(papers)}, "
            f"nonempty_abstracts={nonempty}"
        )

    papers_by_id = {paper["paper_id"]: paper for paper in papers}
    if len(papers_by_id) != len(papers):
        raise RuntimeError("paper_id is not unique in the supplied snapshot")

    log_evidence, log_summary, log_member_bytes = audit_archive_log(args.archive, papers_by_id)
    cache_evidence, cache_summary, cache_input_rows = audit_api_cache(args.cache_dir, papers)
    dedup_rows, dedup_summary = group_duplicates(papers)
    provenance_rows, provenance_summary = provenance_manifest(papers, log_evidence, cache_evidence)

    archive_sha256 = sha256_file(args.archive)
    cache_manifest_sha256 = manifest_digest(cache_input_rows)
    input_rows = [
        {
            "role": "frozen_snapshot",
            "relative_path": logical_input_path("frozen_snapshot", args.snapshot),
            "bytes": args.snapshot.stat().st_size,
            "sha256": snapshot_sha256,
        },
        {
            "role": "archive_zip",
            "relative_path": args.archive.name,
            "bytes": args.archive.stat().st_size,
            "sha256": archive_sha256,
        },
        {
            "role": "archive_log_member",
            "relative_path": LOG_MEMBER,
            "bytes": len(log_member_bytes),
            "sha256": sha256_bytes(log_member_bytes),
        },
        *cache_input_rows,
    ]
    input_rows.sort(key=lambda row: (row["role"], row["relative_path"]))

    output_dir = args.output_dir
    input_path = output_dir / "input_manifest.csv"
    dedup_path = output_dir / "dedup_candidates.csv"
    provenance_path = output_dir / "abstract_provenance_evidence.csv"
    summary_path = output_dir / "audit_summary.json"

    write_csv(
        input_path,
        input_rows,
        ["role", "relative_path", "bytes", "sha256"],
    )
    write_csv(
        dedup_path,
        dedup_rows,
        [
            "cluster_id",
            "classification",
            "cluster_size",
            "paper_id",
            "dblp_key",
            "title",
            "authors",
            "year",
            "pages",
            "doi",
            "event",
            "retained_in_frozen_snapshot",
            "retention_reason",
        ],
    )
    write_csv(
        provenance_path,
        provenance_rows,
        [
            "paper_id",
            "dblp_key",
            "has_abstract",
            "normalized_abstract_sha256",
            "evidence_status",
            "archive_log_sources",
            "archive_log_exact_rows",
            "archive_log_first_timestamp_naive",
            "archive_log_last_timestamp_naive",
            "api_cache_sources",
            "api_cache_exact_files",
            "api_cache_file_names",
            "api_cache_first_created_at_naive",
            "api_cache_last_created_at_naive",
            "actual_insertion_source_known",
            "pdf_origin_countable_from_supplied_evidence",
        ],
    )

    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "provider_mode": "offline",
        "decision_boundary": (
            "report exact retained evidence and duplicate candidates; mutate nothing"
        ),
        "parameters": {
            "abstract_matching": "same paper_id or DOI plus exact text after whitespace normalization",
            "dedup_candidate_matching": "normalized exact title and authors; no fuzzy matching",
            "strict_submitted_snapshot": not args.no_strict_snapshot,
        },
        "inputs": {
            "manifest_rows": len(input_rows),
            "manifest_payload_sha256": manifest_digest(input_rows),
            "frozen_snapshot_sha256": snapshot_sha256,
            "archive_zip_sha256": archive_sha256,
            "cache_json_files": len(cache_input_rows),
            "cache_manifest_payload_sha256": cache_manifest_sha256,
        },
        "snapshot": {
            "records": len(papers),
            "nonempty_abstracts": nonempty,
            "empty_abstracts": len(papers) - nonempty,
            "timestamps": snapshot_timestamps,
        },
        "archive_log_evidence": log_summary,
        "api_cache_evidence": cache_summary,
        "provenance": provenance_summary,
        "deduplication": dedup_summary,
        "excluded_alternatives": [
            "No unresolved abstract was assigned a source.",
            "No PDF origin count was inferred from absent provenance.",
            "No fuzzy title matching was used for duplicate or version detection.",
            "No journal extension was collapsed into a conference record.",
            "No row in the frozen snapshot was edited or deleted.",
        ],
        "outputs": {
            "input_manifest.csv": sha256_file(input_path),
            "dedup_candidates.csv": sha256_file(dedup_path),
            "abstract_provenance_evidence.csv": sha256_file(provenance_path),
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"Audited {len(papers):,} frozen DBLP records without mutation.")
    print(
        "Abstract evidence: "
        f"{provenance_summary['records_with_exact_retained_source_evidence']:,} exact, "
        f"{provenance_summary['unresolved_nonempty_records']:,} unresolved, "
        f"{provenance_summary['records_missing_abstract']:,} missing."
    )
    print(
        "Dedup audit: "
        f"{dedup_summary['exact_bibliographic_duplicate_candidate_clusters']} exact "
        "candidate clusters; 0 rows removed."
    )
    print(f"Reports: {output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
