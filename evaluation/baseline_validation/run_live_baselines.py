#!/usr/bin/env python3
"""Re-run the record-aligned TopVenues/DBLP/S2/OpenAlex live pilot.

The operation is retrieval of one *known* paper and its abstract, not corpus
discovery. Consequently, the resulting match rate is conditional field
coverage over a DBLP-derived denominator; it must not be reported as recall of
papers outside TopVenues. Live services drift, so the script records responses,
timestamps, status codes, logical HTTP requests, network attempts, latency,
rate-limit retries, and OpenAlex server-reported API-budget metering. Metering
is not an invoice or an out-of-pocket-cost measurement.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import statistics
import tempfile
import time
from pathlib import Path
from urllib.parse import quote
from xml.etree import ElementTree

import httpx

if __package__:
    from .generate_sample import DEFAULT_SNAPSHOT, SNAPSHOT_SHA256, file_sha256
else:
    from generate_sample import DEFAULT_SNAPSHOT, SNAPSHOT_SHA256, file_sha256

USER_AGENT = "TopVenues-baseline-audit/1.0 (responsible research benchmark)"


def normalize_title(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def token_set(value: str | None) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", (value or "").lower()))


def jaccard(left: str | None, right: str | None) -> float | None:
    left_tokens, right_tokens = token_set(left), token_set(right)
    if not left_tokens or not right_tokens:
        return None
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def openalex_abstract(inverted_index: dict | None) -> str | None:
    if not inverted_index:
        return None
    positions: dict[int, str] = {}
    for word, indexes in inverted_index.items():
        for index in indexes:
            positions[int(index)] = word
    return " ".join(positions[index] for index in sorted(positions)) or None


def percentile_95(values: list[float]) -> float | None:
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(0.95 * len(values)) - 1)]


def summarize_operation(
    prefix: str,
    operation: str,
    calls: list[dict[str, object]],
    rows: list[dict[str, object]],
) -> dict[str, object]:
    """Summarize one protocol operation without expanding batches into calls."""
    operation_calls = [call for call in calls if str(call.get("operation")) == operation]
    operation_rows = [row for row in rows if row.get(f"{prefix}_operation") == operation]
    successful_latencies = [
        float(call["latency_ms"]) for call in operation_calls if int(call.get("status", 0)) == 200
    ]
    network_attempts = sum(int(call.get("attempts", 0)) for call in operation_calls)
    result: dict[str, object] = {
        "sample_n": len(operation_rows),
        "logical_http_requests": len(operation_calls),
        "network_attempts_including_retries": network_attempts,
        "record_match_n": sum(bool(row.get(f"{prefix}_match")) for row in operation_rows),
        "abstract_n": sum(bool(row.get(f"{prefix}_has_abstract")) for row in operation_rows),
    }

    is_batch = len(operation_calls) == 1 and int(operation_calls[0].get("items", 1)) > 1
    if is_batch:
        result["http_status"] = int(operation_calls[0].get("status", 0))
        result["batch_wall_latency_ms"] = float(operation_calls[0]["latency_ms"])
    else:
        for status in sorted({int(call.get("status", 0)) for call in operation_calls}):
            result[f"http_{status}_n"] = sum(
                int(call.get("status", 0)) == status for call in operation_calls
            )
        result["request_latency_median_ms"] = (
            statistics.median(successful_latencies) if successful_latencies else None
        )
        result["request_latency_p95_ms"] = percentile_95(successful_latencies)

    if prefix == "openalex":
        metering_values = [
            float(call["server_reported_api_budget_metering_usd"])
            for call in operation_calls
            if call.get("server_reported_api_budget_metering_usd") is not None
        ]
        metered_attempts = sum(
            int(call.get("metered_network_attempts", 0)) for call in operation_calls
        )
        result.update(
            server_reported_api_budget_metering_usd=(
                sum(metering_values) if metering_values else None
            ),
            metered_network_attempts=metered_attempts,
            metering_completeness=(
                "complete"
                if metered_attempts == network_attempts
                else f"partial: {metered_attempts}/{network_attempts} attempts"
            ),
        )
    return result


def summarize_service(
    prefix: str,
    calls: list[dict[str, object]],
    rows: list[dict[str, object]],
) -> dict[str, object]:
    """Summarize a service with call-level HTTP counts and operation detail."""
    successful_latencies = [
        float(call["latency_ms"]) for call in calls if int(call.get("status", 0)) == 200
    ]
    network_attempts = sum(int(call.get("attempts", 0)) for call in calls)
    result: dict[str, object] = {
        "http_200_n": sum(int(call.get("status", 0)) == 200 for call in calls),
        "record_match_n": sum(bool(row.get(f"{prefix}_match")) for row in rows),
        "abstract_n": sum(bool(row.get(f"{prefix}_has_abstract")) for row in rows),
        "abstract_jaccard_ge_0_95_n": sum(
            float(row.get(f"{prefix}_abstract_jaccard") or 0) >= 0.95 for row in rows
        ),
        "logical_http_requests": len(calls),
        "logical_http_requests_by_operation": {
            operation: sum(call.get("operation") == operation for call in calls)
            for operation in sorted({str(call.get("operation")) for call in calls})
        },
        "network_attempts_including_retries": network_attempts,
        "latency_median_ms": (
            statistics.median(successful_latencies) if successful_latencies else None
        ),
        "latency_p95_ms": percentile_95(successful_latencies),
    }
    for operation in sorted({str(call.get("operation")) for call in calls}):
        result[operation] = summarize_operation(prefix, operation, calls, rows)

    if prefix == "openalex":
        metering_values = [
            float(call["server_reported_api_budget_metering_usd"])
            for call in calls
            if call.get("server_reported_api_budget_metering_usd") is not None
        ]
        metered_attempts = sum(int(call.get("metered_network_attempts", 0)) for call in calls)
        result.update(
            server_reported_api_budget_metering_usd=(
                sum(metering_values) if metering_values else None
            ),
            metered_network_attempts=metered_attempts,
            metering_completeness=(
                "complete"
                if metered_attempts == network_attempts
                else f"partial: {metered_attempts}/{network_attempts} attempts"
            ),
            metering_interpretation=(
                "Server-reported API-budget consumption across attempts exposing "
                "the meter; not an invoice or out-of-pocket cost."
            ),
        )
    return result


def server_reported_metering_usd(response: httpx.Response) -> float | None:
    """Read OpenAlex's per-response API-budget meter, when present."""
    value = response.headers.get("x-ratelimit-cost-usd")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    maximum_attempts: int = 6,
    **kwargs,
) -> tuple[httpx.Response, float, int, float | None, int]:
    """Return final response/latency, attempts, metering sum, and metered attempts."""
    metering_usd = 0.0
    metered_attempts = 0
    for attempt in range(1, maximum_attempts + 1):
        started = time.perf_counter()
        try:
            response = client.request(method, url, **kwargs)
            latency = time.perf_counter() - started
            response_metering = server_reported_metering_usd(response)
            if response_metering is not None:
                metering_usd += response_metering
                metered_attempts += 1
            retryable = response.status_code in {429, 500, 502, 503, 504}
            if not retryable or attempt == maximum_attempts:
                return (
                    response,
                    latency,
                    attempt,
                    metering_usd if metered_attempts else None,
                    metered_attempts,
                )
            delay = float(response.headers.get("retry-after") or min(2**attempt, 30))
        except httpx.HTTPError as error:
            latency = time.perf_counter() - started
            if attempt == maximum_attempts:
                request = httpx.Request(method, url)
                response = httpx.Response(599, request=request, text=str(error))
                return (
                    response,
                    latency,
                    attempt,
                    metering_usd if metered_attempts else None,
                    metered_attempts,
                )
            delay = min(2**attempt, 30)
        time.sleep(delay + 0.25)
    raise AssertionError("unreachable")


def load_snapshot(
    snapshot: Path, sample_rows: list[dict]
) -> tuple[Path, tempfile.TemporaryDirectory]:
    actual = file_sha256(snapshot)
    if actual != SNAPSHOT_SHA256:
        raise SystemExit(
            "Refusing to benchmark a different denominator. Expected SHA-256 "
            f"{SNAPSHOT_SHA256}; got {actual}."
        )
    temporary = tempfile.TemporaryDirectory(prefix="topvenues-live-")
    database = Path(temporary.name) / "papers.db"
    if snapshot.suffix == ".gz":
        with gzip.open(snapshot, "rb") as source, database.open("wb") as target:
            shutil.copyfileobj(source, target)
    else:
        shutil.copyfile(snapshot, database)

    connection = sqlite3.connect(database)
    for row in sample_rows:
        result = connection.execute(
            "SELECT abstract FROM papers WHERE paper_id = ?", (row["paper_id"],)
        ).fetchone()
        if not result:
            raise SystemExit(f"sample paper_id {row['paper_id']} is absent from snapshot")
        abstract = result[0] or ""
        digest = hashlib.sha256(abstract.encode("utf-8")).hexdigest()
        if digest != row["abstract_sha256"]:
            raise SystemExit(f"abstract digest mismatch for paper_id {row['paper_id']}")
        row["snapshot_abstract"] = abstract
    connection.close()
    return database, temporary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--sample", type=Path, default=Path(__file__).with_name("sample.csv"))
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Fresh directory for rerun outputs. Defaults to a timestamped "
            "reruns/ directory so the frozen July 2026 evidence is never overwritten."
        ),
    )
    parser.add_argument("--skip-dblp", action="store_true")
    parser.add_argument("--skip-semantic-scholar", action="store_true")
    parser.add_argument("--skip-openalex", action="store_true")
    args = parser.parse_args()
    started_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output_dir = args.output_dir or Path(__file__).with_name("reruns") / run_id
    output_files = (
        output_dir / "pilot_observations.csv",
        output_dir / "pilot_summary.json",
        output_dir / "pilot_raw_responses.json.gz",
    )
    existing_outputs = [path for path in output_files if path.exists()]
    if existing_outputs:
        names = ", ".join(str(path) for path in existing_outputs)
        raise SystemExit(f"Refusing to overwrite existing rerun evidence: {names}")

    with args.sample.open(newline="", encoding="utf-8") as source:
        sample = list(csv.DictReader(source))
    for row in sample:
        row["year"] = int(row["year"])

    database, temporary = load_snapshot(args.snapshot, sample)
    client = httpx.Client(timeout=30, headers={"User-Agent": USER_AGENT})
    observations = {row["paper_id"]: {"paper_id": row["paper_id"]} for row in sample}
    raw: dict[str, object] = {
        "started_at_utc": started_at_utc,
        "snapshot_sha256": SNAPSHOT_SHA256,
        "sample_size": len(sample),
        "responses": {"dblp": {}, "semantic_scholar": {}, "openalex": {}},
        "calls": {"dblp": [], "s2": [], "openalex": []},
    }

    # TopVenues: eleven warm primary-key trials per paper, no external calls.
    connection = sqlite3.connect(database)
    local_latencies = []
    for row in sample:
        for _ in range(11):
            started = time.perf_counter_ns()
            result = connection.execute(
                "SELECT title, year, abstract FROM papers WHERE paper_id = ?",
                (row["paper_id"],),
            ).fetchone()
            local_latencies.append((time.perf_counter_ns() - started) / 1_000_000)
        observations[row["paper_id"]].update(
            topvenues_match=bool(result),
            topvenues_has_abstract=bool(result and result[2]),
        )
    connection.close()

    if not args.skip_dblp:
        for index, row in enumerate(sample):
            if index:
                time.sleep(1.05)  # responsible 1-RPS pacing; excluded from latency
            url = "https://dblp.org/rec/" + quote(row["key"], safe="/") + ".xml"
            response, latency, attempts, _metering_usd, _metered_attempts = request_with_retry(
                client, "GET", url
            )
            raw["calls"]["dblp"].append(
                {
                    "operation": "record_xml",
                    "items": 1,
                    "status": response.status_code,
                    "latency_ms": latency * 1000,
                    "attempts": attempts,
                }
            )
            body = response.text if response.status_code == 200 else None
            title = None
            if body:
                root = ElementTree.fromstring(body)
                record = next(iter(root), None)
                title_node = record.find("title") if record is not None else None
                if title_node is not None:
                    title = "".join(title_node.itertext())
            observations[row["paper_id"]].update(
                dblp_operation="record_xml",
                dblp_status=response.status_code,
                dblp_latency_ms=latency * 1000,
                dblp_attempts=attempts,
                dblp_match=normalize_title(title) == normalize_title(row["title"]),
                dblp_has_abstract=False,
            )
            raw["responses"]["dblp"][row["paper_id"]] = body or response.text[:500]

    if not args.skip_semantic_scholar:
        headers = {}
        if os.getenv("S2_API_KEY"):
            headers["x-api-key"] = os.environ["S2_API_KEY"]
        doi_rows = [row for row in sample if row["doi"]]
        response, latency, attempts, _metering_usd, _metered_attempts = request_with_retry(
            client,
            "POST",
            "https://api.semanticscholar.org/graph/v1/paper/batch",
            headers=headers,
            params={"fields": "title,year,abstract,externalIds"},
            json={"ids": ["DOI:" + row["doi"] for row in doi_rows]},
        )
        objects = response.json() if response.status_code == 200 else [None] * len(doi_rows)
        raw["calls"]["s2"].append(
            {
                "operation": "doi_batch",
                "items": len(doi_rows),
                "status": response.status_code,
                "latency_ms": latency * 1000,
                "attempts": attempts,
            }
        )
        for row, item in zip(doi_rows, objects, strict=True):
            match = bool(item)  # a DOI singleton is an unambiguous identity match
            abstract = item.get("abstract") if item and match else None
            observations[row["paper_id"]].update(
                s2_operation="doi_batch",
                s2_status=response.status_code,
                s2_latency_ms=latency * 1000,
                s2_attempts=attempts,
                s2_match=match,
                s2_has_abstract=bool(abstract),
                s2_abstract_jaccard=jaccard(row["snapshot_abstract"], abstract),
            )
            raw["responses"]["semantic_scholar"][row["paper_id"]] = item

        non_doi_rows = [row for row in sample if not row["doi"]]
        for index, row in enumerate(non_doi_rows):
            if index:
                time.sleep(1.05)  # compatible with the documented 1-RPS keyed tier
            response, latency, attempts, _metering_usd, _metered_attempts = request_with_retry(
                client,
                "GET",
                "https://api.semanticscholar.org/graph/v1/paper/search/match",
                headers=headers,
                params={
                    "query": row["title"],
                    "fields": "title,year,abstract,externalIds",
                },
            )
            body = response.json() if response.status_code == 200 else {}
            raw["calls"]["s2"].append(
                {
                    "operation": "title_match",
                    "items": 1,
                    "status": response.status_code,
                    "latency_ms": latency * 1000,
                    "attempts": attempts,
                }
            )
            item = next(
                (
                    candidate
                    for candidate in body.get("data", [])
                    if normalize_title(candidate.get("title")) == normalize_title(row["title"])
                    and abs((candidate.get("year") or 0) - row["year"]) <= 1
                ),
                None,
            )
            abstract = item.get("abstract") if item else None
            observations[row["paper_id"]].update(
                s2_operation="title_match",
                s2_status=response.status_code,
                s2_latency_ms=latency * 1000,
                s2_attempts=attempts,
                s2_match=bool(item),
                s2_has_abstract=bool(abstract),
                s2_abstract_jaccard=jaccard(row["snapshot_abstract"], abstract),
            )
            raw["responses"]["semantic_scholar"][row["paper_id"]] = body

    if not args.skip_openalex:
        api_key = os.getenv("OPENALEX_API_KEY")
        raw["openalex_authentication_mode"] = "api_key" if api_key else "anonymous"
        common_params = {"api_key": api_key} if api_key else {}
        doi_rows = [row for row in sample if row["doi"]]
        for row in doi_rows:
            response, latency, attempts, metering_usd, metered_attempts = request_with_retry(
                client,
                "GET",
                "https://api.openalex.org/works/doi:" + quote(row["doi"], safe=""),
                params={
                    **common_params,
                    "select": "id,title,publication_year,abstract_inverted_index,doi",
                },
            )
            item = response.json() if response.status_code == 200 else None
            abstract = openalex_abstract(item.get("abstract_inverted_index")) if item else None
            raw["calls"]["openalex"].append(
                {
                    "operation": "doi_singleton",
                    "items": 1,
                    "status": response.status_code,
                    "latency_ms": latency * 1000,
                    "attempts": attempts,
                    "server_reported_api_budget_metering_usd": metering_usd,
                    "metered_network_attempts": metered_attempts,
                }
            )
            observations[row["paper_id"]].update(
                openalex_operation="doi_singleton",
                openalex_status=response.status_code,
                openalex_latency_ms=latency * 1000,
                openalex_attempts=attempts,
                openalex_match=bool(item),
                openalex_has_abstract=bool(abstract),
                openalex_abstract_jaccard=jaccard(row["snapshot_abstract"], abstract),
                openalex_server_reported_api_budget_metering_usd=metering_usd,
            )
            raw["responses"]["openalex"][row["paper_id"]] = item or response.text[:500]

        non_doi_rows = [row for row in sample if not row["doi"]]
        for index, row in enumerate(non_doi_rows):
            if index:
                time.sleep(0.12)  # stays below the documented 100-RPS ceiling
            response, latency, attempts, metering_usd, metered_attempts = request_with_retry(
                client,
                "GET",
                "https://api.openalex.org/works",
                params={
                    **common_params,
                    "search": row["title"],
                    "filter": f"publication_year:{row['year']}",
                    "per_page": 5,
                    "select": "id,title,publication_year,abstract_inverted_index,doi",
                },
            )
            body = response.json() if response.status_code == 200 else {}
            item = next(
                (
                    candidate
                    for candidate in body.get("results", [])
                    if normalize_title(candidate.get("title")) == normalize_title(row["title"])
                ),
                None,
            )
            abstract = openalex_abstract(item.get("abstract_inverted_index")) if item else None
            raw["calls"]["openalex"].append(
                {
                    "operation": "title_search",
                    "items": 1,
                    "status": response.status_code,
                    "latency_ms": latency * 1000,
                    "attempts": attempts,
                    "server_reported_api_budget_metering_usd": metering_usd,
                    "metered_network_attempts": metered_attempts,
                }
            )
            observations[row["paper_id"]].update(
                openalex_operation="title_search",
                openalex_status=response.status_code,
                openalex_latency_ms=latency * 1000,
                openalex_attempts=attempts,
                openalex_match=bool(item),
                openalex_has_abstract=bool(abstract),
                openalex_abstract_jaccard=jaccard(row["snapshot_abstract"], abstract),
                openalex_server_reported_api_budget_metering_usd=metering_usd,
            )
            raw["responses"]["openalex"][row["paper_id"]] = body

    rows = [observations[row["paper_id"]] for row in sample]
    summary: dict[str, object] = {
        "summary_schema_version": 2,
        "started_at_utc": started_at_utc,
        "completed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "snapshot_sha256": SNAPSHOT_SHA256,
        "sample_size": len(sample),
        "operation": "known-record alignment and abstract-field availability",
        "conditioning": "sampled from TopVenues records with non-empty abstracts",
        "logical_http_request_definition": (
            "One protocol operation; retries are counted separately as network attempts."
        ),
        "latency_scope": (
            "Retained final-attempt response latency; excludes deliberate pacing, "
            "retry backoff, and earlier failed attempts."
        ),
        "topvenues": {
            "record_match_n": sum(row.get("topvenues_match", False) for row in rows),
            "abstract_n": sum(row.get("topvenues_has_abstract", False) for row in rows),
            "logical_http_requests": 0,
            "network_attempts_including_retries": 0,
            "local_sql_lookup_trials": len(local_latencies),
            "latency_median_ms": statistics.median(local_latencies),
            "latency_p95_ms": percentile_95(local_latencies),
        },
    }
    for prefix in ("dblp", "s2", "openalex"):
        if not any(f"{prefix}_status" in row for row in rows):
            continue
        calls = raw["calls"][prefix]
        service_summary = summarize_service(prefix, calls, rows)
        if prefix == "openalex":
            service_summary["authentication_mode"] = raw.get("openalex_authentication_mode")
        summary[prefix] = service_summary

    output_dir.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({field for row in rows for field in row})
    with (output_dir / "pilot_observations.csv").open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    (output_dir / "pilot_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    with gzip.open(output_dir / "pilot_raw_responses.json.gz", "wt", encoding="utf-8") as target:
        json.dump(raw, target, ensure_ascii=False)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    temporary.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
