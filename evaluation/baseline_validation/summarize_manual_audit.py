#!/usr/bin/env python3
"""Validate and summarize the frozen 200-record human abstract audit.

This program deliberately has no network or automatic-labelling code.  It
refuses to emit a result until two distinct annotators have labelled every
sampled record and all disagreements have been explicitly adjudicated.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlparse

EXPECTED_N = 200
CONFIDENCE_LEVEL = 0.95
ALPHA = 1.0 - CONFIDENCE_LEVEL
LABELS = (
    "exact",
    "correct_formatting",
    "truncated",
    "contaminated",
    "wrong_paper",
    "not_abstract",
    "unverifiable",
)
VERIFIABLE_LABELS = tuple(label for label in LABELS if label != "unverifiable")
STRICT_CORRECT_LABELS = ("exact", "correct_formatting")

SAMPLE_FIELDS = (
    "paper_id",
    "event",
    "venue_population_n",
    "venue_sample_n",
    "sampling_weight",
)
WORKSHEET_FIELDS = (
    "paper_id",
    "annotator_1_id",
    "annotator_1_label",
    "annotator_1_evidence_url",
    "annotator_1_access_date",
    "annotator_2_id",
    "annotator_2_label",
    "annotator_2_evidence_url",
    "annotator_2_access_date",
    "adjudicated_label",
    "adjudicator_id",
    "adjudication_notes",
)
REQUIRED_COMPLETION_FIELDS = (
    "annotator_1_id",
    "annotator_1_label",
    "annotator_1_evidence_url",
    "annotator_1_access_date",
    "annotator_2_id",
    "annotator_2_label",
    "annotator_2_evidence_url",
    "annotator_2_access_date",
    "adjudicated_label",
)


class AuditIncompleteError(ValueError):
    """Raised when the worksheet does not satisfy the human-audit contract."""


@dataclass(frozen=True)
class Stratum:
    event: str
    population_n: int
    sample_n: int
    weight: float


def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description=(
            "Refuse incomplete human worksheets; otherwise compute agreement, "
            "Cohen's kappa, an exact binomial interval, and a venue-weighted "
            "strict-correctness estimate."
        )
    )
    parser.add_argument("--sample", type=Path, default=here / "sample.csv")
    parser.add_argument("--worksheet", type=Path, default=here / "manual_adjudication.csv")
    parser.add_argument("--output", type=Path, default=here / "manual_audit_summary.json")
    return parser.parse_args()


def read_csv(path: Path, required_fields: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.is_file():
        raise AuditIncompleteError(f"missing CSV: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if len(fieldnames) != len(set(fieldnames)):
            raise AuditIncompleteError(f"{path.name} contains duplicate column names")
        fields = set(fieldnames)
        missing = sorted(set(required_fields) - fields)
        if missing:
            raise AuditIncompleteError(f"{path.name} is missing columns: {', '.join(missing)}")
        rows = []
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise AuditIncompleteError(
                    f"{path.name}:{line_number} has more values than the header"
                )
            rows.append({key: (value or "").strip() for key, value in row.items()})
        return rows


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def paper_ids(rows: list[dict[str, str]], name: str) -> list[str]:
    ids = [row["paper_id"] for row in rows]
    blank_n = sum(not paper_id for paper_id in ids)
    duplicates = sorted(
        paper_id for paper_id, count in Counter(ids).items() if paper_id and count > 1
    )
    problems = []
    if blank_n:
        problems.append(f"{name}: {blank_n} blank paper_id value(s)")
    if duplicates:
        problems.append(f"{name}: duplicate paper_id value(s): {', '.join(duplicates[:8])}")
    if problems:
        raise AuditIncompleteError("; ".join(problems))
    return ids


def valid_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def valid_iso_date(value: str) -> bool:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return False
    return value == parsed.isoformat()


def parse_strata(sample_rows: list[dict[str, str]]) -> dict[str, Stratum]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in sample_rows:
        grouped[row["event"]].append(row)

    strata: dict[str, Stratum] = {}
    problems: list[str] = []
    for event, rows in sorted(grouped.items()):
        try:
            populations = {int(row["venue_population_n"]) for row in rows}
            declared_samples = {int(row["venue_sample_n"]) for row in rows}
            weights = {float(row["sampling_weight"]) for row in rows}
        except ValueError:
            problems.append(f"{event}: non-numeric stratum metadata")
            continue
        if len(populations) != 1 or len(declared_samples) != 1 or len(weights) != 1:
            problems.append(f"{event}: inconsistent stratum metadata")
            continue
        population_n = populations.pop()
        sample_n = declared_samples.pop()
        weight = weights.pop()
        if sample_n != len(rows):
            problems.append(
                f"{event}: venue_sample_n={sample_n}, but sample.csv has {len(rows)} rows"
            )
        if population_n < sample_n or sample_n <= 0 or not math.isfinite(weight):
            problems.append(f"{event}: invalid population/sample sizes")
        if sample_n > 0 and math.isfinite(weight):
            expected_weight = population_n / sample_n
            if not math.isclose(weight, expected_weight, rel_tol=1e-10, abs_tol=1e-10):
                problems.append(f"{event}: sampling_weight={weight} != {population_n}/{sample_n}")
        strata[event] = Stratum(event, population_n, sample_n, weight)
    if problems:
        raise AuditIncompleteError("; ".join(problems))
    return strata


def validate_complete(
    sample_rows: list[dict[str, str]],
    worksheet_rows: list[dict[str, str]],
    expected_n: int,
) -> tuple[list[dict[str, str]], dict[str, Stratum], tuple[str, str]]:
    problems: list[str] = []
    if len(sample_rows) != expected_n:
        problems.append(f"sample.csv has {len(sample_rows)} rows; exactly {expected_n} required")
    if len(worksheet_rows) != expected_n:
        problems.append(
            f"manual_adjudication.csv has {len(worksheet_rows)} rows; exactly {expected_n} required"
        )

    sample_ids = paper_ids(sample_rows, "sample.csv")
    worksheet_ids = paper_ids(worksheet_rows, "manual_adjudication.csv")
    sample_id_set = set(sample_ids)
    worksheet_id_set = set(worksheet_ids)
    if sample_id_set != worksheet_id_set:
        missing = sorted(sample_id_set - worksheet_id_set)
        extra = sorted(worksheet_id_set - sample_id_set)
        if missing:
            problems.append(f"worksheet missing paper IDs: {', '.join(missing[:8])}")
        if extra:
            problems.append(f"worksheet has unknown paper IDs: {', '.join(extra[:8])}")

    by_id = {row["paper_id"]: row for row in worksheet_rows}
    ordered = [by_id[paper_id] for paper_id in sample_ids if paper_id in by_id]
    for field in REQUIRED_COMPLETION_FIELDS:
        blank = [row for row in ordered if not row[field]]
        if blank:
            examples = ", ".join(row["paper_id"] for row in blank[:5])
            problems.append(f"{field}: blank in {len(blank)} row(s) (e.g., {examples})")

    for field in ("annotator_1_label", "annotator_2_label", "adjudicated_label"):
        invalid = [row for row in ordered if row[field] and row[field] not in set(LABELS)]
        if invalid:
            examples = ", ".join(f"{row['paper_id']}={row[field]!r}" for row in invalid[:5])
            problems.append(f"{field}: invalid label(s) ({examples})")

    for prefix in ("annotator_1", "annotator_2"):
        url_field = f"{prefix}_evidence_url"
        bad_urls = [row for row in ordered if row[url_field] and not valid_http_url(row[url_field])]
        if bad_urls:
            examples = ", ".join(row["paper_id"] for row in bad_urls[:5])
            problems.append(
                f"{url_field}: {len(bad_urls)} invalid HTTP(S) URL(s) (e.g., {examples})"
            )
        date_field = f"{prefix}_access_date"
        bad_dates = [
            row for row in ordered if row[date_field] and not valid_iso_date(row[date_field])
        ]
        if bad_dates:
            examples = ", ".join(row["paper_id"] for row in bad_dates[:5])
            problems.append(
                f"{date_field}: {len(bad_dates)} value(s) not YYYY-MM-DD (e.g., {examples})"
            )

    annotator_1_ids = {row["annotator_1_id"] for row in ordered if row["annotator_1_id"]}
    annotator_2_ids = {row["annotator_2_id"] for row in ordered if row["annotator_2_id"]}
    if len(annotator_1_ids) != 1:
        problems.append(
            "annotator_1_id must identify one consistent author across all rows; "
            f"found {sorted(annotator_1_ids)!r}"
        )
    if len(annotator_2_ids) != 1:
        problems.append(
            "annotator_2_id must identify one consistent author across all rows; "
            f"found {sorted(annotator_2_ids)!r}"
        )
    if (
        len(annotator_1_ids) == 1
        and len(annotator_2_ids) == 1
        and annotator_1_ids == annotator_2_ids
    ):
        problems.append("annotator_1_id and annotator_2_id must be distinct authors")

    for row in ordered:
        label_1 = row["annotator_1_label"]
        label_2 = row["annotator_2_label"]
        final = row["adjudicated_label"]
        if label_1 and label_1 == label_2 and final and final != label_1:
            problems.append(
                f"paper {row['paper_id']}: adjudicated label changes an annotator "
                "agreement; record a disagreement instead of silently overriding it"
            )
        if label_1 and label_2 and label_1 != label_2:
            if not row["adjudicator_id"]:
                problems.append(f"paper {row['paper_id']}: disagreement lacks adjudicator_id")
            if not row["adjudication_notes"]:
                problems.append(f"paper {row['paper_id']}: disagreement lacks adjudication_notes")

    if problems:
        displayed = problems[:30]
        suffix = "" if len(problems) <= 30 else f"\n... plus {len(problems) - 30} more"
        raise AuditIncompleteError("\n- " + "\n- ".join(displayed) + suffix)

    strata = parse_strata(sample_rows)
    annotators = (next(iter(annotator_1_ids)), next(iter(annotator_2_ids)))
    return ordered, strata, annotators


def binomial_cdf(k: int, n: int, probability: float) -> float:
    return sum(
        math.comb(n, i) * probability**i * (1.0 - probability) ** (n - i) for i in range(k + 1)
    )


def clopper_pearson(k: int, n: int, alpha: float = ALPHA) -> tuple[float, float]:
    """Return a dependency-free equal-tailed exact binomial interval."""
    if not (0 <= k <= n) or n <= 0:
        raise ValueError("Clopper-Pearson requires 0 <= k <= n and n > 0")
    tail = alpha / 2.0

    if k == 0:
        lower = 0.0
    else:
        low, high = 0.0, k / n
        for _ in range(100):
            mid = (low + high) / 2.0
            survival = 1.0 - binomial_cdf(k - 1, n, mid)
            if survival < tail:
                low = mid
            else:
                high = mid
        lower = (low + high) / 2.0

    if k == n:
        upper = 1.0
    else:
        low, high = k / n, 1.0
        for _ in range(100):
            mid = (low + high) / 2.0
            cumulative = binomial_cdf(k, n, mid)
            if cumulative > tail:
                low = mid
            else:
                high = mid
        upper = (low + high) / 2.0

    return lower, upper


def agreement_metrics(rows: list[dict[str, str]]) -> dict[str, object]:
    agree_n = sum(row["annotator_1_label"] == row["annotator_2_label"] for row in rows)
    kappa_rows = [
        row
        for row in rows
        if row["annotator_1_label"] != "unverifiable" and row["annotator_2_label"] != "unverifiable"
    ]
    excluded_n = len(rows) - len(kappa_rows)

    kappa_value: float | None
    kappa_reason: str | None = None
    observed: float | None
    expected: float | None
    if not kappa_rows:
        observed = None
        expected = None
        kappa_value = None
        kappa_reason = "no row has two verifiable labels"
    else:
        n = len(kappa_rows)
        observed = (
            sum(row["annotator_1_label"] == row["annotator_2_label"] for row in kappa_rows) / n
        )
        counts_1 = Counter(row["annotator_1_label"] for row in kappa_rows)
        counts_2 = Counter(row["annotator_2_label"] for row in kappa_rows)
        expected = sum((counts_1[label] / n) * (counts_2[label] / n) for label in VERIFIABLE_LABELS)
        if math.isclose(expected, 1.0, abs_tol=1e-15):
            kappa_value = None
            kappa_reason = "undefined because both annotators used one category only"
        else:
            kappa_value = (observed - expected) / (1.0 - expected)

    return {
        "raw_all_categories": {
            "n": len(rows),
            "agree_n": agree_n,
            "proportion": agree_n / len(rows),
        },
        "cohens_kappa_six_verifiable_categories": {
            "included_n": len(kappa_rows),
            "excluded_if_either_unverifiable_n": excluded_n,
            "observed_agreement": observed,
            "chance_expected_agreement": expected,
            "value": kappa_value,
            "undefined_reason": kappa_reason,
        },
    }


def adjudicated_metrics(
    rows: list[dict[str, str]],
    sample_rows: list[dict[str, str]],
    strata: dict[str, Stratum],
) -> dict[str, object]:
    sample_by_id = {row["paper_id"]: row for row in sample_rows}
    label_counts = Counter(row["adjudicated_label"] for row in rows)
    verifiable = [row for row in rows if row["adjudicated_label"] != "unverifiable"]
    if not verifiable:
        raise AuditIncompleteError(
            "all adjudicated labels are unverifiable; strict correctness is undefined"
        )
    correct_n = sum(row["adjudicated_label"] in STRICT_CORRECT_LABELS for row in verifiable)
    lower, upper = clopper_pearson(correct_n, len(verifiable))

    weighted_correct = 0.0
    weighted_verifiable = 0.0
    by_venue: list[dict[str, object]] = []
    for event, stratum in sorted(strata.items()):
        venue_rows = [row for row in rows if sample_by_id[row["paper_id"]]["event"] == event]
        venue_verifiable = [row for row in venue_rows if row["adjudicated_label"] != "unverifiable"]
        venue_correct = sum(
            row["adjudicated_label"] in STRICT_CORRECT_LABELS for row in venue_verifiable
        )
        weighted_correct += stratum.weight * venue_correct
        weighted_verifiable += stratum.weight * len(venue_verifiable)
        by_venue.append(
            {
                "event": event,
                "population_n": stratum.population_n,
                "sample_n": stratum.sample_n,
                "verifiable_n": len(venue_verifiable),
                "unverifiable_n": len(venue_rows) - len(venue_verifiable),
                "strict_correct_n": venue_correct,
                "strict_correctness": (
                    venue_correct / len(venue_verifiable) if venue_verifiable else None
                ),
                "sampling_weight": stratum.weight,
            }
        )

    estimate = correct_n / len(verifiable)
    return {
        "strict_correct_labels": list(STRICT_CORRECT_LABELS),
        "label_counts": {label: label_counts[label] for label in LABELS},
        "verifiable_n": len(verifiable),
        "unverifiable_n": len(rows) - len(verifiable),
        "strict_correct_n": correct_n,
        "unweighted_strict_correctness": estimate,
        "unweighted_exact_binomial_95_ci": {
            "method": "Clopper-Pearson equal-tailed",
            "confidence_level": CONFIDENCE_LEVEL,
            "lower": lower,
            "upper": upper,
        },
        "unweighted_strict_error_rate": 1.0 - estimate,
        "unweighted_exact_binomial_95_error_ci": {
            "method": "complement of Clopper-Pearson correctness interval",
            "confidence_level": CONFIDENCE_LEVEL,
            "lower": 1.0 - upper,
            "upper": 1.0 - lower,
        },
        "venue_weighted_strict_correctness": weighted_correct / weighted_verifiable,
        "venue_weighted_correct_mass": weighted_correct,
        "venue_weighted_verifiable_mass": weighted_verifiable,
        "venue_weighted_ci": None,
        "venue_weighted_ci_note": (
            "No ordinary binomial CI is attached to the unequal-weight estimate; "
            "doing so would assert false independent-identically-distributed sampling."
        ),
        "by_venue": by_venue,
    }


def main() -> int:
    args = parse_args()
    try:
        sample_rows = read_csv(args.sample, SAMPLE_FIELDS)
        worksheet_rows = read_csv(args.worksheet, WORKSHEET_FIELDS)
        ordered, strata, annotators = validate_complete(sample_rows, worksheet_rows, EXPECTED_N)
        summary = {
            "status": "completed_human_adjudication",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "sample": {
                "path": str(args.sample.resolve()),
                "sha256": sha256_file(args.sample),
                "n": len(sample_rows),
            },
            "worksheet": {
                "path": str(args.worksheet.resolve()),
                "sha256": sha256_file(args.worksheet),
                "n": len(ordered),
            },
            "annotators": list(annotators),
            "agreement": agreement_metrics(ordered),
            "adjudicated": adjudicated_metrics(ordered, sample_rows, strata),
            "interpretation_guardrails": [
                "The sample is conditional on a non-empty snapshot abstract.",
                "Semantic Scholar and OpenAlex are not reference labels.",
                "The venue-weighted estimate is reported without an IID binomial CI.",
            ],
        }
    except AuditIncompleteError as error:
        print(
            "REFUSED: the human audit is incomplete or internally inconsistent.\n"
            f"{error}\nNo summary was written.",
            file=sys.stderr,
        )
        return 2

    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    args.output.write_text(payload, encoding="utf-8")
    print(f"Wrote completed human-audit summary: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
