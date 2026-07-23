"""Summarize the completed manual audit of abstract validity.

One author labeled every record in the frozen 200-record sample against the
publisher sources. This program recomputes the reported figures from those
labels: the overall validity rate with an exact binomial interval, the
per-venue breakdown, and the population-weighted estimate.

It labels nothing itself and reaches no network. Exit code 0 when the labels
are complete and internally consistent, 1 otherwise.
"""

from __future__ import annotations

import argparse
import collections
import csv
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
LABELS = HERE / "manual_labels.csv"
SAMPLE = HERE / "sample.csv"

VALID = "valid"
DEFECTS = ("truncated", "contaminated", "wrong_paper", "not_abstract")
UNDECIDED = "uncertain"


def _binomial_cdf(successes: int, trials: int, probability: float) -> float:
    """P(X <= successes) for X ~ Binomial(trials, probability)."""
    if successes < 0:
        return 0.0
    return sum(
        math.comb(trials, i) * probability**i * (1 - probability) ** (trials - i)
        for i in range(successes + 1)
    )


def _solve(predicate, iterations: int = 300) -> float:
    """Bisect [0, 1] for the boundary where predicate stops holding."""
    low, high = 0.0, 1.0
    for _ in range(iterations):
        middle = (low + high) / 2
        if predicate(middle):
            low = middle
        else:
            high = middle
    return low


def exact_interval(successes: int, total: int, alpha: float = 0.05) -> tuple[float, float]:
    """Clopper-Pearson bounds, computed directly so no numerical stack is needed.

    Inverting the binomial tails keeps this exact rather than approximate: a
    normal approximation would widen the interval and disagree with the figure
    the paper reports.
    """
    lower = 0.0
    if successes > 0:
        lower = _solve(lambda p: 1 - _binomial_cdf(successes - 1, total, p) < alpha / 2)
    upper = 1.0
    if successes < total:
        upper = _solve(lambda p: _binomial_cdf(successes, total, p) > alpha / 2)
    return lower, upper


def load_rows(path: Path) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8") as handle:
        return {row["paper_id"]: row for row in csv.DictReader(handle)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true", help="print only the headline rate")
    args = parser.parse_args()

    labels = load_rows(LABELS)
    sample = load_rows(SAMPLE)

    unlabeled = [pid for pid, row in labels.items() if not row["label"].strip()]
    if unlabeled:
        print(f"{len(unlabeled)} records carry no label; the audit is incomplete.", file=sys.stderr)
        return 1

    without_evidence = [pid for pid, row in labels.items() if not row["evidence_url"].strip()]
    if without_evidence:
        print(f"{len(without_evidence)} records carry no evidence URL.", file=sys.stderr)
        return 1

    counts = collections.Counter(row["label"] for row in labels.values())
    total = sum(counts.values())
    decided = total - counts[UNDECIDED]
    valid = counts[VALID]
    low, high = exact_interval(valid, decided)

    print(
        f"Intrinsic abstract validity: {valid}/{decided} = {valid / decided:.1%} "
        f"(exact 95% interval {low:.1%} to {high:.1%})"
    )
    if args.quiet:
        return 0

    print("\nLabel distribution")
    for label, count in counts.most_common():
        print(f"  {label:<22}{count:>4}")

    print("\nBy venue")
    per_venue: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0])
    for paper_id, row in labels.items():
        venue = sample[paper_id]["event"]
        per_venue[venue][1] += 1
        if row["label"] == VALID:
            per_venue[venue][0] += 1

    weighted_numerator = weighted_denominator = 0.0
    for venue, (venue_valid, venue_total) in sorted(per_venue.items(), key=lambda item: -item[1][1]):
        population = next(
            int(row["venue_population_n"]) for row in sample.values() if row["event"] == venue
        )
        rate = venue_valid / venue_total
        weighted_numerator += population * rate
        weighted_denominator += population
        print(f"  {venue:<44}{venue_valid:>3}/{venue_total:<4}{rate:>7.1%}  (population {population})")

    print(f"\nPopulation-weighted estimate: {weighted_numerator / weighted_denominator:.1%}")

    defective = sum(counts[label] for label in DEFECTS)
    repaired = sum(1 for row in labels.values() if row["corrected_abstract"].strip())
    print(f"Records with a defect: {defective}; of these, {repaired} carry the corrected text.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
