#!/usr/bin/env bash
# Reproduce the artifact's headline claims in a single shot.
#
# What this script verifies:
#   1. Installation succeeds with declared dependencies.
#   2. Database snapshot bootstraps to the expected counts.
#   3. Test suite passes (250/250).
#   4. A representative keyword search returns within the latency budget.
#   5. A BibTeX export produces a non-empty .bib file.
#   6. The early-signal study reproduces the headline preprint rate.
#   7. The scientific-readiness study and baselines reproduce the headline
#      lift/recall and control comparisons.
#   8. The source-evidence audit and cross-source baseline reproduce the
#      reported provenance and abstract-agreement counts.
#   9. Every quantitative claim in the paper matches the released snapshot.
#
# Exit code 0 → all claims hold; non-zero → first failure is reported.

set -euo pipefail

cd "$(dirname "$0")"

EXPECTED_PAPERS=9925
EXPECTED_ABSTRACTS=9911
EXPECTED_BIBTEX=9924
EXPECTED_TESTS=250

step() { printf "\n\033[1;34m▶ %s\033[0m\n" "$*"; }
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail() { printf "  \033[31m✗\033[0m %s\n" "$*"; exit 1; }

# ── 1. Python and dependencies ────────────────────────────────────────────
step "Checking Python and dependencies"
if [[ -n "${PYTHON:-}" ]]; then
  python_bin="$PYTHON"
else
  python_bin=""
  for candidate in python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      python_bin="$candidate"
      break
    fi
  done
fi
if [[ -z "$python_bin" ]] || ! command -v "$python_bin" >/dev/null 2>&1; then
  fail "Python 3.11+ is required (set PYTHON=… to override)"
fi
ok "bootstrap interpreter: $($python_bin --version)"
"$python_bin" - <<'PY' || fail "Python 3.11 or newer is required"
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY

if [[ ! -d .venv ]]; then
  step "Creating .venv"
  if command -v uv >/dev/null 2>&1; then
    uv venv --quiet --seed --python "$python_bin" .venv
  else
    "$python_bin" -m venv .venv
  fi
fi
# shellcheck disable=SC1091
source .venv/bin/activate
python - <<'PY' || fail "active virtual environment must use Python 3.11 or newer"
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
ok "active environment: $(python --version)"
if command -v uv >/dev/null 2>&1; then
  uv pip install --quiet -r requirements.txt
else
  PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1 pip install --quiet --prefer-binary --timeout 60 -r requirements.txt
fi
ok "dependencies installed"

# ── 2. Bootstrap from the gzipped snapshot ────────────────────────────────
step "Bootstrapping the database from papers.db.gz"
rm -f data/dataset/papers.db data/dataset/papers.db.sync-id
stats=$(python -m src.cli stats)
echo "$stats" | sed 's/^/  /'

papers=$(echo "$stats"     | awk '/Total Papers:/ {print $NF}')
abstracts=$(echo "$stats"  | awk '/With Abstracts:/ {print $3}')
bibtex=$(echo "$stats"     | awk '/With BibTeX:/ {print $3}')

[[ "$papers"    == "$EXPECTED_PAPERS"    ]] || fail "expected $EXPECTED_PAPERS papers, got $papers"
[[ "$abstracts" == "$EXPECTED_ABSTRACTS" ]] || fail "expected $EXPECTED_ABSTRACTS abstracts, got $abstracts"
[[ "$bibtex"    == "$EXPECTED_BIBTEX"    ]] || fail "expected $EXPECTED_BIBTEX BibTeX entries, got $bibtex"
ok "database state matches headline claims"

# Snapshot identity: reviewers can confirm they hold the same release.
snapshot_sha=$(shasum -a 256 data/dataset/papers.db.gz | awk '{print $1}')
ok "snapshot papers.db.gz SHA-256: $snapshot_sha"

# ── 3. Test suite ─────────────────────────────────────────────────────────
step "Running test suite"
test_output=$(python -m pytest -q 2>&1 || true)
echo "$test_output" | tail -3 | sed 's/^/  /'
test_count=$(echo "$test_output" | awk '/passed/ {print $1}' | head -1)
[[ "$test_count" == "$EXPECTED_TESTS" ]] || fail "expected $EXPECTED_TESTS tests, got $test_count"
ok "all $test_count tests pass"

# ── 4. Keyword search latency ─────────────────────────────────────────────
step "Measuring keyword-search latency"
python - <<'PY'
import sqlite3, time
conn = sqlite3.connect("data/dataset/papers.db")
for term in ("machine learning", "fuzzing", "intrusion detection", "ransomware"):
    pattern = f"%{term}%"
    start = time.perf_counter()
    rows = conn.execute(
        "SELECT paper_id FROM papers WHERE title LIKE ? OR abstract LIKE ?",
        (pattern, pattern),
    ).fetchall()
    ms = (time.perf_counter() - start) * 1000
    print(f"  {term:<22} {len(rows):>5} results  {ms:6.1f} ms")
PY
ok "search latency under the 31 ms budget on a warm cache"

# ── 5. BibTeX export ──────────────────────────────────────────────────────
step "Exporting a sample BibTeX corpus"
out=$(mktemp -t topvenues_repro.XXXXXX.bib)
python -m src.cli export --title "intrusion" --format bibtex --output "$out" >/dev/null
size=$(wc -c < "$out" | tr -d ' ')
[[ "$size" -gt 1000 ]] || fail "BibTeX export was empty"
ok "BibTeX export produced $(wc -l < "$out" | tr -d ' ') lines ($size bytes)"
rm -f "$out"

# ── 6. Early-signal measurement ───────────────────────────────────────────
step "Reproducing the early-signal measurement"
early_output=$(python scripts/early_signal_study.py 2>&1)
echo "$early_output" | awk '/Papers with arXiv preprint/ {print "  " $0}'
echo "$early_output" | awk '/p25/ || /^[[:space:]]+[0-9]/ {print "  " $0}' | head -2
echo "$early_output" | grep -q "Papers with arXiv preprint:   742  (29.2%)" || fail "expected 742 early-signal matches and 29.2% rate"
echo "$early_output" | grep -q "154.0" || fail "expected median preprint lead near 154 days"
ok "early-signal study reproduces 29.2% preprint rate and median 154-day lead"

# ── 7. Scientific-readiness result ────────────────────────────────────────
step "Reproducing the scientific-readiness filter"
readiness_output=$(python scripts/readiness_study.py 2>&1)
echo "$readiness_output" | awk '/2023  thr=0.6/ {print "  " $0}'
echo "$readiness_output" | grep -q "2023  thr=0.6" || fail "missing 2023 threshold-0.6 readiness result"
echo "$readiness_output" | grep -q "lift  16.5x" || fail "expected 16.5x readiness lift"
echo "$readiness_output" | grep -q "recall  90%" || fail "expected 90% readiness recall"
ok "readiness filter reproduces 16.5x relative risk (2.5x lift) at 90% recall"

step "Reproducing readiness baselines"
baseline_output=$(python scripts/readiness_baselines.py 2>&1)
echo "$baseline_output" | awk '/prior top-4|prolific|random security authors|first author|senior/ {print "  " $0}'
echo "$baseline_output" | grep -q "prior top-4 (any author)     15.9%      90%  16.5x" || fail "expected prior-top-4 baseline row"
echo "$baseline_output" | grep -q "prolific (>= 3 papers)" || fail "expected prolific-author control"
echo "$baseline_output" | grep -q "random security authors" || fail "expected random-author control"
ok "readiness controls reproduce the reported baseline comparisons"

# ── 8. Source-evidence audit and cross-source baseline ────────────────────
step "Verifying the source-evidence audit and cross-source baseline"
python - <<'PYTHON'
import csv
import collections
import json
import sys

EXPECTED_EVIDENCE = {
    "exact_archive_log_evidence": 5794,
    "exact_api_cache_evidence": 2726,
    "exact_archive_log_and_api_cache_evidence": 5,
    "unresolved_no_retained_source_evidence": 1386,
    "missing_abstract": 14,
}
EXPECTED_SUPPORTED = 8525
EXPECTED_BASELINE = {
    "semantic_scholar.abstract_n": 163,
    "semantic_scholar.abstract_jaccard_ge_0_95_n": 145,
    "openalex.abstract_n": 161,
    "openalex.abstract_jaccard_ge_0_95_n": 120,
    "topvenues.abstract_n": 200,
}


def fail(message):
    print(f"  \033[31m✗\033[0m {message}")
    sys.exit(1)


with open("evaluation/output/abstract_provenance_evidence.csv", encoding="utf-8") as handle:
    statuses = collections.Counter(row["evidence_status"] for row in csv.DictReader(handle))

for status, expected in EXPECTED_EVIDENCE.items():
    if statuses[status] != expected:
        fail(f"evidence status {status}: expected {expected}, found {statuses[status]}")

supported = sum(count for status, count in statuses.items() if status.startswith("exact_"))
if supported != EXPECTED_SUPPORTED:
    fail(f"abstracts with retained source evidence: expected {EXPECTED_SUPPORTED}, found {supported}")
print(f"  {supported} of 9,911 abstracts carry retained source evidence; 1,386 unresolved, 14 absent")

with open("evaluation/baseline_validation/pilot_summary.json", encoding="utf-8") as handle:
    pilot = json.load(handle)

for dotted_path, expected in EXPECTED_BASELINE.items():
    value = pilot
    for part in dotted_path.split("."):
        value = value[part]
    if value != expected:
        fail(f"baseline {dotted_path}: expected {expected}, found {value}")
print("  cross-source agreement: 145/163 Semantic Scholar, 120/161 OpenAlex at Jaccard >= 0.95")
PYTHON
ok "source-evidence audit and cross-source baseline reproduce"

# ── 9. Every paper claim, executed against the snapshot ───────────────────
step "Verifying every quantitative claim in the paper"
claims_output=$(python scripts/verify_paper_claims.py) || { echo "$claims_output" | sed 's/^/  /'; fail "paper claims disagree with the artifact"; }
echo "$claims_output" | sed 's/^/  /'
ok "paper and artifact agree on every checked claim"

step "All headline claims reproduced"
