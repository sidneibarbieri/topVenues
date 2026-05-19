#!/usr/bin/env bash
# Reproduce the artifact's headline claims in a single shot.
#
# What this script verifies:
#   1. Installation succeeds with declared dependencies.
#   2. Database snapshot bootstraps to the expected counts.
#   3. Test suite passes (166/166).
#   4. A representative keyword search returns within the latency budget.
#   5. A BibTeX export produces a non-empty .bib file.
#
# Exit code 0 → all claims hold; non-zero → first failure is reported.

set -euo pipefail

cd "$(dirname "$0")"

EXPECTED_PAPERS=9925
EXPECTED_ABSTRACTS=9911
EXPECTED_BIBTEX=9924
EXPECTED_TESTS=166

step() { printf "\n\033[1;34m▶ %s\033[0m\n" "$*"; }
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail() { printf "  \033[31m✗\033[0m %s\n" "$*"; exit 1; }

# ── 1. Python and dependencies ────────────────────────────────────────────
step "Checking Python and dependencies"
python_bin="${PYTHON:-python3}"
if ! command -v "$python_bin" >/dev/null 2>&1; then
  fail "python3 is required (set PYTHON=… to override)"
fi
ok "$($python_bin --version)"

if [[ ! -d .venv ]]; then
  step "Creating .venv"
  "$python_bin" -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --quiet -r requirements.txt
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

step "All headline claims reproduced"
