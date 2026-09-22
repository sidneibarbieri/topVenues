#!/usr/bin/env bash
# Reproduce one immutable TopVenues profile.

set -euo pipefail

cd "$(dirname "$0")"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$PWD/.cache/uv}"

profile="${TOPVENUES_PROFILE:-security-20-v4}"
skip_install=false

usage() {
  printf 'Usage: %s [--profile security-20|security-20-v2|security-20-v3|security-20-v4] [--skip-install]\n' "$0"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      [[ $# -ge 2 ]] || { usage >&2; exit 2; }
      profile="$2"
      shift 2
      ;;
    --skip-install)
      skip_install=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$profile" in
  security-20|security-20-v2|security-20-v3|security-20-v4) ;;
  *)
    printf 'Unknown profile: %s\n' "$profile" >&2
    exit 2
    ;;
esac
export TOPVENUES_PROFILE="$profile"

step() { printf "\n\033[1;34m▶ %s\033[0m\n" "$*"; }
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail() { printf "  \033[31m✗\033[0m %s\n" "$*"; exit 1; }

step "Checking Python and dependencies"
if [[ -n "${PYTHON:-}" ]]; then
  python_bin="$PYTHON"
else
  python_bin=""
  for candidate in python3.14 python3.13 python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      python_bin="$candidate"
      break
    fi
  done
fi
if [[ -z "$python_bin" ]] || ! command -v "$python_bin" >/dev/null 2>&1; then
  fail "Python 3.11–3.14 is required (set PYTHON=… to override)"
fi
"$python_bin" - <<'PY' || fail "Python 3.11–3.14 is required"
import sys
raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 15) else 1)
PY

if [[ ! -d .venv ]]; then
  if [[ "$skip_install" == true ]]; then
    fail "--skip-install requires an existing .venv"
  fi
  step "Creating .venv"
  if command -v uv >/dev/null 2>&1; then
    uv venv --quiet --seed --python "$python_bin" .venv
  else
    "$python_bin" -m venv .venv
  fi
fi
# shellcheck disable=SC1091
source .venv/bin/activate
python - <<'PY' || fail "active virtual environment must use Python 3.11–3.14"
import sys
raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 15) else 1)
PY
ok "active environment: $(python --version)"
ok "dependencies installed from requirements-frozen.txt (pinned and hash-checked,"
ok "  including the web interface: streamlit, altair, watchdog)"

if [[ "$skip_install" == false ]]; then
  if command -v uv >/dev/null 2>&1; then
    uv pip install --quiet --require-hashes -r requirements-frozen.txt
  else
    PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1 \
      pip install --quiet --prefer-binary --timeout 60 \
        --require-hashes -r requirements-frozen.txt
  fi
  ok "dependencies installed"
else
  ok "dependency installation skipped explicitly"
fi

step "Verifying immutable profile inputs"
manifest_row=$(python scripts/verify_profile_snapshot.py --profile "$profile" --machine) || \
  fail "profile manifest verification failed"
IFS=$'\t' read -r snapshot_path workspace_data_dir expected_papers expected_abstracts \
  expected_bibtex expected_snapshot_sha <<< "$manifest_row"
[[ -n "$snapshot_path" && -n "$workspace_data_dir" && -n "$expected_snapshot_sha" ]] || \
  fail "profile verifier returned an incomplete machine record"
ok "profile $profile: $snapshot_path (sha256=$expected_snapshot_sha)"

step "Refreshing the disposable profile workspace"
python -m src.cli --profile "$profile" refresh-db >/dev/null
stats=$(python -m src.cli --profile "$profile" stats)
echo "$stats" | sed 's/^/  /'

papers=$(echo "$stats"    | awk '/Total Papers:/ {print $NF}')
abstracts=$(echo "$stats" | awk '/With Abstracts:/ {print $3}')
bibtex=$(echo "$stats"    | awk '/With BibTeX:/ {print $3}')
[[ "$papers" == "$expected_papers" ]] || \
  fail "expected $expected_papers papers, got $papers"
[[ "$abstracts" == "$expected_abstracts" ]] || \
  fail "expected $expected_abstracts abstracts, got $abstracts"
[[ "$bibtex" == "$expected_bibtex" ]] || \
  fail "expected $expected_bibtex BibTeX entries, got $bibtex"
python scripts/verify_claims.py --profile "$profile" || fail "profile counts changed"
ok "workspace matches the immutable $profile manifest"

step "Running the test suite"
test_output=$(python -m pytest -q 2>&1 || true)
echo "$test_output" | tail -3 | sed 's/^/  /'
echo "$test_output" | grep -qE "failed|error" && fail "test suite did not pass cleanly"
test_count=$(echo "$test_output" | awk '/passed/ {print $1}' | head -1)
[[ -n "$test_count" ]] || fail "no tests ran"
ok "all $test_count tests pass"

step "Starting the research interface"
python scripts/smoke_web_app.py || fail "the web interface did not render"
ok "Streamlit interface rendered without raising"

step "Verifying the claims stated in the paper"
python scripts/verify_paper_claims.py --profile "$profile" || fail "a paper claim does not hold"

step "Reproducing Table 2 of the paper"
python scripts/reproduce_paper_table2.py --profile "$profile" || fail "Table 2 does not reproduce"

step "Comparing the ranked search against the substring baseline"
python scripts/benchmark_search.py --profile "$profile" --trials 11 || \
  fail "search benchmark failed"
ok "baseline substring search and ranked BM25 search both returned results"

step "Exporting a sample BibTeX corpus"
sample_bib=$(mktemp -t topvenues_repro.XXXXXX.bib)
trap 'rm -f "$sample_bib"' EXIT
python -m src.cli --profile "$profile" export \
  --title "intrusion" --format bibtex --output "$sample_bib" >/dev/null
sample_size=$(wc -c < "$sample_bib" | tr -d ' ')
[[ "$sample_size" -gt 1000 ]] || fail "BibTeX export was empty"
ok "BibTeX export produced $(wc -l < "$sample_bib" | tr -d ' ') lines ($sample_size bytes)"
rm -f "$sample_bib"
trap - EXIT

evidence="evidence-$profile-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
  echo "TopVenues reproduction evidence"
  echo "profile:     $profile"
  echo "commit:      $(git rev-parse --short HEAD 2>/dev/null || echo 'not a git checkout')"
  echo "date (UTC):  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "os:          $(uname -srm)"
  echo "python:      $(python --version 2>&1)"
  echo "snapshot:    sha256=$expected_snapshot_sha"
  echo
  echo "--- paper claims ---"
  python scripts/verify_paper_claims.py --profile "$profile"
  echo
  echo "--- Table 2 ---"
  python scripts/reproduce_paper_table2.py --profile "$profile"
  echo
  echo "--- test suite ---"
  python -m pytest -q 2>&1 | tail -3
} > "$evidence" 2>&1
ok "execution evidence written to $evidence"

step "Profile $profile reproduced successfully"
