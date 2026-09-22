# TopVenues SBSeg-SF Research Artifact (Frozen)

> **Historical document.** Written for the tools-track artifact evaluation. The
> release it describes lives in the archived repository
> `sidneibarbieri/topvenues-tool`, so the commands below still work as written.
> In this repository the same claims reproduce with
> `bash reproduce.sh --profile security-20`; see [docs/PAPERS.md](docs/PAPERS.md).

TopVenues materializes a declared cybersecurity-literature scope as a local, versioned SQLite corpus. This document packages the frozen `security-20` profile used by the accepted SBSeg-SF paper. The current tool uses `security-20-v4` with a separate manifest and evidence boundary.

## Artifact identity

| Property | Value |
| --- | --- |
| Release tag | `v1.0.1` |
| Profile | `security-20` |
| Records | 20,305 |
| Abstract-enriched records | 17,491 |
| BibTeX entries | 20,305 |
| Automated tests | 243 |
| Snapshot gzip SHA-256 | `5a35bd6e3ec6845a0fde4cc3d6aa05b1db04e511cb39e783eeaee2cea7493b08` |

## Public reviewer paths

### Linux and macOS

Requires Python 3.11 or 3.12, Git, and Bash.

```bash
git clone --branch v1.0.1 https://github.com/sidneibarbieri/topvenues-tool.git
cd topvenues-tool
bash reproduce.sh
```

This is the authoritative zero-cost validation path. It verifies the manifest, materializes the snapshot, runs the test suite, exercises search, and produces a sample export. It does not call live scholarly APIs or reproduce results from another paper.

### Native Windows

Requires Python 3.11 or 3.12 with the Python Launcher (`py`), Git, and
PowerShell.

```powershell
git clone --branch v1.0.1 https://github.com/sidneibarbieri/topvenues-tool.git
cd topvenues-tool
powershell -ExecutionPolicy Bypass -File .\reproduce.ps1
```

The script creates `.venv` and installs `requirements.txt`; if a previous
attempt created that environment with Python 3.10 or older, remove only
`.venv` before rerunning.
Do not start a second reproduction while one is running; concurrent
materialization is reported explicitly rather than risking a partial SQLite file.

## Structure

| Path | Purpose |
| --- | --- |
| `src/` | Python implementation of collection, normalization, search, exports, and profiles |
| `web/` | Local Streamlit interface |
| `tests/` | Automated regression and integrity tests |
| `scripts/verify_profile_snapshot.py` | Manifest and checksum verifier |
| `scripts/benchmark_search.py` | Host-local search exercise |
| `profiles/security-20/config.yaml` | Declared venue and year scope |
| `data/profiles/security-20/` | Immutable snapshot and manifest |
| `data/awards/` | Source-backed optional award annotations |

## Reproducibility boundary

The committed snapshot is the evidence object. Future DBLP downloads or abstract enrichment may create a new snapshot, but cannot silently modify this release. Benchmark timings are machine-dependent diagnostics, not scientific performance claims. The artifact records missing abstracts instead of deleting records or inferring their content.

## License and data terms

The code is MIT licensed. DBLP metadata and BibTeX follow DBLP's CC0 terms. Abstract text is retained with provenance for research indexing and remains subject to the terms of its original sources.
