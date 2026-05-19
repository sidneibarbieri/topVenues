# topVenues — Artifact README

This file is the reviewer-oriented entry point for the topVenues artifact.

## Artifact Summary

topVenues helps researchers construct and reproduce venue-bounded
cybersecurity literature reviews. It bundles a curated corpus, a
collection pipeline, a SQLite database layer with monotonic upserts, a
command-line interface, a Streamlit web interface, BibTeX generation,
and 166 automated tests.

## Repository Layout

| Path | Purpose |
|------|---------|
| `src/` | pipeline, database, models, extractors, CLI |
| `web/` | Streamlit interface |
| `tests/` | pytest suite (166 tests) |
| `data/dataset/papers.db.gz` | compressed SQLite snapshot, 15 MB |
| `scripts/` | reproducibility and verification scripts |
| `reproduce.sh` | one-command verification of all headline claims |
| `Dockerfile`, `docker-compose.yml` | self-contained execution environment |

## Badges Targeted

For SBSeg artifact evaluation:

- **Available** — public source, snapshot, documentation, MIT licence.
- **Functional** — CLI, web UI and tests execute locally.
- **Reproducible** — `reproduce.sh` validates every headline claim from
  a fresh clone in under a minute.
- **Sustainable** — modular Python package, 166 tests, declared schema
  migrations, and incremental snapshot updates.

## Requirements

- Python 3.11 or 3.12 (or Docker).
- macOS or Linux recommended.
- Internet access is **not** required for inspection — the committed
  `papers.db.gz` snapshot makes the corpus available offline.

## Reviewer Quickstart (3 paths)

### Path A — one-command reproduction (recommended)

```bash
git clone https://github.com/sidneibarbieri/topVenues.git
cd topVenues
./reproduce.sh
```

`reproduce.sh` installs dependencies in `.venv/`, bootstraps the SQLite
database from `papers.db.gz`, runs the 166-test suite, measures query
latency on five representative cybersecurity topics, and exports a
sample BibTeX file. It prints `✓ All headline claims reproduced` on
success and the first failure otherwise.

### Path B — Docker

```bash
docker compose up
# open http://localhost:8501
```

Or any CLI command:

```bash
docker compose run --rm app python -m src.cli stats
docker compose run --rm app python -m pytest -q
```

### Path C — manual

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
python -m src.cli stats
streamlit run web/app.py
```

## Headline Claims

The artifact ships with a populated SQLite snapshot. After bootstrap:

| Claim | Verification command |
|-------|----------------------|
| 9,925 papers | `python -m src.cli stats` |
| 9,911 abstracts (99.86 %) | `python -m src.cli stats` |
| 9,924 BibTeX entries (99.99 %) | `python -m src.cli stats` |
| 11 venues, 2017–2026 | `python -m src.cli stats` |
| 166 tests pass | `python -m pytest -q` |
| Search latency below 31 ms | `./reproduce.sh` |

## Reproducing the Full Collection Pipeline

The committed snapshot is the recommended reproduction path because it
is offline and deterministic. To rebuild the corpus from scratch
(requires network access and several minutes):

```bash
python -m src.cli download
python -m src.cli consolidate
python -m src.cli extract
python -m src.cli bibtex-from-dump
python -m src.cli write-snapshot   # rewrites papers.db.gz
```

Each stage is idempotent: re-running preserves existing enriched
records (the database upsert uses `COALESCE(papers.abstract,
excluded.abstract)`, guaranteeing monotonic enrichment).

## Network Footprint

| Operation | Network required? |
|-----------|-------------------|
| Bootstrap from `papers.db.gz` | No |
| Stats, search, export | No |
| Test suite | No |
| Web interface | No |
| `download` / `extract` / `bibtex*` | Yes (DBLP and open scholarly APIs) |

Reviewers can complete every claim verification without network access.

## Licence

MIT — see `LICENSE`.
