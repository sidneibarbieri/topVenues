# TopVenues — Reviewer Guide

This guide is the practical entry point for reviewers who want to
inspect the TopVenues artifact.

## What TopVenues Is

TopVenues is an open-source, reproducible literature-review substrate
for cybersecurity. It combines a tool and a methodology for constructing,
refreshing, querying, and exporting venue-bounded paper collections.

## What To Inspect First

1. `ARTIFACT_README.md` — artifact overview and badge mapping.
2. `reproduce.sh` — single-shot verification of every headline claim.
3. `data/dataset/papers.db.gz` — committed corpus snapshot.
4. `data/dataset/arxiv_cs_cr_2022_2026.jsonl.gz` — committed preprint snapshot for the measurement studies.
5. `src/` and `web/` — implementation.
6. `tests/` — executable checks (250 tests).

## Minimal Verification

```bash
bash reproduce.sh
```

Expected output: `✓ All headline claims reproduced`.

The script verifies:

- 250 tests pass after dependency installation;
- the SQLite snapshot bootstraps to 9,925 papers, 9,911 abstracts and
  9,924 BibTeX entries;
- keyword search returns results in under 31 ms on representative
  queries;
- BibTeX export produces a non-empty `.bib` file ready for LaTeX use;
- the scientific-readiness study reproduces the reported 16.5x relative risk at
  90% recall.

Total runtime is well under a minute on a 2020-or-later laptop.

## Web Review Path

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-web.txt
streamlit run web/app.py
```

If the shell prompt already ends in `TopVenues`, skip `cd TopVenues`.
The first page, **Overview**, is the shortest evaluation path: it exposes the
claim set, the reproduction command, artifact-badge evidence and the two
measurement findings. **Search** is for corpus inspection and reference
export; **Insights** is for coverage and scope checks; **Pipeline** is only
for refreshing the corpus from live sources.

## Alternative Verification Paths

- Docker: `docker compose up` then `http://localhost:8501`.
- Manual: `pip install -r requirements.txt -r requirements-web.txt` + `python -m pytest -q` +
  `python -m src.cli stats`.

## Positioning

TopVenues is a tool-supported methodology, not a generic paper generator or
paper search engine. Its scientific value is the reproducible construction and
preservation of a declared cybersecurity collection, auditable as a single
file in version control.

## Common Questions

**Q: Does the artifact require publisher credentials?**
No. The committed corpus and preprint snapshots make claim verification
independent of publisher portals; only the fresh-collection pipeline
(`download`, `extract`, `bibtex-from-dump`) calls external services.

**Q: Is the dataset volatile?**
No. The snapshot is versioned with the source code. Re-running
verification on the same commit always produces the same numbers.

**Q: How do I extend the collection to a new venue?**
Add one entry to `config.yaml`; no code change is required.
