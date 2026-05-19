# topVenues — Reviewer Guide

This guide is the practical entry point for SBSeg reviewers who want to
inspect the artifact behind the topVenues submission.

## What topVenues Is

topVenues is an open-source, reproducible literature-review substrate
for cybersecurity. It combines a tool and a methodology for constructing,
refreshing, querying, and exporting venue-bounded paper collections.

## What To Inspect First

1. `ARTIFACT_README.md` — artifact overview and badge mapping.
2. `reproduce.sh` — single-shot verification of every headline claim.
3. `data/dataset/papers.db.gz` — committed dataset snapshot.
4. `src/` and `web/` — implementation.
5. `tests/` — executable checks (166 tests).

## Minimal Verification

```bash
./reproduce.sh
```

Expected output: `✓ All headline claims reproduced`.

The script verifies:

- 166 tests pass without network access;
- the SQLite snapshot bootstraps to 9,925 papers, 9,911 abstracts and
  9,924 BibTeX entries;
- keyword search returns results in under 31 ms on representative
  queries;
- BibTeX export produces a non-empty `.bib` file ready for LaTeX use.

Total runtime is well under a minute on a 2020-or-later laptop.

## Alternative Verification Paths

- Docker: `docker compose up` then `http://localhost:8501`.
- Manual: `pip install -r requirements.txt` + `python -m pytest -q` +
  `python -m src.cli stats`.

## Submission Positioning

topVenues is presented as a tool-supported methodology, not as a generic
paper search engine. Its scientific value is the reproducible
construction and preservation of a declared cybersecurity collection,
auditable as a single file in version control.

## Common Reviewer Questions

**Q: Does the artifact require publisher credentials?**
No. The committed `papers.db.gz` snapshot makes all verification
offline; only the fresh-collection pipeline (`download`, `extract`,
`bibtex-from-dump`) calls external services.

**Q: Is the dataset volatile?**
No. The snapshot is versioned with the source code. Re-running
verification on the same commit always produces the same numbers.

**Q: How do I extend the collection to a new venue?**
Add one entry to `config.yaml`; no code change is required.
