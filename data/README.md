# Data Directory

This directory contains local data used by the topVenues artifact.

## Canonical Dataset Snapshot

- `dataset/papers.db`: current SQLite source of truth for local search,
  statistics, and paper claims.
- `dataset/master_dataset.csv`: derived CSV export.
- `dataset/master_dataset.rds`: derived RDS export.

Current verified snapshot:

- 9,925 papers.
- 9,911 papers with abstracts.
- 9,924 papers with BibTeX.

## Reproducibility Inputs

- `dblp/`: DBLP XML dump and DTD used for offline BibTeX enrichment.
- `json/`: DBLP venue/year JSON downloads.
- `cache/`: abstract-fetch cache.
- `checkpoints/`: long-running pipeline checkpoints.

## Archive

- `archive/`: historical data package retained for traceability. Do not cite it
  as the current corpus unless it is explicitly revalidated.

## Maintenance Rule

Generated logs and Python caches should not be kept here. Dataset snapshots,
DBLP inputs, JSON downloads, cache files, and checkpoints are retained because
they support repeatability and artifact evaluation.
