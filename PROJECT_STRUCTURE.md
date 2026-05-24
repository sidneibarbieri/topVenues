# TopVenues — Project Structure

The repository is organised around three purposes: the runnable tool,
the curated research artifact, and active paper workspaces.

## Runtime Artifact

| Path | Purpose |
|------|---------|
| `src/` | collection, enrichment, persistence, export, CLI |
| `web/` | Streamlit interface |
| `tests/` | pytest coverage for core behavior (250 tests) |
| `data/dataset/papers.db.gz` | committed compressed SQLite snapshot |
| `data/dataset/master_dataset.csv` | derived CSV export of the frozen snapshot |
| `config.yaml` | venue and pipeline configuration |
| `scripts/` | reproducibility, claim verification, ad-hoc maintenance |
| `Dockerfile`, `docker-compose.yml` | reproducible execution environment |
| `reproduce.sh` | single-command end-to-end verification |

## Evaluation Documents

| Path | Purpose |
|------|---------|
| `README.md` | primary entry point for users |
| `ARTIFACT_README.md` | artifact overview for evaluation |
| `REVIEWER_GUIDE.md` | how to verify each headline claim |

## Paper Workspaces

Manuscript drafts live under `papers/`, a local writing workspace that is
excluded from the public artifact so the released code and corpus remain
independent of any specific manuscript or venue.
