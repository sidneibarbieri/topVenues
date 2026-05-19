# topVenues — Project Structure

The repository is organised around three purposes: the runnable tool,
the curated research artifact, and active paper workspaces.

## Runtime Artifact

| Path | Purpose |
|------|---------|
| `src/` | collection, enrichment, persistence, export, CLI |
| `web/` | Streamlit interface |
| `tests/` | pytest coverage for core behaviour (166 tests) |
| `data/dataset/papers.db.gz` | committed compressed SQLite snapshot |
| `config.yaml` | venue and pipeline configuration |
| `scripts/` | reproducibility, claim verification, ad-hoc maintenance |
| `Dockerfile`, `docker-compose.yml` | reproducible execution environment |
| `reproduce.sh` | single-command end-to-end verification |

## Reviewer Documents

| Path | Purpose |
|------|---------|
| `README.md` | primary entry point for users |
| `ARTIFACT_README.md` | reviewer-oriented artifact overview |
| `REVIEWER_GUIDE.md` | how to verify each headline claim |

## Paper Workspaces

| Path | Purpose |
|------|---------|
| `papers/sbseg2026-main/` | full-paper submission (Trilha Principal) |
| `papers/sbseg2026-tools/` | tool-paper submission (Salão de Ferramentas) |
| `papers/sbseg2026-submission-strategy.md` | submission policy notes |

These directories are local writing workspaces and are excluded from
the public artifact distribution to preserve double-blind review.
