# Top Security Venues — Paper Collector

A research productivity tool that builds a curated, searchable dataset of papers from top-tier security conferences and journals. Downloads metadata from DBLP, enriches it with abstracts scraped from publisher websites and open APIs, and exposes a full-text search interface.

**Supported venues:** ACM CCS · IEEE S&P · USENIX Security · NDSS · ACM ASIA CCS · IEEE EURO S&P · HotNets · ACM SACMAT · ACM Computing Surveys · IEEE Communications Surveys & Tutorials · Foundations and Trends in Privacy and Security

---

## Quick start

```bash
git clone https://github.com/your-user/topVenues.git
cd topVenues
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### One-command full run

```bash
python -m src.cli run-all          # download → consolidate → extract
```

### Step by step

```bash
python -m src.cli download         # fetch DBLP JSON for all venues and years
python -m src.cli consolidate      # merge into SQLite (idempotent; abstracts preserved)
python -m src.cli extract          # fetch abstracts (rate-limited)
```

### Web interface

```bash
streamlit run web/app.py
```

Open <http://localhost:8501>.  
Filters: title, abstract, authors, topic, venue, year, SoK, survey, poster, has-abstract.

### Search from the CLI

```bash
python -m src.cli search --title "SOC" --author "Sekar" --abstract "autonomous"
python -m src.cli search --tech "LLM" --event "ACM CCS" --year 2023
python -m src.cli stats
```

---

## Project layout

```
src/
  models.py            Pydantic DTOs
  config.py            Configuration loader
  collector.py         Orchestrator (download → consolidate → extract)
  downloader.py        Async DBLP JSON downloader with circuit breaker
  consolidator.py      Merges JSON files into deduplicated Paper objects
  database.py          SQLite layer — single source of truth
  abstract_fetcher.py  Parallel fallback APIs (Semantic Scholar, OpenAlex, CrossRef)
  event_normalizer.py  Venue string → canonical name (Strategy pattern)
  venue_config.py      DBLP URL generation per venue (Strategy/Registry pattern)
  circuit_breaker.py   Circuit breaker for unstable external services
  extractors/          Per-publisher HTML scrapers (ACM, IEEE, USENIX, NDSS)
  cache.py             Local abstract cache
  checkpoint.py        Checkpoint/resume for long extraction runs
  cli.py               Click CLI

web/
  app.py               Streamlit interface

tests/                 pytest unit tests (110 tests)
legacy/                Original R script preserved for reference
```

---

## Configuration

Copy and edit `config.yaml` (or use the defaults):

```yaml
year_start: 2019        # fetch papers from this year onwards
events:                 # subset of supported venue keys
  - ccs
  - uss
  - sp
  - ndss
batch_size: 10
cache_enabled: true
```

---

## Data notes

- The SQLite database (`data/dataset/papers.db`) is the single source of truth.  
  Running `consolidate` repeatedly is safe — existing abstracts are never overwritten (`COALESCE` on upsert).
- `data/dataset/master_dataset.csv` is a derived export regenerated on each consolidate run.
- All `data/` content is excluded from version control (see `.gitignore`).

---

## Development

```bash
pip install -e ".[dev]"
pytest                  # 110 tests
```

---

## Citation

```bibtex
@software{topvenues,
  title  = {Top Security Venues — Paper Collector},
  year   = {2025},
  url    = {https://github.com/your-user/topVenues}
}
```

MIT License.
