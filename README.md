# topVenues

**A bibliographic explorer for the top-tier security research venues.**

`topVenues` builds a curated, searchable SQLite dataset of papers published in
the leading computer-security conferences and survey journals. It downloads
metadata from DBLP, enriches every paper with abstracts pulled from open APIs
and publisher websites, and exposes a fast full-text search interface for
researchers, students and reviewers preparing literature reviews.

The dataset shipped with the project covers **9 900+ papers** across **11
venues** with **99.9 % abstract coverage**.

---

## Indexed venues

| Venue                                                | Type       |
| ---------------------------------------------------- | ---------- |
| ACM CCS — Conference on Computer & Comm. Security    | Conference |
| IEEE S&P — Symposium on Security and Privacy         | Conference |
| USENIX Security                                      | Conference |
| NDSS — Network and Distributed System Security       | Conference |
| ACM ASIA CCS                                         | Conference |
| IEEE EURO S&P                                        | Conference |
| ACM SACMAT                                           | Conference |
| HotNets                                              | Workshop   |
| ACM Computing Surveys                                | Journal    |
| IEEE Communications Surveys & Tutorials              | Journal    |
| Foundations and Trends in Privacy and Security       | Journal    |

The set is configurable in `config.yaml`. Adding a new venue requires only a
URL strategy and an event-name normaliser — see *Extending* below.

---

## Quick start

```bash
git clone https://github.com/sidneibarbieri/topVenues.git
cd topVenues
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Web interface (recommended)

```bash
streamlit run web/app.py
```

Open <http://localhost:8501>. Three pages:

- **🔍 Search** — full-text filters on title, abstract, authors, topic; venue,
  year, paper class (SoK / Survey / Poster / Workshop / Short / Journal /
  Article), abstract-length and BibTeX filters; sortable, paginated table
  that shows an abstract preview and the `\cite{…}` command for each row.
  CSV / JSON / `.bib` export.
- **📊 Insights** — distributions by venue, year, class; abstract and
  BibTeX coverage.
- **⚙ Pipeline** — run download / consolidate / extract / bibtex directly
  from the UI.

### Command line

```bash
python -m src.cli download         # fetch DBLP JSON for all venues and years
python -m src.cli consolidate      # merge into SQLite (idempotent)
python -m src.cli extract          # fetch missing abstracts (rate-limited)
python -m src.cli bibtex           # fetch BibTeX entries from DBLP
python -m src.cli run-all          # download + consolidate + extract + bibtex

python -m src.cli search --title "SOC" --author "Sekar" --abstract "LLM"
python -m src.cli search --tech "blockchain" --year 2024
python -m src.cli stats
```

### BibTeX & LaTeX integration

Every paper carries the BibTeX entry served by DBLP and a derived
`\cite{cite_key}` snippet. The web UI shows both inline; the **Search**
page exports a ready-to-use `.bib` file containing every paper in the
current result set. Drop it into your LaTeX project and reference papers
directly with the displayed `\cite{…}`.

> **Companion tool:** once your `.bib` is in your paper, run
> [Vyas Sekar's AcademicLinter](https://github.com/vyassekar/AcademicLinter)
> against it to catch unused entries, weasel words, repeated words and
> double-blind privacy leaks.

### Incremental updates

The pipeline is fully incremental. Re-running `download → consolidate` next
year (or after a venue posts new proceedings) only fetches what is missing and
preserves every existing abstract via SQL `COALESCE`. To pick up a new year,
just bump `year_start` in `config.yaml` or leave it on the default — it
auto-extends to the current calendar year.

---

## Architecture

```
src/
  models.py            Pydantic DTOs (Paper, Configuration, SearchFilters,
                       AbstractImportResult, PaperClass)
  config.py            YAML configuration loader
  collector.py         Orchestrator (download → consolidate → extract)
  downloader.py        Async DBLP JSON downloader with circuit breaker
  consolidator.py      Merges JSON files into deduplicated Paper objects
  database.py          SQLite layer — single source of truth
  abstract_fetcher.py  Parallel fallback: Semantic Scholar / OpenAlex / CrossRef
  bibtex_fetcher.py    Concurrent DBLP .bib fetcher with retry / backoff
  event_normalizer.py  Venue string → canonical name (Strategy pattern)
  venue_config.py      DBLP URL strategy registry
  circuit_breaker.py   Circuit breaker for unstable upstreams
  extractors/          Per-publisher HTML extractors (xidel-based)
  cache.py             Local abstract cache (SQLite)
  checkpoint.py        Long-run resumability
  cli.py               Click CLI

web/app.py             Streamlit interface
tests/                 pytest suite (126 tests)
scripts/
  api_blitz.py         Concurrent API back-fill for missing abstracts
  bibtex_blitz.py      Concurrent BibTeX back-fill from DBLP
  verify_extractors.py Live integration check for publisher extractors
```

### Design highlights

- **SQLite is the single source of truth.** CSV and Pickle outputs are
  derived exports; the database survives every step of the pipeline.
- **Idempotent upsert.** Re-running `consolidate` 100× converges to the same
  state as running it once: existing abstracts are never overwritten.
- **Two-track abstract fetching.** Open APIs (Semantic Scholar, OpenAlex,
  CrossRef) are fired *in parallel* with `asyncio.as_completed` — first
  successful response wins. Publisher sites (ACM, IEEE, USENIX, NDSS) run
  *sequentially* with throttling because they sit behind Cloudflare.
- **Strategy / Registry patterns** for both venue URL generation and event
  name normalisation. Adding a new venue is purely additive.
- **Circuit breaker** wraps the DBLP downloader so a transient upstream
  outage stops cascading failures.
- **NDSS author-leak cleaner.** A comma-aware iterative matcher strips the
  `Name (Affiliation), Name (Affiliation), …` block that NDSS pages render
  before the abstract body — without ever truncating legitimate
  parentheticals like `Industrial Control Systems (ICS), …`.

---

## Configuration

`config.yaml` (defaults are sensible — edit only as needed):

```yaml
year_start: 2019                       # auto-extends to current year
events: [ccs, asiaccs, uss, ndss, sp,
         eurosp, hotnets, sacmat,
         acm_csur, ieee_comst, fnt_privsec]
batch_size: 10
acm_wait_min: 60.0                     # throttle window for publisher scrapers
acm_wait_max: 300.0
cache_enabled: true
cache_ttl_hours: 168
```

---

## Extending

To add a new venue:

1. Add the short identifier to `Configuration.events` and `EventType` in
   `src/models.py`.
2. Register a `VenueURLStrategy` in `src/venue_config.py` (point it at the
   DBLP page for that venue).
3. Add a normalisation rule in `src/event_normalizer.py` mapping DBLP's venue
   string to the canonical display name.
4. (Optional) add a publisher-specific extractor under `src/extractors/` if
   the open APIs don't cover that venue's papers reliably.

No code outside those four touch-points needs to change.

---

## Development

```bash
pip install -e ".[dev]"
pytest                         # 126 tests
ruff check src/ web/ tests/
```

---

## Data sources

- [DBLP](https://dblp.org) — paper metadata
- [Semantic Scholar](https://www.semanticscholar.org/product/api) — abstracts
- [OpenAlex](https://openalex.org) — abstracts (inverted index)
- [CrossRef](https://www.crossref.org) — abstracts (JATS XML)
- Publisher sites (ACM Digital Library, IEEE Xplore, USENIX, NDSS) — abstracts

All retrieval is read-only and respects published API rate limits.

---

## Citation

If `topVenues` helps your research, please cite it:

```bibtex
@software{barbieri_topvenues,
  author = {Barbieri, Sidnei},
  title  = {topVenues: a bibliographic explorer for top-tier security venues},
  year   = {2025},
  url    = {https://github.com/sidneibarbieri/topVenues}
}
```

---

## Author

**Sidnei Barbieri** — [@sidneibarbieri](https://github.com/sidneibarbieri)

Built to support systematic literature reviews and threat-landscape mapping
across the top-tier security research venues.

---

## License

MIT — see [LICENSE](LICENSE).
