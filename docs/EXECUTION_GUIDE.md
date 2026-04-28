# Execution Guide - Top Venues Paper Collector

## Prerequisites

1. **Python 3.11+**
2. **xidel** (required for abstract extraction)
   - macOS: `brew install xidel`
   - Linux: Download from https://github.com/benibela/xidel/releases
3. **Virtual environment** (recommended)

## Setup

```bash
cd topVenues
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Execution Flow

### 1. Download JSON Files from DBLP

```bash
python -m src.cli download
```

**What it does:**
- Iterates through configured events and years
- Downloads DBLP HTML pages
- Extracts JSON links
- Downloads and validates JSON files
- Logs all operations to `data/log/download_log.csv`

**Control points:**
- Skips existing valid files
- Retries on failure (max 3 attempts)
- Handles 429/403 with backoff
- Validates JSON structure before saving

**Risks:**
- DBLP rate limiting (mitigated by retries)
- Network failures (handled by retries)
- Corrupt JSON (validated before saving)

### 2. Consolidate JSON into Dataset

```bash
python -m src.cli consolidate
```

**What it does:**
- Loads all JSON files from `data/json/`
- Parses DBLP structure
- Normalizes event names
- Filters editorship entries
- Applies IEEE COMST topic filter
- Deduplicates by paper ID
- Merges with existing dataset (preserves abstracts)
- Saves to `data/dataset/master_dataset.pkl` and `.csv`
- Syncs to SQLite database

**Control points:**
- Type mapping for paper types
- Event normalization
- Deduplication
- IEEE COMST topic filtering
- Abstract preservation

**Risks:**
- Missing required fields (skipped)
- Invalid JSON (logged, skipped)
- Type conversion errors (handled by Pydantic)

### 3. Extract Abstracts

```bash
python -m src.cli extract
```

**What it does:**
- Loads papers without abstracts
- Processes in batches (default 10)
- Interleaves ACM/non-ACM requests (rate limiting)
- For each paper:
  1. Checks cache
  2. Tries venue-specific extractor (USENIX, NDSS, IEEE, ACM)
  3. Falls back to APIs (Semantic Scholar, OpenAlex, CrossRef)
  4. Caches result
  5. Updates database
- Saves checkpoints every 5 papers
- Saves dataset after each batch
- Delays 60-300s between batches

**Control points:**
- Cache hit/miss tracking
- ACM rate limiting (backoff, failure threshold)
- Checkpoint recovery
- Batch interleaving
- Random delays

**Risks:**
- ACM blocking (handled by backoff)
- xidel timeout (30s)
- API rate limits (handled by delays)
- Invalid abstracts (min 100 chars)

### 4. Full Workflow

```bash
python -m src.cli run-all
```

Runs all three phases sequentially.

## Database Operations

### Migrate CSV to SQLite

```bash
python -m src.cli db-migrate
```

Migrates existing `master_dataset.csv` to SQLite database.

### View Statistics

```bash
python -m src.cli stats
```

Shows:
- Total papers
- With/without abstracts
- Distribution by event
- Distribution by year

## Search

```bash
python -m src.cli search --title "SOC" --abstract "Autonomous" --author "Sekar" --event "ACM CCS" --year 2024 --tech "LLM" --limit 50
```

## Web Interface

```bash
python -m src.cli web
```

Opens Streamlit interface at http://localhost:8501

## Configuration

Edit `config.yaml` to customize:

```yaml
events: ["ccs", "asiaccs", "uss", "ndss", "sp", "eurosp", "hotnets", "sacmat", "acm_csur", "ieee_comst", "fnt_privsec"]
year_start: 2019
ieee_comst_topics: ["network", "IoT", "cloud", "edge", "wireless", "5G", "6G", "network security", "cybersecurity", "privacy", "blockchain", "distributed"]
request_timeout: 120
default_interval: [5.0, 15.0]
acm_wait_min: 60.0
acm_wait_max: 300.0
batch_size: 10
acm_failure_threshold: 3
acm_backoff_initial: 60.0
acm_backoff_max: 600.0
max_retries: 3
cache_enabled: true
cache_ttl_hours: 168
checkpoint_enabled: true
checkpoint_interval: 5
```

## Data Structure

```
data/
├── dataset/
│   ├── master_dataset.csv
│   ├── master_dataset.pkl
│   └── papers.db (SQLite)
├── json/
│   ├── data_ccs2019.json
│   ├── data_ccs2020.json
│   └── ...
├── log/
│   ├── download_log.csv
│   └── abstract_log.csv
├── cache/
│   └── *.json (cached abstracts)
└── checkpoints/
    └── checkpoint_*.pkl
```

## Error Handling

### Download Phase
- Network errors: Retry with exponential backoff
- 429/403: Wait and retry
- Invalid JSON: Log and skip
- xidel missing: Graceful degradation

### Consolidation Phase
- Missing fields: Skip record
- Type errors: Skip record
- Duplicate IDs: Keep first occurrence

### Extraction Phase
- ACM blocking: Backoff and skip after threshold
- xidel timeout: Return None, try fallback
- API failures: Try next API
- Invalid abstract: Skip (min 100 chars)

## Recovery

### Checkpoints
- Saved every 5 papers during extraction
- Location: `data/checkpoints/`
- Automatic recovery on restart

### Cache
- Abstracts cached for 7 days (configurable)
- Location: `data/cache/`
- Improves performance on re-runs

### Database
- SQLite database preserves all papers
- Abstracts updated incrementally
- Can export to CSV anytime

## Monitoring

### Logs
- Download log: `data/log/download_log.csv`
- Abstract log: `data/log/abstract_log.csv`

### Statistics
```bash
python -m src.cli stats
```

### Cache Stats
```python
from src.cache import CacheManager
cache = CacheManager(Path("data/cache"))
print(cache.get_stats())
```

## Testing

### Test Individual Extractor

```python
import asyncio
from src.collector import Collector
from src.extractors import USENIXExtractor

async def test():
    collector = Collector()
    extractor = USENIXExtractor()
    abstract = await extractor.extract(
        "https://www.usenix.org/conference/usenixsecurity25/presentation/agarwal-sharad",
        "301375",
        collector
    )
    print(abstract)

asyncio.run(test())
```

### Test Fallback APIs

```python
import asyncio
from src.collector import Collector
from src.abstract_fetcher import AbstractFetcher

async def test():
    collector = Collector()
    fetcher = AbstractFetcher(collector)
    abstract = await fetcher.fetch_semanticscholar("10.1145/1234567")
    print(abstract)
    await fetcher.close()

asyncio.run(test())
```

## Common Issues

### xidel not found
```
Error: FileNotFoundError: [Errno 2] No such file or directory: 'xidel'
```
**Solution:** Install xidel (see Prerequisites)

### ACM rate limiting
```
ACM blocked, waiting...
```
**Solution:** Normal behavior, will backoff automatically

### Database locked
```
sqlite3.OperationalError: database is locked
```
**Solution:** Close other processes accessing the database

### Memory issues
**Solution:** Reduce batch size in config.yaml

## Performance Optimization

1. **Enable cache** (default on)
2. **Reduce batch size** if memory constrained
3. **Increase delays** if rate limiting occurs
4. **Use SQLite** for complex queries
5. **Process events in parallel** (future enhancement)

## Security Considerations

1. User agents rotated to avoid detection
2. Rate limiting to prevent abuse
3. No credentials stored in code
4. HTTPS for all API calls
5. Input validation via Pydantic

## Best Practices

1. Always use virtual environment
2. Keep xidel updated
3. Monitor logs for errors
4. Use checkpoints for long runs
5. Test with small batch first
6. Backup database before major runs
7. Review config before execution
