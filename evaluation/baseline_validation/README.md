# TopVenues baseline-comparison pilot and manual-audit package

This directory isolates the cross-source baseline evaluation from the corpus code. It
does not modify `main.tex`, does not replace the accepted 9,925-paper
denominator, and does not call machine agreement a human validation.

## Frozen object and sample

- Submitted snapshot SHA-256:
  `0f4dbaa97d0cf39abd2340adb3280643df090b5de9cd1a29bff39a0b53ef64cd`.
- Snapshot counts: 9,925 papers, 9,911 non-empty abstracts, 9,924 BibTeX
  entries, 11 venues, 2017--2026.
- Pilot/manual-audit sample: 200 records drawn only from the 9,911 records with
  abstracts, random seed `20260721`, proportional venue strata with a minimum
  of one record per venue.
- Allocation: USENIX Security 41; ACM CCS 40; ACM Computing Surveys 35; IEEE
  S&P 23; IEEE Communications Surveys & Tutorials 16; NDSS 16; ACM ASIA CCS
  13; IEEE EuroS&P 7; HotNets 5; ACM SACMAT 3; Foundations and Trends in
  Privacy and Security 1.

`generate_sample.py` refuses any snapshot with a different digest. Run:

```bash
python generate_sample.py --snapshot /path/to/submitted/papers.db.gz --verify
```

## Live pilot performed on 2026-07-21 (America/Sao_Paulo)

The operation was *known-record alignment and abstract-field availability*.
It asks whether each service can return a sampled TopVenues record and an
abstract. Because the sample is selected from the DBLP-backed TopVenues
denominator, these numbers do **not** measure recall outside TopVenues and do
not establish that TopVenues recovers every paper in a venue.

| System | Aligned records | Abstracts | Logical HTTP requests | Retained request latency, median / p95 |
|---|---:|---:|---:|---:|
| TopVenues SQLite | 200/200 | 200/200 | 0 | 0.0043 / 0.0065 ms per warm SQL lookup |
| DBLP frozen keys/schema | 200/200 by construction | 0/200 by data contract | not retained; live run incomplete | n/a |
| Semantic Scholar | 199/200 | 163/200 | 58: 1 DOI batch + 57 title matches | 1,237 ms batch wall; title 1,083 / 2,834 ms |
| OpenAlex | 171/200 | 161/200 | 200: 143 DOI singletons + 57 title searches | DOI 362 / 381 ms; title 512 / 895 ms |

Logical HTTP requests count protocol operations and exclude retries. The initial
pilot did not retain retry counts or end-to-end wall time. Retained latency
excludes deliberate rate-limit pacing, retry backoff, and earlier failed
attempts. TopVenues made no HTTP request for this read-path measurement, but its
latency comes from 2,200 warm local SQL lookup trials (11 per sampled record),
not from an end-to-end corpus build. The table therefore compares conditional
known-record availability and read paths; it is not a construction-cost
comparison.

Semantic Scholar used one DOI batch request for 143 DOI-bearing rows, then the
`paper/search/match` endpoint for 57 non-DOI rows, paced at 1.05 seconds. The
single batch latency is repeated as the retrieval latency for its members; it
must not be interpreted as 143 independent requests. Semantic Scholar's
official keyed introductory tier is 1 request/s; anonymous calls share a
throttled pool and returned transient HTTP 429 responses during setup.

OpenAlex used free DOI singleton lookups for 143 rows and 57 title searches for
non-DOI rows. The retained final-response headers reported $0 of API-budget
metering for DOI singletons and $0.001 for each title search; those retained
values sum to $0.057. This value is server-reported tariff consumption, not an
invoice or an out-of-pocket charge; the pilot did not retain
account/free-allowance state or earlier retry attempts. Three title queries
returned HTTP 400 because punctuation was interpreted as search syntax, and
their retained $0.001 metering values are included in the total. The failures
are retained rather than silently repaired.

For DBLP, record alignment is known from construction: every sampled record has
the DBLP key that defines the snapshot spine, and DBLP's exported record schema
does not contain abstracts. We nevertheless attempted the official
`/rec/{key}.xml` endpoint with 1.05-second pacing and `Retry-After` handling.
After one earlier availability probe completed in 744.6 ms, the controlled run
encountered repeated 20-second zero-byte timeouts. It was stopped rather than
hammering the public service. A single success plus timeouts cannot support a
median, so DBLP latency is `n/a`, not a fabricated number.

Agreement is diagnostic, not accuracy. Among service-provided abstracts,
145/163 Semantic Scholar and 120/161 OpenAlex texts had token-set Jaccard at
least 0.95 with the snapshot. Disagreements are precisely the records that need
human inspection; neither upstream index is a gold standard.

## Reproduction and outputs

Install the artifact dependencies, set `S2_API_KEY` and `OPENALEX_API_KEY` if
available, and run:

```bash
python run_live_baselines.py --snapshot /path/to/submitted/papers.db.gz
```

The script writes:

- `pilot_observations.csv`: one row per sampled paper and service;
- `pilot_summary.json`: counts, latency, logical requests, and OpenAlex
  server-reported API-budget metering;
- `pilot_raw_responses.json.gz`: timestamped live response bodies for audit.

By default, a rerun writes these files under a fresh timestamped `reruns/`
directory. `--output-dir /new/empty/directory` selects another location. The
runner refuses to overwrite any existing evidence file. Rerun summaries use
schema version 2 and retain the operation-level blocks used by the paper
(`doi_batch`, `title_match`, `doi_singleton`, and `title_search`). In every
summary, `http_200_n` counts successful logical HTTP calls, never records
carried by a batch.

The pilot commits `pilot_observations.csv` (the raw per-record
measurements) and `pilot_summary.json`. A future rerun also produces the
compressed full response bodies; those upstream response payloads are not
needed to interpret the committed counts.

Live responses will drift. Reproducibility here means a fixed sample, exact
snapshot, executable protocol, and committed per-record observations—not the
false promise that broad scholarly indices will return identical content in
the future. The initial pilot did not retain its full response bodies; the
runner now writes them on rerun. This limits forensic re-parsing of the July 21
payloads but does not promote cross-index agreement to ground truth. Latency
excludes deliberately imposed inter-request sleep.

The frozen `pilot_observations.csv` is preserved as raw historical evidence and
retains legacy columns named `dblp_cost_usd`, `s2_cost_usd`, and
`openalex_cost_usd`. The DBLP and Semantic Scholar zeros were constants in the
pilot code, not measured monetary costs. Only the OpenAlex values came from its
per-response API-budget meter. New runs use the explicit field
`openalex_server_reported_api_budget_metering_usd` and report logical requests
separately from network attempts. They sum values across attempts that expose
the meter and report whether metering coverage is complete or partial.

## Manual audit remains open

`manual_adjudication.csv` is intentionally blank and `manual_protocol.md`
defines the labels. The six-record `scripts/verify_extractors.py` smoke test is
not representative, and Semantic Scholar must not be called “ground truth.”
Until two authors complete and adjudicate this sheet, the paper can report only
the live baseline pilot, not manual extraction precision.

After two distinct authors independently label all 200 rows, preserve their
evidence URLs and access dates, and adjudicate every disagreement, run:

```bash
python summarize_manual_audit.py
```

The utility is intentionally offline: it neither queries APIs nor proposes
labels. It refuses a partial worksheet, inconsistent annotator identities,
invalid labels, missing evidence, or undocumented disagreements. Only a
complete sheet produces `manual_audit_summary.json`, containing raw agreement,
Cohen's kappa over the six verifiable categories, adjudicated label counts, an
unweighted strict-correctness estimate with an exact 95% Clopper--Pearson
interval, and a venue-weighted estimate. The unequal-weight estimate does not
receive an ordinary binomial interval because that would impose an invalid IID
sampling model. The committed blank worksheet is expected to fail this check;
that refusal is the guardrail, not an error to bypass.

The submitted SQLite schema also lacks an `abstract_source` field. Local
extraction logs are not part of the released snapshot, and the code has no PDF
abstract-extraction stage. Therefore the existing phrase “a small number of
abstracts are reconstructed from PDF text” has no auditable count. Do not invent
one: recover a provenance manifest from contemporaneous logs/manual records, or
state that the submitted snapshot cannot distinguish those records.

## External-service facts used in interpretation

- DBLP states that it does not provide abstracts and rate-limits excessive
  automated requests with HTTP 429 and `Retry-After`.
- Semantic Scholar recommends batch endpoints; unauthenticated calls share a
  rate-limited pool, while the introductory API-key tier is 1 request/s.
- OpenAlex documents free DOI singletons, $0.001 of API-budget metering per
  search, a $1/day free keyed allowance, a 100 requests/s ceiling, and
  mandatory free keys for scaled API use since February 2026.

These are time-sensitive operational facts. Any write-up should link the
official service documentation and date the comparison.

Official references checked on 2026-07-21:

- DBLP abstract policy: <https://dblp.org/faq/1474740>
- DBLP crawler/rate-limit policy: <https://dblp.org/faq/1474706>
- Semantic Scholar API access and limits:
  <https://www.semanticscholar.org/product/api>
- Semantic Scholar batching guidance:
  <https://www.semanticscholar.org/product/api/tutorial>
- OpenAlex authentication and API-budget metering:
  <https://developers.openalex.org/api-reference/authentication>
- OpenAlex February 2026 key migration:
  <https://developers.openalex.org/guides/deprecations>
