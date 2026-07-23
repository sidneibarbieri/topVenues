# Frozen-snapshot deduplication and provenance audit

`audit_frozen_snapshot.py` performs an offline, read-only audit of the exact
submitted TopVenues snapshot. It never opens SQLite in write mode and never
changes the snapshot, archive, or API cache.

From the released artifact root, run:

```bash
python3 evaluation/audit_frozen_snapshot.py
```

The defaults bind the audit to the submitted `papers.db.gz` (SHA-256
`0f4dbaa97d0cf39abd2340adb3280643df090b5de9cd1a29bff39a0b53ef64cd`)
and use `inputs/source_evidence.zip`, a deterministic 6.7 MB bundle containing
the exact historical log member and 3,270 API-cache records used by the audit.
The bundle SHA-256 is
`a9ccfd2c6a6755d32238a511d3b6d19678feb4b77a84005bb93c1d015a93ec4e`.
The script refuses a different snapshot hash or counts unless
`--no-strict-snapshot` is supplied explicitly.

Outputs in `output/`:

- `audit_summary.json`: aggregate counts, policies, input/output hashes, and
  limitations;
- `input_manifest.csv`: SHA-256 and size of every input file;
- `abstract_provenance_evidence.csv`: one row per frozen DBLP record, with an
  abstract content hash and any exact retained log/cache evidence;
- `dedup_candidates.csv`: records in exact normalized title+author clusters.

For the supplied inputs, exact evidence covers 8,525 of 9,911 non-empty
abstracts. The remaining 1,386 stay explicitly unresolved. This is deliberate:
the frozen database has no per-record abstract-source column, so an exact match
is evidence consistent with a source, not proof that the source wrote the
field. Consequently the historical PDF-derived count is reported as `null`,
not zero; none of the retained exact matches has a PDF-labelled source.

The deduplication report is also non-destructive. It reports five exact
bibliographic candidate clusters (ten IEEE S&P records) but removes nothing
from the accepted 9,925-record denominator. Distinct DBLP publications,
including journal extensions, remain separate. The audit does not use fuzzy or
semantic matching to guess journal-version relationships.

Given identical input bytes, the four output files are byte-for-byte
deterministic. The bundle can be rebuilt with
`build_source_evidence_bundle.py` when the original archive and cache are
available. Source timestamps remain naive because the historical inputs do not
record a timezone; the audit does not relabel them as UTC.
