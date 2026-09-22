# Manual abstract audit

Abstract coverage is exhaustive to count mechanically, but text quality requires
comparison with publisher or repository evidence. TopVenues therefore uses a
deterministic, venue-stratified 200-record sample and three explicit criteria.

## Completed result

Sidnei Barbieri manually reviewed all 200 sampled records. A record counts as
usable only when all three labels are `yes`:

| Criterion | Yes |
| --- | ---: |
| Complete rather than truncated | 171 |
| Free of navigation, copyright, references, or unrelated text | 198 |
| Belongs to the sampled paper | 190 |
| All three criteria | 169 (84.5%) |

The 95% Wilson interval for the usable rate is 78.8%–88.9%. The result is a
quality estimate for abstract-enriched records, not a precision or recall claim
for bibliographic retrieval.

Primary evidence is preserved in:

- `evaluation/security-20-v3/manual_abstract_audit.csv`;
- `evaluation/security-20-v3/manual_abstract_audit_decisions.jsonl`;
- `evaluation/security-20-v3/manual_abstract_audit_summary.json`.

All 200 final decisions are `human_only` and name the reviewer. The append-only
JSONL contains 473 events because provenance corrections and backfills retain
superseded history. Resolve it by the latest event for each `sample_id`; do not
count events as additional sampled records.

## Applicability to v4

The annotations were made against `security-20-v3`. They transfer to
`security-20-v4` because the profiles contain the same 14,859 paper IDs and
identical abstract text. The ten v4 changes are title repairs, and none belongs
to the audit sample. The comparison and decision are machine-readable in
`evaluation/security-20-v4/audit_transfer.json` and explained in
`evaluation/security-20-v4/AUDIT_TRANSFER.md`.

## Repeat or extend the protocol

Generate the fixed sample:

```bash
python scripts/manual_abstract_audit.py \
  --profile security-20-v4 \
  --sample-size 200 \
  --output security-20-v4-manual-audit.csv
```

For each row, open `source_url`, compare the displayed source with `abstract`,
and answer all three label questions. Record the reviewer and a concise note for
every negative label. Do not infer labels from length or an automated heuristic.

Summarize a completed sheet:

```bash
python scripts/manual_abstract_audit.py \
  --summarize security-20-v4-manual-audit.csv
```

Partially labelled rows are reported as incomplete and excluded; they are never
silently imputed.
