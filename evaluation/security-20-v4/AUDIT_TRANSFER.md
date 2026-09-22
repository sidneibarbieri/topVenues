# Manual abstract-audit transfer from security-20-v3

The 200-record manual audit was performed against `security-20-v3`. Its labels
remain applicable to `security-20-v4` because the successor contains the same
14,859 `paper_id` values and unchanged abstract fields. The only record-level
changes are ten repaired titles, and none of those ten records belongs to the
deterministic 200-record audit sample.

The primary labels and append-only decision history remain under
`evaluation/security-20-v3/`; their profile identifiers are not rewritten.
`audit_transfer.json` records the executable transfer check for the successor.

This transfer supports abstract completeness, contamination, and paper-identity
claims for the sampled records. It does not turn the audit into evidence for
unrelated future corpus changes.
