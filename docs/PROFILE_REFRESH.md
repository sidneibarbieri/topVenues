# Controlled profile refresh

The published `security-20-v4` snapshot is read-only. The web interface can
explore it, but it must not be used to refresh it from live APIs.

To publish a successor, an operator works in a separate profile and records:

1. the venue and year declaration;
2. source versions and collection time;
3. the exact-resource deduplication report;
4. per-venue abstract coverage and provenance;
5. automated test and search/export results; and
6. the gzip and SQLite SHA-256 values in a new manifest.

Before promotion, compare the previous and successor profiles:

```bash
python scripts/compare_profiles.py PREVIOUS SUCCESSOR --output profile-diff.json
```

The report records exact added, removed, and retained paper IDs plus headline
coverage. Review removals, identity merges, venue/year changes, and a fresh
manual-audit sample before freezing the successor. If the previous binary has
been archived, retrieve it first with `scripts/fetch_archived_profile.py`.

Only after those checks pass should the new snapshot receive a new semantic
release tag and replace the default researcher-facing profile. Never overwrite
an existing release snapshot or reinterpret its published counts.

The historical `scripts/build_deduplicated_profile.py` builder refuses to run
when its target already exists. It is not a refresh command. A maintainer must
first declare a new profile identifier, target directory, configuration, and
manifest, then review the resulting snapshot as a new scientific object.

The current exact-resource policy merges records only when a canonical DOI or
stable landing page is identical. Similar titles, journal extensions, and
records with different canonical resources remain distinct until they are
manually adjudicated.

When an exact-resource group or multiple abstract providers expose different
abstract texts, selection is deterministic but not based on character count
alone. Candidates are normalized while preserving source-exposed paragraph
boundaries, rejected metadata is ranked below valid prose, a candidate with a
terminal sentence is ranked above a likely truncation, and word count is used
only after those quality checks. The final lexical tie-break makes a replay
stable. A successor must rerun the manual audit because this rule can change
abstract bytes without changing the bibliographic denominator.

## Additive extension from a newer DBLP release

When DBLP publishes new editions of declared venues, a successor can add them
without touching anything the source already holds.
[`scripts/build_extended_profile.py`](../scripts/build_extended_profile.py)
admits a record only when:

- its DBLP key and canonical resource are absent from the source;
- its year is declared;
- no decision in the identity log already merged it as an alias.

Every excluded record is logged with its reason.

```bash
python scripts/build_extended_profile.py stage --dump dblp-2026-09-01.xml.gz --staging STAGE
python -m src.cli --base-dir STAGE backfill-abstracts
python -m src.cli --base-dir STAGE extract
python -m src.cli --base-dir STAGE bibtex-from-dump --dump dblp-2026-09-01.xml.gz
python scripts/build_extended_profile.py freeze --staging STAGE \
  --dump-release 10.4230/dblp.xml.2026-09-01 --output-root REVIEW
```

- **Where the dump comes from:** use the monthly DBLP XML release archived on
  [Dagstuhl DROPS](https://drops.dagstuhl.de/entities/collection/dblp), and
  check its MD5.
- **How to review before promoting:** `--output-root` writes the candidate
  outside the repository. Review it with `compare_profiles.py` and a manual audit
  sample drawn from the added records, then freeze it into the repository.
- **What stays unchanged:** the added records use the DBLP key as `paper_id`,
  because the dump carries no numeric DBLP identifier. The source records keep
  theirs.
