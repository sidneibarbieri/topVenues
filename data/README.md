# Data layout

`data/profiles/security-20/` declares the frozen data object for the accepted SBSeg-SF paper. `security-20-v2` is its exact-resource deduplicated successor, `security-20-v3` applies the strict year window and identity adjudication, and `security-20-v4` repairs ten titles truncated at inline DBLP markup. Every profile retains a manifest with its SHA-256, counts, venue-level coverage, and observed years. Only the current v4 binary is bundled; historical binaries remain unchanged in their original release tags and can be fetched explicitly with `scripts/fetch_archived_profile.py`.

The local web and CLI default to `security-20-v4`; manifested profile snapshots are authoritative for reproduction. `data/adjudication/` records evidence-backed identity and title-repair decisions rather than hiding them in implementation code.

`data/awards/` contains source-backed optional award annotations. They enrich exploration output and do not change corpus inclusion, coverage, or ranking claims.

Generated databases, caches, downloaded DBLP dumps, and live-enrichment workspaces are intentionally excluded from the release.
