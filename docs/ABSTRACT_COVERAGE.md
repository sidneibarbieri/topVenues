# Where the corpus has no abstract, and what can be done about it

`security-20-v4` carries an abstract for 13,987 of its 14,859 records
(94.1%). This records what the remaining 872 are, so the gap is a known
quantity rather than a surprise during a review.

## The gap is concentrated, not diffuse

| Source | Missing | Recoverable |
|---|---:|---|
| Springer (ESORICS) | 559 | No — subscription |
| USENIX | 131 | **Yes — open access** |
| ACM Digital Library | 95 | No — subscription |
| IEEE Xplore | 86 | No — subscription |
| NDSS | 1 | **Yes — open access** |

Two thirds of the gap is ESORICS, published in Springer LNCS. Automated
retrieval from a subscription host is treated as systematic downloading and
can suspend an institution's access, so the pipeline does not attempt it.

## The recoverable 132 are ready to collect

The USENIX extractor tried a single-paragraph selector before the joining
one until v1.5.1, which both truncated multi-paragraph abstracts and failed
outright on some pages. With the corrected ordering, a sample of eight
missing USENIX records recovered 8 of 8, between 409 and 1,924 characters.

Collecting them is a release action, not a maintenance one. It produces a
new snapshot with a new SHA-256, which:

- supersedes the snapshot identity every release in this line publishes;
- unbinds the 200-record manual audit, which is evidence for the snapshot it
  was performed on;
- invalidates the recorded demonstration, which states the current counts.

So it belongs in a successor release with its own audit transfer note, not in
a patch. Until then the interface states the uneven coverage where it matters,
and a query that depends on abstracts warns which venues it cannot reach.

## Reproducing this table

```bash
python scripts/report_abstract_gap.py
```
