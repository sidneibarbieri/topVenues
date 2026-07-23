# Human adjudication protocol for the frozen 200-record sample

Status: protocol and blank worksheet only. No author has completed the labels,
so this directory does **not** contain a manual-validation result.

## Target quantity

Estimate abstract-extraction correctness conditional on the May 2026 snapshot
having a non-empty abstract. The exact field-coverage quantity is reported
separately as 9,911/9,925. Do not multiply the two quantities or describe a
200-record audit as proof of 99.86% extraction accuracy.

## Reference source and evidence

For every row, open the canonical source in this order: publisher/venue landing
page; publisher PDF; DOI landing page. Record the exact URL and access date.
Semantic Scholar and OpenAlex are comparison baselines, not ground truth. If no
canonical text can be accessed, label the record `unverifiable`; do not guess.

## Labels

- `exact`: normalized canonical and snapshot texts carry the complete same
  abstract; HTML, Unicode, whitespace, and line-break changes are allowed.
- `correct_formatting`: the same complete abstract with harmless formatting or
  formula degradation that is more than normalization alone.
- `truncated`: the right paper, but meaningful leading, internal, or trailing
  abstract text is absent.
- `contaminated`: the right abstract includes author lists, affiliations,
  navigation text, references, or another non-abstract block.
- `wrong_paper`: the text belongs to another work.
- `not_abstract`: the field contains a title, preface, author list, or other
  metadata rather than an abstract.
- `unverifiable`: no authoritative reference was accessible.

Primary strict correctness counts `exact` and `correct_formatting` as correct.
Report the other categories separately; do not silently fold `unverifiable`
into either numerator or denominator. Also report a sensitivity analysis that
counts `truncated` and `contaminated` as incorrect, even if keyword search may
still work.

## Human process

Two authors should label all 200 rows independently and without seeing the
live-API agreement scores. Compare labels, report raw agreement and Cohen's
kappa over the six verifiable categories, then adjudicate disagreements in a
recorded third pass. The final worksheet must retain both original labels and
the adjudicated label. Corrections to the snapshot, if any, belong in a
separate versioned correction manifest; never overwrite the submitted object
without changing its digest.

With zero strict errors in 200 independent observations, the rule-of-three
upper 95% bound on the error rate is about 1.5%; the exact binomial interval
should be reported from the observed labels. Venue-stratified sampling with a
minimum of one record for the smallest venue requires population weights for
an overall estimate. Report both unweighted raw counts and the weighted
estimate; the script's allocation and venue population sizes make the weights
auditable.
