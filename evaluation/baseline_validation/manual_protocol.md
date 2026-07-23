# Manual validation protocol: intrinsic abstract validity

Status: completed. One author labeled all 200 records against the publisher
sources; `manual_labels.csv` holds the labels, evidence URLs and access dates.
Result: 168/200 valid (84.0%, exact 95% interval 78.2-88.8%).

## What this measures, and why

Reviewer 1 asked for a manual audit to report extraction precision, and Reviewer 2
tied it to the interpretation of the 99.86% coverage claim: a coverage figure
counts non-empty `abstract` fields, so the open question is what fraction of
those fields are genuine, usable abstracts rather than truncated, contaminated,
or misplaced text.

This protocol answers that question from the stored text and title alone. That
is deliberate: a consumer of the corpus (keyword search, triage, or a reader)
sees only the stored abstract, never the publisher's page, so validity judged on
the stored text is validity as experienced. It also stays consistent with the
paper's provenance limitation, which already reports that publisher-canonical
provenance is unresolved for 1,386 records; a byte-level fidelity audit would
measure exactly what the artifact cannot establish.

## Target quantity

Estimate the fraction of non-empty abstract fields that are complete, on-topic,
uncontaminated abstracts of the paper named by the title. This is conditional on
the field being non-empty; the field-coverage quantity (9,911/9,925) is reported
separately. Do not multiply the two or describe the audit as proof of 99.86%.

## What the author reads

The stored abstract and the title. Do not open the publisher source as a matter
of course; the point is to judge what the corpus actually serves. For a borderline
case only, the author may open the DOI/URL to resolve doubt, and must note in
`notes` that the source was consulted for that row.

## Labels (judgeable from stored text + title)

- `valid`: a complete, on-topic abstract of the paper named by the title.
- `truncated`: clearly cut off (ends mid-sentence, or a section is obviously
  missing).
- `contaminated`: a real abstract with non-abstract text mixed in — author lists,
  affiliations, references, headers, or navigation.
- `wrong_or_not_abstract`: the text is plainly about a different paper, or is not
  an abstract at all (a title, author block, preface, or other metadata).
- `uncertain`: cannot be decided from the stored text and title.

Primary metric: `valid` over the verifiable rows (`200 - uncertain`). Report a
sensitivity analysis that also counts `truncated` and `contaminated` as failures,
even though keyword search may still work on them. Report `uncertain` separately;
never fold it silently into numerator or denominator.

## Human process as executed

One author labeled all 200 rows against the publisher sources, recording the
evidence URL and access date for each. Because a single annotator produced the
labels, no inter-annotator agreement is reported. The labels are auditable
instead: every defective record carries the corrected abstract taken from the
authoritative source, so any reader can re-check the judgment against the
publisher page.

The 32 failures in 200 verifiable observations give an intrinsic validity of
84.0%, with an exact Clopper-Pearson 95% interval of 78.2% to 88.8%.
Venue-stratified sampling with a minimum of one record for the smallest venue
requires population weights for an overall estimate, so
`summarize_manual_audit.py` reports both the unweighted raw counts and the
weighted estimate. The sampling script and venue population sizes make the
weights auditable.

## What this does not measure

Judging from the stored text cannot detect a subtle wrong-paper substitution: an
abstract that reads correctly for the title but belongs to a different work on the
same topic. That residual risk is small, because extraction matches by DOI and
DBLP key, and it is partially covered by the automated cross-source agreement
already reported (145/163 Semantic Scholar and 120/161 OpenAlex at token-set
Jaccard 0.95). State this limitation when reporting the result; do not describe
the number as byte-level fidelity to the publisher's canonical text.
