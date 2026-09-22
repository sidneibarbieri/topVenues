# Research workflows and tier policy

## 1. Map a topic's reference venues

Select **Security top-4** in Search or Insights. In TopVenues this
means only ACM CCS, IEEE S&P, USENIX Security, and NDSS. Search the topic,
inspect the records, and export the defensible subset.

Use this for orientation, reference mapping, and monitoring a narrow elite-venue
stream. It is not a claim that work outside those venues is unimportant.

## 2. Find recurring authors without overstating the result

In **Researcher Radar**, choose a topic and select `Any author`, `First author`,
or `Last author`, then apply a tier scope. Use **Paper count** for the literal
frequency ranking requested by the research team. Use **Tier-weighted
visibility** only when the protocol explicitly justifies the declared venue
weights. Open an author's records before treating either ordering as
actionable. Neither metric measures citation impact, quality, seniority, or
fully identity-resolved individuals.

After selecting an author, inspect the annual trajectory and direct coauthors.
The **Emerging activity** table compares papers/year in the latest three corpus
years with the earlier annual rate. It is a descriptive acceleration signal;
do not present it as predicted impact. Every selected name can be opened in
Search to inspect the records behind the signal.

## 3. Carry a research watch into a successor snapshot

Download the portable watchlist from Researcher Radar. It records the profile,
authors, topics, tier scope, and paper IDs already observed. Evaluate it against
a later immutable profile:

```bash
python scripts/evaluate_watchlist.py topvenues-watchlist.json \
  --profile security-20-v4
```

The delta is deterministic. To retrieve possible new preprints, run
`scripts/monitor_preprints.py`. Its output is deliberately labelled **name-match
candidates**: DBLP and arXiv names are not cross-source identity proof. Confirm
identity before citing, alerting, or attributing a preprint.

## 4. Build a systematic-review candidate set

Start with **All declared venues** unless the review protocol justifies a
restriction. Apply topic, year, paper-class, and abstract-availability filters,
then export the resulting candidate set. Record every scope choice with the
snapshot identifier; TopVenues fixes the denominator but does not decide
relevance or inclusion.

## Tier policy

| Research-facing scope | Included venue tier(s) |
| --- | --- |
| Security top-4 | ACM CCS, IEEE S&P, USENIX Security, NDSS |
| Tier 1 plus regional editions | Top-4 plus ACM ASIA CCS and IEEE EURO S&P |
| Other top-tier venues | Explicit `top-tier` mapping in `src/tiers.py` |
| Strong venues | Explicit `strong` mapping in `src/tiers.py` |
| Survey journals | ACM Computing Surveys, IEEE Communications Surveys & Tutorials, Foundations and Trends in Privacy and Security |
