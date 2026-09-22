# SBSeg-SF 2026 presentation and bench script

The programme schedules TopVenues in the Thursday, 3 September bench session
(16:00-16:45) and as item 9 of Friday's Block 2 (11:00-12:30). The Friday
presentation lasts seven minutes. The released demonstration follows that exact
duration and is available in [`docs/demo/`](demo/README.md); use the same path
for the live bench walkthrough.

## Objective

A cybersecurity researcher turns an informal literature search into an
inspectable, exportable set of records from a declared snapshot.

## Proof obligation

The audience must see a fixed corpus identity, a real query, a transparent
author-visibility view, a chart-to-record transition, and an export.

## Seven-minute shot list

The production timing, US-English narration, Brazilian Portuguese translation,
and exact screen actions are frozen in [`demo/narration.json`](demo/narration.json) and
[`demo/SHOT_PLAN.md`](demo/SHOT_PLAN.md). The concise live sequence remains:

| Time | Visible action | Takeaway |
| --- | --- | --- |
| 0:00-0:35 | State the problem: reviews need a stable denominator before screening. | TopVenues fixes the population, not the researcher's judgment. |
| 0:35-1:05 | Open Overview; point to profile, counts, abstract coverage, and offline verification. | The corpus is versioned and auditable. |
| 1:05-2:05 | Search `LLM` or `fuzzing`, restrict to a venue/year, open a paper, and export BibTeX. | Search is a review workflow, not a static catalogue. |
| 2:05-3:00 | In Insights, click a venue or year bar and show the transferred record filter. | Aggregate claims are traceable to records. |
| 3:00-4:05 | Enter a topic and show its yearly share and main venues. | Volume is normalized by yearly corpus size. |
| 4:05-5:25 | Switch author view among any, first, and last author; open one author's records, trajectory, and emerging-activity evidence. | These are transparent corpus observations, not citation, authority, or impact rankings. |
| 5:25-5:55 | Download a portable watchlist and point to the arXiv candidate handoff without opening the network-dependent page. | The evidence can support later surveillance without making the live demo depend on Wi-Fi. |
| 5:55-6:30 | Show abstract coverage and the immutable Dataset lifecycle boundary. | Missing data is disclosed and a refresh cannot silently change a cited snapshot. |
| 6:30-7:00 | Close with the repository, release tag, and the claim boundary. | Researchers can reproduce, inspect, and export the same denominator. |

## Do not claim

- that the author score measures quality, seniority, influence, or citations;
- that abstract search has perfect recall in venues with missing abstracts;
- that a correlation with author history causes future acceptance; or
- that a live API refresh reproduces a prior release.
