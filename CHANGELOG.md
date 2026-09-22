# Changelog

## 1.14.0 — 2026-09-22

- The current release is `security-20-v5`: 15,286 records, 14,394 with an
  abstract (94.2%), every one with BibTeX. It keeps all 14,859 `security-20-v4`
  records unchanged in every field and adds the 427 that the September 2026 DBLP
  release (10.4230/dblp.xml.2026-09-01) published after v4 froze:
  - IEEE S&P 2026 (252) and IEEE EuroS&P 2026 (82);
  - ACM SACMAT 2026 (28), ACM Computing Surveys (27), USENIX WOOT 2026 (26),
    IEEE COMST (9), and three late DBLP additions.
  USENIX Security 2026 and IEEE SaTML 2026 were not yet in DBLP.
- The additions carry their own human audit: 60 records sampled by venue, all
  judged by Sidnei Barbieri alone, 59 usable (98.3%; 95% Wilson interval
  91.1%–99.7%), in `evaluation/security-20-v5/`. The v3 audit still holds for
  the retained records, as `evaluation/security-20-v5/audit_transfer.json`
  checks.
- The one audit failure, an IEEE S&P 2026 record whose abstract collection had
  failed, is filled from the reviewer's transcription of the publisher record.
  `scripts/build_extended_profile.py repair-abstracts` applies such repairs; it
  refuses to overwrite existing text, and it logs each repair in
  `data/adjudication/security-20-v5-abstract-repairs.json`.
- `security-20-v4` stays bundled and reproducible with
  `bash reproduce.sh --profile security-20-v4`; `security-20` still reproduces
  the tools-track paper.
- The Hugging Face dataset `sidneibarbieri/topvenues` now exports
  `security-20-v5`.

## 1.13.1 — 2026-09-22

- A fresh clone following the README now passes its reproduction. A CLI test
  that selects `--profile` left a temporary profile in the process-wide
  configuration, and the interface tests that followed opened its empty
  workspace, so `bash reproduce.sh` stopped with two failures.
  - `tests/conftest.py` starts every test from the configuration the
    environment selects.
  - Continuous integration runs the README's command first, on the untouched
    checkout. Run second, it had inherited the `security-20` workspace, which
    hid the leak.
- No released profile or code path touching the data changed.

## 1.13.0 — 2026-09-22

- The interface speaks Brazilian Portuguese as well as English. It opens in the
  browser's language, the switch sits at the top of the sidebar, and
  `?lang=pt` or `?lang=en` in a link opens it in that language.
  - Strings stay in English at the call site and are looked up in
    `web/locales/pt.json` by `web/i18n.py`; option values stay English, so
    filters, links between pages and session state never depend on the
    language.
  - Numbers follow the reader: 20.305 and 94,5% in Portuguese, in the text,
    the tables and the chart labels.
  - `tests/test_interface_translations.py` fails on a string without a
    translation, a translation that drops a placeholder, an unused entry, a
    displayed literal that bypasses translation, and any page that raises or
    shows English in Portuguese.
- Picking Light or Dark in the menu now recolours the interface at once; it
  used to wait for the next interaction.
- "Open author records" from Researcher Radar with "Any author" selected now
  opens Search with "Any position" instead of an invalid option.
- Emerging-activity results are cached like the other analytics; a doubled
  decorator had left them uncached.
- `scripts/build_extended_profile.py` builds a successor profile additively from
  a newer DBLP release, keeping every existing record byte-identical.
- The citation guidance is a rule instead of a hint: whoever used the tool cites
  the tools-track paper and names the release and profile; whoever uses the
  corpus method or its measurements cites the main-track paper. The README,
  `README.pt-BR.md`, `CITATION.cff`, the landing page and the Hugging Face card
  that `export-hf` writes say the same.
- The documentation screenshots show the language switch, and
  `README.pt-BR.md` shows the interface in Portuguese:
  `scripts/capture_screenshots.py --language pt` captures the same scenes
  into `docs/assets/screenshots/pt-BR/`, reading the labels from the catalog.
- The project page shows the Portuguese screenshots in Portuguese.
- No released profile changed; every profile and paper reproduces as before.

## 1.12.0 — 2026-09-22

- The interface has a real dark theme. It follows the reader's system, and the
  menu offers Light, Dark or System. Every colour comes from `web/theme.py`,
  whose Pydantic-validated tokens drive the CSS custom properties in
  `web/styles.css`, the charts, and the reversed logo on dark.
- Charts use a palette validated for colour-vision separation and contrast
  against each theme's surface: the brand blue for single series, orange and
  teal for the second and third.
  - Coverage draws over a 100% track, so the gap is visible.
  - The unfinished year is marked partial.
  - Long venue names print whole.
  - Tables use thousands separators.
- Fixes a silent loss of chart styling: Altair's `configure()` replaced the whole
  config and ran last, so every axis, legend and view setting was discarded.
- `AGENTS.md` and `docs/AI_ASSISTANTS.md` show how to use TopVenues with
  Claude, Codex or ChatGPT: answer from the snapshot, cite with the exported
  BibTeX, and state the denominator with every number.
- `scripts/capture_screenshots.py` regenerates the documentation screenshots;
  they and the demonstration are refreshed for this release.
- No code path touching the data changed; every profile and paper reproduces as
  before.

## 1.11.1 — 2026-09-22

- The demonstration is re-recorded with this release in the current visual
  identity: the title and closing cards, the installation that clones
  `topVenues`, the interface with its new mark, and a new poster. The
  narration and its timing are unchanged, because nothing it says depends on
  the release. No code or data changes.

## 1.11.0 — 2026-09-22

- TopVenues moves home to `sidneibarbieri/topVenues`, the repository the
  main-track paper cites. `main` carries the current tool; the main-track
  paper's artifact stays frozen at the `sbseg2026-camera-ready` release.
  `sidneibarbieri/topvenues-tool`, the address the tools-track paper prints, is
  archived with its history and tags, and still answers the printed command.
- Continuous integration re-runs the main-track paper from its frozen release,
  next to both profiles on Ubuntu and Windows, so a change that breaks a
  published result cannot pass.
- New visual identity from the brand board: the wordmark with the faceted V,
  rebuilt as vector geometry (`docs/brand/build_brand.py`), with reversed,
  mono and app-icon variants. The interface, the project page and the favicon
  use it.
- `README.md` is the English documentation; the CTA-template documentation in
  Portuguese is `README.pt-BR.md`.
- The demonstration video streams from Hugging Face instead of shipping in the
  repository; its poster uses the new identity.
- The project page generator uses typed models for every value it states and
  now also offers a way to star the repository.

## 1.10.0 — 2026-09-21

- Apply the TopVenues visual identity to the interface: logo and favicon from
  the brand masters in `docs/brand/`, brand tokens in the stylesheet and the
  Streamlit theme. Chart colours are data colours, kept apart from the brand
  accent, and multi-series lines differ by dash as well as hue so that no chart
  relies on colour alone.
- Add `docs/PAPERS.md`, which binds each published paper to its repository,
  release, snapshot, SHA-256 and reproduction command, and route readers to the
  right artifact from both READMEs.
- Add the landing-page source under `site/`; it reads every figure about the
  current release from the profile manifest at build time.
- `CITATION.cff` declares the current version and cites the tools-track paper,
  with the main-track paper as a reference; a test keeps its version in step.
- The container image ships `docs/brand/`, which the interface now loads.

## 1.9.3 — 2026-09-02

- Move the one-command reproduction to the top of the README, before anything
  else a reviewer reads.

## 1.9.2 — 2026-09-01

- The minimal test tells the reader to activate the virtual environment, without
  which the first command failed on a clean machine. Memory and disk
  requirements are measured values.

## 1.9.1 — 2026-08-31

- Continuous integration also reproduces the profile the paper cites, in all
  eight environments, and publishes the execution record as an artifact.

## 1.9.0 — 2026-08-31

- Validate the container image by running it: the Evidence page no longer
  rendered inside it. Database identity is the file content, not its size and
  modification time.

## 1.8.1 — 2026-08-31

- Screenshot gallery and demonstration at the top of the README; version
  history.

## 1.8.0 — 2026-08-31

- Reproduce the paper's Table 2 by command (`reproduce_paper_table2.py`); each
  reproduction writes an execution record with environment, hashes and results.

## 1.7.1 — 2026-08-31

- Each claim declares the snapshot it was measured on, so a different profile is
  reported as out of scope instead of failing. Document Windows troubleshooting.

## 1.7.0 — 2026-08-31

- Reorganize the documentation on the CTA minimal template and describe the
  execution environment. `verify_paper_claims.py` binds every numeric claim of
  the paper to the query that verifies it. Fix the `Dockerfile`, which copied a
  missing path and installed unpinned versions.

## 1.6.0 — 2026-08-25

- New 7:49 demonstration video at 1920×1080, cut to the narration, with
  Brazilian Portuguese and English captions.

## 1.5.9 — 2026-08-25

- The arXiv search button passed API syntax to the web form and returned nothing
  for every author.

## 1.5.8 — 2026-08-25

- Derive the height of horizontal charts from the number of bars, removing empty
  frames.

## 1.5.7 — 2026-08-25

- Fix the scale of *Papers by class*: a bar starts at zero and a log scale has
  no zero, so no bar was drawn. Escape typed text before it becomes a `LIKE`
  pattern.

## 1.5.6 — 2026-08-25

- Describe the snapshots the package actually ships.

## 1.5.5 — 2026-08-25

- The interface also starts with `streamlit run web/app.py`. One headline-card
  row across pages.

## 1.5.4 — 2026-08-25

- Refuse to ship without the snapshots that published papers name.

## 1.5.3 — 2026-08-25

- The smoke test checks that the interface renders, not only that the port
  answers.

## 1.5.2 — 2026-08-24

- State the reproduction command for the reader's platform.

## 1.5.1 — 2026-08-24

- Restore the USENIX extractor fix and the top-4 concentration metric.

## 1.5.0 — 2026-08-24

- Publish `security-20-v4`, repairing ten titles truncated at inline DBLP markup
  without changing the 14,859-record denominator or any abstract text.
- Complete and publish a deterministic 200-record human abstract audit: 169
  records satisfy completeness, contamination, and paper-identity criteria
  (84.5%; 95% Wilson interval 78.8%–88.9%).
- Add exact audit-transfer evidence from v3 to v4 and retain the append-only
  provenance log rather than rewriting superseded events.
- Standardize researcher-facing `top-4` terminology, remove internal release
  counters from ordinary search screens, and improve chart semantics,
  readability, and record-level drill-down.
- Document and package a seven-minute end-to-end demonstration with US-English
  narration and English and Brazilian Portuguese captions.

## 1.4.0 — 2026-08-24

- Add researcher trajectories, direct collaboration evidence, and a transparent
  recent-versus-prior publication-rate signal with supporting-record drill-down.
- Add portable watchlists that preserve known paper IDs, successor-profile delta
  evaluation, and optional arXiv name-match candidate retrieval.
- Add a deterministic 200-record, venue-stratified v3 manual-audit instrument,
  upload validation, and Wilson interval summarizer without inventing labels.
- Keep only the current snapshot in the default package; historical binaries
  remain immutable in their original release tags and can be fetched with SHA-256
  verification.
- Correct DBLP numeric-suffix handling in external arXiv searches and replace the
  generic trajectory chart with a controlled editorial chart.

## 1.3.0 — 2026-08-24

- Publish `security-20-v3`, preserving prior snapshots while enforcing the
  declared 2019–2026 window and merging two DOI aliases confirmed by Crossref.
- Record all six same-metadata identity decisions in a versioned adjudication
  log; four pairs remain distinct because publisher resources remain distinct.
- Add Researcher Radar rankings by raw paper count (default) and by the
  separately labeled venue-tier heuristic, each with any/first/last-author views.
- Reproduce with a cross-platform, hash-locked dependency set on Python
  3.11–3.14.

## 1.2.1 — 2026-08-24

- Keep display preferences outside the semantic search-filter reset, removing
  a redundant Streamlit session-state assignment and its runtime warning.

## 1.2.0 — 2026-08-24

- Make declared venue tiers first-class in web search, author analytics,
  topic trends, CLI search, and exports.
- Preserve chronological order in annual charts and add selectable,
  consistently styled volume and share views.
- Reset stale search state before insight drill-downs and expose the active
  tier scope and tier evidence in result tables and author shortlists.
- Mark partial publication years to prevent incomplete-year trend claims.
- Install and health-check the web interface in the Linux/macOS and native
  Windows reviewer workflows.
- Test the artifact on Linux and Windows with Python 3.11 through 3.14 and
  refuse in-place overwrite of an immutable successor snapshot.

## 1.1.0 — 2026-08-24

- Publish `security-20-v2`, a successor snapshot that merges only records
  sharing an exact canonical DOI or landing page; the frozen `security-20`
  profile used by the SBSeg-SF paper remains available unchanged.
- Make the identity policy executable and testable, preserving distinct works
  that merely have similar titles.
- Add all-author, first-author, and last-author views to tier-aware author
  visibility.
- Add explicit insight-to-search navigation for venue and year distributions.
- Remove the stale, hard-coded test count from the web interface.

## 1.0.1 — 2026-08-18

- Add a native Windows PowerShell reproduction workflow.
- Close every core SQLite connection deterministically before temporary-file cleanup.
- Validate the reviewer workflow on Windows and Linux with Python 3.11 and 3.12 in CI.
- Align release identity, test counts, and platform instructions across reviewer documentation.

## 1.0.0 — 2026-07-25

- Publish the immutable `security-20` corpus profile and initial reviewer workflow.
