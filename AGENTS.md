# Working with TopVenues as an AI agent

TopVenues is a frozen, versioned corpus of cybersecurity papers with a CLI. Use
it to answer literature questions from a declared population, not from memory.

## Answer from the corpus

- Run `bash reproduce.sh --profile security-20-v5` once. It creates `.venv/` and
  verifies the snapshot. Then call the CLI as
  `.venv/bin/python -m src.cli --profile security-20-v5 <command>`
  (`.venv\Scripts\python` on Windows).
- **Find papers:**
  - `search --rank "<query>" --tier-scope "Security top-4" --limit 20` returns
    ranked results.
  - `export --format json -T "<topic>" --limit 50` returns JSON with the DBLP
    key, link, abstract and BibTeX.
- **Measure:**
  - `trends -T "<topic>"` gives yearly volume and share of the corpus.
  - `authors -T "<topic>" --position last` gives recurring authors.
  - `stats` gives the corpus size.
- **Cite:** use `export --format bibtex`. Never write a BibTeX entry by hand.
- **Report the denominator:** state the profile, the scope and the counts with
  every number, in the form "N of M records, <profile>, <scope>". `stats` gives
  M for the whole profile.
- **Stay inside the snapshot:** never add a paper the corpus does not contain.
  If the answer needs one, say that it lies outside the declared scope.
- **Mind the limits:** 2026 is a partial year. Author rankings measure presence
  in this corpus, not quality or seniority. ESORICS and WOOT have low abstract
  coverage, so topic matches there are a lower bound.

## Change the code

- Python 3.11–3.14.
- Run `.venv/bin/python -m pytest -q` and `.venv/bin/ruff check .` before
  proposing a change.
- Never modify `data/profiles/*/papers.db.gz` or a published manifest. A new
  corpus is a new profile (`docs/PROFILE_REFRESH.md`).
- Every published result is bound to a frozen release (`docs/PAPERS.md`).
  Changes must keep `bash reproduce.sh --profile security-20` passing.
