# TopVenues Reviewer Guide

> **Historical document.** Written for the tools-track artifact evaluation. The
> release it describes lives in the archived repository
> `sidneibarbieri/topvenues-tool`, so the commands below still work as written.
> In this repository the same claims reproduce with
> `bash reproduce.sh --profile security-20`; see [docs/PAPERS.md](../PAPERS.md).

This guide evaluates the immutable `security-20` profile used by the accepted SBSeg-SF paper. The current `security-20-v4` profile is a separate, post-publication successor documented in the main README; it does not alter the paper's frozen claims.

## What the release verifies

- 20,305 bibliographic records from 20 declared security and security-relevant venues;
- 17,491 abstract-enriched records and BibTeX for every record;
- the compressed SQLite snapshot SHA-256;
- 243 automated tests; and
- local FTS5 ranked search and a BibTeX export.

The artifact is a corpus-construction and review-workflow tool. Its evidence is limited to the declared snapshot and the workflows exercised below.

## Reproduce from a fresh clone

### Linux and macOS

Requires Python 3.11 or 3.12, Git, and Bash.

```bash
git clone --branch v1.0.1 https://github.com/sidneibarbieri/topvenues-tool.git
cd topvenues-tool
bash reproduce.sh
```

### Native Windows

Requires Python 3.11 or 3.12 with the Python Launcher (`py`), Git, and
PowerShell.

```powershell
git clone --branch v1.0.1 https://github.com/sidneibarbieri/topvenues-tool.git
cd topvenues-tool
powershell -ExecutionPolicy Bypass -File .\reproduce.ps1
```

Each command creates an isolated Python environment, verifies the immutable snapshot, materializes a disposable database, runs 243 tests, builds FTS5, exercises substring and ranked search, and exports a BibTeX sample. Network access is needed only to install Python dependencies on the first run; all validation after installation uses committed files.

Expected final line:

```text
Profile security-20 reproduced successfully
```

## Inspect the tool

```bash
source .venv/bin/activate
python -m streamlit run web/app.py
```

On Windows PowerShell, replace the activation command with
`.\.venv\Scripts\Activate.ps1`.

Open `http://localhost:8501`, inspect coverage, run a ranked search, and export a result set as BibTeX, CSV, or JSON. An [abstract-evidence capture](../assets/topvenues-abstract-search.pdf) applies the **Abstract contains** filter to `intrusion detection`; every displayed row has an abstract preview.

## Scope and limitations

The profile declares its venue identifiers in `profiles/security-20/config.yaml` and its snapshot identity in `data/profiles/security-20/manifest.json`. Venue names are part of the scientific scope and intentionally remain visible. Records lacking an abstract remain available for metadata and BibTeX workflows, but abstract-dependent queries must not treat them as abstract-enriched.

Live collection and enrichment are optional maintenance operations. They depend on external sources and are not required to validate this release. The public Hugging Face export is a Parquet representation of the same profile; its card records the snapshot SHA-256 and source tag.

## Reviewer boundary

No API key, publisher credential, paid service, GPU, or institutional access is needed for the reproduction command. The repository contains no author-private data or operational traces. Tool code is MIT licensed; third-party abstract text remains subject to its original source terms.
