# Published papers and the artifacts that reproduce them

The software evolves. The evidence behind a published paper does not.

Each paper below is bound to one release, one corpus snapshot and one SHA-256.
Its numbers are reproduced from that snapshot, never from whatever the current
release happens to ship. The corpora differ in size because they are different,
declared populations; none of them supersedes the evidence of another.

Continuous integration re-runs both reproductions on every change to `main`,
so a change that breaks a published result cannot pass.

> **About the DOIs.** SBC assigned both DOIs in the proceedings metadata, but as
> of September 2026 they are not yet registered with Crossref, so `doi.org` does
> not resolve them. The SOL links below are the working ones.

## At a glance

| I want to… | Go to |
| --- | --- |
| Use TopVenues today | [README](../README.md#quick-start), profile `security-20-v5` |
| Download the current corpus | [Hugging Face `sidneibarbieri/topvenues`](https://huggingface.co/datasets/sidneibarbieri/topvenues) |
| Reproduce the main-track paper | [Paper A](#paper-a--main-track-sbseg-2026) below |
| Reproduce the tools-track paper | [Paper B](#paper-b--tools-track-sbseg-2026) below |
| Explore the widest corpus | [Hugging Face `sidneibarbieri/topvenues-complete`](https://huggingface.co/datasets/sidneibarbieri/topvenues-complete) — exploratory, not a paper denominator |

---

## Paper A — main track, SBSeg 2026

**TopVenues: A Reproducible Corpus and Tooling Substrate for Cybersecurity
Literature Reviews.** Sidnei Barbieri, Ágney Lopes Roth Ferraz, Lourenço Alves
Pereira Júnior. *Anais do XXVI Simpósio Brasileiro de Cibersegurança (SBSeg
2026)*, pp. 1150–1165.
[Read on SOL](https://sol.sbc.org.br/index.php/sbseg/article/view/44350) ·
DOI `10.5753/sbseg.2026.29056`

| | |
| --- | --- |
| Repository | [`sidneibarbieri/topVenues`](https://github.com/sidneibarbieri/topVenues) |
| Release | `sbseg2026-camera-ready` |
| Snapshot | May 2026 · 9,925 records · 11 venues |
| SHA-256 (`papers.db.gz`) | `0f4dbaa97d0cf39abd2340adb3280643df090b5de9cd1a29bff39a0b53ef64cd` |
| Test suite | 252 tests |

```bash
git clone https://github.com/sidneibarbieri/topVenues
cd topVenues
git checkout sbseg2026-camera-ready
bash reproduce.sh
```

The paper prints the repository address; the release pins the code and data it
evaluated. Reproduces all 62 checked claims, among them: 9,925 records, 99.86%
abstract coverage, 99.99% BibTeX coverage, keyword search under 31 ms, the
252-test suite, 29.2% of 2024–2025 top-4 papers with a matching arXiv `cs.CR`
preprint and a median lead of 154 days, and the prior-authorship triage filter
at 16.5× relative risk (2.5× conventional lift) and 90.3% recall.

---

## Paper B — tools track, SBSeg 2026

**TopVenues: An Executable Corpus and Research Tool for Cybersecurity
Literature Reviews.** Sidnei Barbieri, Ágney Lopes Roth Ferraz, Lourenço Alves
Pereira Júnior. *Anais Estendidos do XXVI Simpósio Brasileiro de Cibersegurança
(SBSeg 2026) — Salão de Ferramentas*, pp. 234–241.
[Read on SOL](https://sol.sbc.org.br/index.php/sbseg_estendido/article/view/44470) ·
DOI `10.5753/sbseg_estendido.2026.33733`

| | |
| --- | --- |
| Repository printed in the paper | [`sidneibarbieri/topvenues-tool`](https://github.com/sidneibarbieri/topvenues-tool) (archived, read-only) |
| Release | `sbseg2026-sf-submission-r1` |
| Profile | `security-20` · 20,305 records · 20 venues · 2017–2026 |
| SHA-256 (`papers.db.gz`) | `5a35bd6e3ec6845a0fde4cc3d6aa05b1db04e511cb39e783eeaee2cea7493b08` |

The command printed in the paper:

```bash
git clone https://github.com/sidneibarbieri/topvenues-tool
cd topvenues-tool
bash reproduce.sh --profile security-20
```

The archived repository keeps answering that command unchanged. This
repository ships the same snapshot, so the same verification also runs here:

```bash
git clone https://github.com/sidneibarbieri/topVenues
cd topVenues
bash reproduce.sh --profile security-20
```

Reproduces: 20,305 records and 17,491 abstracts (86.1%), every record with a
BibTeX entry, the six numeric claims of the paper, and its Table 2 row by row.

---

## The current release

Not a paper. The corpus TopVenues ships today, maintained and extended.

| | |
| --- | --- |
| Repository | [`sidneibarbieri/topVenues`](https://github.com/sidneibarbieri/topVenues) |
| Profile | `security-20-v5` · 15,286 records · 20 venues · 2019–2026 |
| SHA-256 (`papers.db.gz`) | `2487ea98bfae38982d9752e56391bbdb83820f134c01b8555dfc7d26d8a9fe58` |
| Parquet export | [Hugging Face `sidneibarbieri/topvenues`](https://huggingface.co/datasets/sidneibarbieri/topvenues) |

`security-20-v5` is a successor of `security-20`, not a correction of Paper B:
exact-resource deduplication and a declared 2019–2026 window make it a
different population, with its own identity.

## One repository

TopVenues lives in `sidneibarbieri/topVenues`: the current tool on `main`, and
the main-track paper's artifact frozen at the `sbseg2026-camera-ready` release.
Until v1.10.0 the tool was developed in `sidneibarbieri/topvenues-tool`, the
address the tools-track paper prints. That repository is archived: its history
and tags remain, and its `main` still reproduces `security-20`, so the printed
command keeps working. New work happens only here, under the same continuous
integration that re-runs both papers, so quality can only move forward.
