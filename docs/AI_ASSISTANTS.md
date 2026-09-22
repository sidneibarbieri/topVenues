# Using TopVenues with AI assistants

An AI assistant answers literature questions fluently. Asked alone, it can
invent papers, miss recent ones, and give counts that nobody can check.
TopVenues fixes the part it cannot fix by itself: every paper comes from a
frozen snapshot with a SHA-256, and every count has a declared denominator.
The assistant does the reading, filtering and writing.

## Terminal agents: Claude Code, Codex CLI

These agents run commands, so they can drive the TopVenues CLI directly.

1. Clone and verify once:

   ```bash
   git clone https://github.com/sidneibarbieri/topVenues.git
   cd topVenues
   bash reproduce.sh --profile security-20-v4
   ```

2. Start the agent in the `topVenues` directory. It reads
   [`AGENTS.md`](../AGENTS.md) (Codex directly, Claude Code through
   `CLAUDE.md`). That file tells it to answer from the corpus, cite with
   `export --format bibtex`, and state the denominator with every number.

3. Ask a question in plain language. The agent translates it into commands such
   as:

   ```bash
   .venv/bin/python -m src.cli --profile security-20-v4 search --rank "prompt injection" --tier-scope "Security top-4" --limit 20
   .venv/bin/python -m src.cli --profile security-20-v4 trends -T "prompt injection"
   .venv/bin/python -m src.cli --profile security-20-v4 export --format bibtex -T "prompt injection" -o prompt-injection.bib
   ```

## Desktop apps: Claude Desktop, ChatGPT Desktop

- **With a terminal** (the Code tab in Claude Desktop, or an agent mode that
  runs commands): work as in the previous section, in the `topVenues` folder.
- **Chat only:** export the records first, then attach the file.
  1. Open the interface (`.venv/bin/python -m streamlit run web/app.py`),
     search, and use **Export CSV** or **Export JSON**.
  2. Attach the file to the conversation, with this first line:

  > The attached file is an export of TopVenues, profile security-20-v4.
  > Treat it as the complete population. Do not add papers from memory. Report
  > every count as "N of M records" and name the filter you applied.

## Without installing anything

The same corpus is a Hugging Face dataset. Notebook assistants and data agents
can load it with one line:

```python
from datasets import load_dataset

corpus = load_dataset("sidneibarbieri/topvenues", split="train")
```

## Prompts that work

| Goal | Ask |
| --- | --- |
| Candidate set for a review | "List every paper about *federated learning attacks* in the Security top-4 from 2021 to 2025. Give the count, the profile, and a BibTeX file." |
| Is a topic growing? | "Trace *prompt injection* year by year, as a share of the corpus. Flag partial years." |
| Where to publish or read | "Which declared venues publish most on *fuzzing* since 2022? Give counts per venue." |
| Who works on it | "Which authors recur as last author on *LLM security* in the Security top-4? Treat it as a shortlist, not a ranking of quality." |
| Check a claim | "Does the corpus contain a USENIX Security paper about *Rowhammer on mobile* in 2024? Answer from the corpus only." |
| Related work | "From the export, group the 40 papers by approach and write one paragraph per group, citing each paper by its DBLP key." |

## Good practice

- **Keep the profile in the conversation.** A number without its profile and
  scope cannot be checked.
- **Cite from the export.** The BibTeX that TopVenues exports comes from DBLP.
  Do not let the assistant retype entries.
- **Read the limits.** 2026 is a partial year. Author rankings measure presence
  in this corpus, never quality. Venues with low abstract coverage undercount
  topic matches. The **Evidence** page and
  [`ABSTRACT_COVERAGE.md`](ABSTRACT_COVERAGE.md) list them.
- **Reproduce before you publish.** A result that goes into a paper cites the
  profile and its SHA-256, as [`PAPERS.md`](PAPERS.md) shows for our own papers.
