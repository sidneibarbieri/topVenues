"""Flag DBLP front-matter records that are not research papers.

DBLP indexes a venue's whole publication stream, so for journals it also lists
quarterly editorials, obituaries, errata, and chairs' messages. These are not
research papers and carry no abstract by nature, so counting them as papers with
a missing abstract would misstate coverage. This module identifies them with a
conservative title rule (unambiguous front matter only, never a research paper
with a front-matter-like title) so a consumer can restrict the corpus to
research papers, and so the paper's coverage numbers stay auditable.

Run directly to list the flagged records and the resulting paper-level coverage.
The frozen snapshot is never modified; this is a read-only classifier.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

DATABASE = Path(__file__).resolve().parent.parent / "data" / "dataset" / "papers.db"

# Each pattern anchors at the start of the title and matches only unambiguous
# front matter. Kept deliberately narrow: a research paper whose title merely
# contains one of these words (for example "Message Time of Arrival Codes" or
# "Welcome to Jurassic Park") must not be flagged.
_FRONT_MATTER = re.compile(
    r"""^\s*(
        editorial[:\s]
      | editorial\s+(first|second|third|fourth|fifth|sixth)\b
      | in\s+memoriam\b
      | message\s+from\s+(the\s+)?(general|program|technical|steering)\b
      | guest\s+editorial\b | editor'?s\s+note\b | editor-in-chief\b
      | erratum\b | corrigendum\b | correction\s+to\b | retraction\b
      | front\s+matter\b | table\s+of\s+contents\b | title\s+page\b
      | program\s+committee\b | author\s+index\b | reviewers?\s+list\b
      | (welcome\s+(message|from|note))\b | foreword\b | preface\b
    )""",
    re.IGNORECASE | re.VERBOSE,
)


def is_front_matter(title: str) -> bool:
    """True when the title is unambiguous non-research front matter."""
    return bool(_FRONT_MATTER.match(title or ""))


def flag(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT paper_id, event, year, title, abstract FROM papers ORDER BY event, year, title"
    ).fetchall()
    return [r for r in rows if is_front_matter(r["title"])]


def main() -> int:
    if not DATABASE.exists():
        print(f"database not found at {DATABASE}; run `python -m src.cli stats` first", file=sys.stderr)
        return 1
    conn = sqlite3.connect(DATABASE)
    total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    has_abstract = "abstract IS NOT NULL AND TRIM(abstract) <> ''"
    abstracts = conn.execute(f"SELECT COUNT(*) FROM papers WHERE {has_abstract}").fetchone()[0]

    flagged = flag(conn)
    flagged_with_abstract = sum(1 for r in flagged if (r["abstract"] or "").strip())
    flagged_without_abstract = len(flagged) - flagged_with_abstract

    research = total - len(flagged)
    research_with_abstract = abstracts - flagged_with_abstract
    research_missing = research - research_with_abstract
    research_coverage = 100.0 * research_with_abstract / research

    print(f"records in snapshot:            {total}")
    print(f"non-research front matter:      {len(flagged)}")
    print(f"  with abstract text:           {flagged_with_abstract}")
    print(f"  without abstract:             {flagged_without_abstract}")
    print(f"research papers:                {research}")
    print(f"research papers with abstract:  {research_with_abstract}")
    print(f"research papers missing one:    {research_missing}")
    print(f"research-paper abstract coverage: {research_coverage:.2f}%")
    print()
    for r in flagged:
        mark = "abstract" if (r["abstract"] or "").strip() else "no-abstract"
        print(f"  {r['paper_id']:<9} {r['event'][:34]:<34} {r['year']}  {mark:<11} {r['title'][:52]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
