"""Check every quantitative claim the paper makes against the released snapshot.

`reproduce.sh` validates the headline results. This script covers the long tail:
per-venue coverage, corpus structure, case-study counts, export sizes and the
evaluation bundle. Each claim is executed, not asserted from memory, so a
reviewer can confirm the paper number and the artifact agree.

Exit code 0 when every claim holds; 1 on the first set of mismatches, which are
listed with the expected and observed values.
"""

from __future__ import annotations

import argparse
import collections
import csv
import gzip
import json
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATABASE = REPO_ROOT / "data" / "dataset" / "papers.db"
SNAPSHOT = REPO_ROOT / "data" / "dataset" / "papers.db.gz"
PROVENANCE = REPO_ROOT / "evaluation" / "output" / "abstract_provenance_evidence.csv"
PILOT = REPO_ROOT / "evaluation" / "baseline_validation" / "pilot_summary.json"
SAMPLE = REPO_ROOT / "evaluation" / "baseline_validation" / "sample.csv"
ADJUDICATION = REPO_ROOT / "evaluation" / "baseline_validation" / "manual_adjudication.csv"

CORE_A_STAR = ("USENIX Security", "ACM CCS", "IEEE S&P", "NDSS")
SURVEY_VENUES = (
    "ACM Computing Surveys",
    "IEEE Communications Surveys & Tutorials",
    "Foundations and Trends in Privacy and Security",
)
HAS_ABSTRACT = "abstract IS NOT NULL AND TRIM(abstract) <> ''"
HAS_BIBTEX = "bibtex IS NOT NULL AND TRIM(bibtex) <> ''"

# Per-venue coverage as printed in the paper's corpus table.
VENUE_COVERAGE = {
    "USENIX Security": 2054,
    "ACM CCS": 1972,
    "ACM Computing Surveys": 1734,
    "IEEE S&P": 1165,
    "IEEE Communications Surveys & Tutorials": 810,
    "NDSS": 794,
    "ACM ASIA CCS": 628,
    "IEEE EURO S&P": 339,
    "HotNets": 245,
    "ACM SACMAT": 176,
    "Foundations and Trends in Privacy and Security": 8,
}


class ClaimChecker:
    """Collects claim outcomes so every mismatch is reported, not just the first."""

    def __init__(self) -> None:
        self.failures: list[str] = []
        self.checked = 0

    def expect(self, claim: str, observed: object, expected: object) -> None:
        self.checked += 1
        if observed != expected:
            self.failures.append(f"{claim}: paper says {expected!r}, artifact yields {observed!r}")

    def expect_near(self, claim: str, observed: float, expected: float, tolerance: float) -> None:
        self.checked += 1
        if abs(observed - expected) > tolerance:
            self.failures.append(
                f"{claim}: paper says {expected} (+-{tolerance}), artifact yields {observed:.2f}"
            )


def keyword_matches(conn: sqlite3.Connection, term: str) -> int:
    pattern = f"%{term.lower()}%"
    return conn.execute(
        "SELECT COUNT(*) FROM papers WHERE LOWER(title) LIKE ? OR LOWER(COALESCE(abstract, '')) LIKE ?",
        (pattern, pattern),
    ).fetchone()[0]


def check_corpus_totals(conn: sqlite3.Connection, checker: ClaimChecker) -> None:
    scalar = lambda sql: conn.execute(sql).fetchone()[0]
    checker.expect("records in the snapshot", scalar("SELECT COUNT(*) FROM papers"), 9925)
    checker.expect(
        "non-empty abstracts", scalar(f"SELECT COUNT(*) FROM papers WHERE {HAS_ABSTRACT}"), 9911
    )
    checker.expect(
        "non-empty BibTeX entries", scalar(f"SELECT COUNT(*) FROM papers WHERE {HAS_BIBTEX}"), 9924
    )
    checker.expect(
        "records carrying both fields",
        scalar(f"SELECT COUNT(*) FROM papers WHERE {HAS_ABSTRACT} AND {HAS_BIBTEX}"),
        9910,
    )
    checker.expect(
        "abstract but no BibTeX",
        scalar(f"SELECT COUNT(*) FROM papers WHERE {HAS_ABSTRACT} AND NOT ({HAS_BIBTEX})"),
        1,
    )
    checker.expect(
        "BibTeX but no abstract",
        scalar(f"SELECT COUNT(*) FROM papers WHERE {HAS_BIBTEX} AND NOT ({HAS_ABSTRACT})"),
        14,
    )
    checker.expect_near(
        "abstract coverage",
        scalar(f"SELECT 100.0 * SUM(CASE WHEN {HAS_ABSTRACT} THEN 1 ELSE 0 END) / COUNT(*) FROM papers"),
        99.86,
        0.01,
    )
    checker.expect_near(
        "BibTeX coverage",
        scalar(f"SELECT 100.0 * SUM(CASE WHEN {HAS_BIBTEX} THEN 1 ELSE 0 END) / COUNT(*) FROM papers"),
        99.99,
        0.01,
    )


def check_corpus_structure(conn: sqlite3.Connection, checker: ClaimChecker) -> None:
    scalar = lambda sql: conn.execute(sql).fetchone()[0]
    checker.expect("distinct DBLP keys", scalar("SELECT COUNT(DISTINCT key) FROM papers"), 9925)
    checker.expect(
        "duplicate DOIs",
        scalar("SELECT COUNT(*) - COUNT(DISTINCT ee) FROM papers WHERE ee IS NOT NULL AND TRIM(ee) <> ''"),
        0,
    )
    checker.expect(
        "near-duplicate pairs retained",
        scalar(
            "SELECT COUNT(*) FROM (SELECT 1 FROM papers"
            " GROUP BY LOWER(TRIM(title)), LOWER(TRIM(authors)), year, pages HAVING COUNT(*) > 1)"
        ),
        5,
    )
    checker.expect("records before 2019", scalar("SELECT COUNT(*) FROM papers WHERE year < 2019"), 2)
    checker.expect("records dated 2026", scalar("SELECT COUNT(*) FROM papers WHERE year = 2026"), 397)
    checker.expect("papers in 2019", scalar("SELECT COUNT(*) FROM papers WHERE year = 2019"), 820)
    checker.expect("papers in 2025", scalar("SELECT COUNT(*) FROM papers WHERE year = 2025"), 1991)

    placeholders = ",".join("?" * len(CORE_A_STAR))
    top_tier_since_2019 = conn.execute(
        f"SELECT COUNT(*) FROM papers WHERE event IN ({placeholders}) AND year >= 2019", CORE_A_STAR
    ).fetchone()[0]
    # The paper states this one approximately ("nearly 6,000"), so check the band
    # it claims rather than asserting a precision the sentence does not carry.
    checker.expect(
        "top-tier papers since 2019 supporting \"nearly 6,000\"",
        5800 <= top_tier_since_2019 <= 6000,
        True,
    )

    no_2026_proceedings = conn.execute(
        f"SELECT COUNT(*) FROM papers WHERE event IN ({placeholders}) AND year = 2026", CORE_A_STAR
    ).fetchone()[0]
    checker.expect("2026 proceedings in the outcome venues", no_2026_proceedings, 0)

    recent_years = dict(
        conn.execute(
            "SELECT year, COUNT(*) FROM papers WHERE year BETWEEN 2023 AND 2025 GROUP BY year"
        ).fetchall()
    )
    outside_band = {y: n for y, n in recent_years.items() if not 1600 <= n <= 2000}
    checker.expect("recent full years within the 1,600-2,000 band", outside_band, {})

    missing = collections.Counter(
        row[0]
        for row in conn.execute(f"SELECT event FROM papers WHERE NOT ({HAS_ABSTRACT})").fetchall()
    )
    checker.expect("venues holding a record without an abstract", len(missing), 7)
    checker.expect(
        "records without an abstract in IEEE Communications Surveys",
        missing["IEEE Communications Surveys & Tutorials"],
        6,
    )


def check_venue_coverage(conn: sqlite3.Connection, checker: ClaimChecker) -> None:
    counts = dict(conn.execute("SELECT event, COUNT(*) FROM papers GROUP BY event").fetchall())
    for venue, expected in VENUE_COVERAGE.items():
        checker.expect(f"corpus table row for {venue}", counts.get(venue), expected)


def check_case_studies(conn: sqlite3.Connection, checker: ClaimChecker) -> None:
    checker.expect("intrusion-detection matches", keyword_matches(conn, "intrusion detection"), 120)
    checker.expect("threat-intelligence matches", keyword_matches(conn, "threat intelligence"), 36)
    checker.expect("LLM-security matches", keyword_matches(conn, "large language model"), 288)

    placeholders = ",".join("?" * len(SURVEY_VENUES))
    refined = conn.execute(
        "SELECT COUNT(*) FROM papers WHERE (LOWER(title) LIKE '%intrusion detection%'"
        " OR LOWER(COALESCE(abstract, '')) LIKE '%intrusion detection%')"
        f" AND year BETWEEN 2021 AND 2026 AND event NOT IN ({placeholders})",
        SURVEY_VENUES,
    ).fetchone()[0]
    checker.expect("intrusion-detection papers after refining to conference venues", refined, 68)

    cti = dict(
        conn.execute(
            "SELECT event, COUNT(*) FROM papers WHERE LOWER(title) LIKE '%threat intelligence%'"
            " OR LOWER(COALESCE(abstract, '')) LIKE '%threat intelligence%' GROUP BY event"
        ).fetchall()
    )
    expected_cti = {
        "ACM CCS": 12,
        "USENIX Security": 7,
        "ACM Computing Surveys": 4,
        "NDSS": 4,
        "IEEE Communications Surveys & Tutorials": 3,
        "IEEE EURO S&P": 3,
        "ACM ASIA CCS": 2,
        "ACM SACMAT": 1,
    }
    checker.expect("CTI distribution across venues", cti, expected_cti)

    llm_years = dict(
        conn.execute(
            "SELECT year, COUNT(*) FROM papers WHERE LOWER(title) LIKE '%large language model%'"
            " OR LOWER(COALESCE(abstract, '')) LIKE '%large language model%' GROUP BY year"
        ).fetchall()
    )
    checker.expect(
        "LLM-security papers per year", llm_years, {2021: 1, 2023: 10, 2024: 73, 2025: 152, 2026: 52}
    )

    cohort = conn.execute(
        "SELECT COUNT(*) FROM papers WHERE year IN (2024, 2025) AND event IN (?, ?, ?, ?)", CORE_A_STAR
    ).fetchone()[0]
    checker.expect("early-signal cohort", cohort, 2537)

    exported = conn.execute(
        f"SELECT COUNT(*) FROM papers WHERE year > 2022 AND {HAS_BIBTEX}"
    ).fetchone()[0]
    checker.expect("post-2022 entries serialised to BibTeX", exported, 5922)


def check_artifact_sizes(checker: ClaimChecker) -> None:
    mebibyte = 1024 * 1024
    checker.expect_near(
        "compressed snapshot size in MB", SNAPSHOT.stat().st_size / mebibyte, 15.0, 1.0
    )
    checker.expect_near(
        "materialised database size in MB", DATABASE.stat().st_size / mebibyte, 74.0, 1.0
    )


def check_evaluation_bundle(checker: ClaimChecker) -> None:
    with PROVENANCE.open(encoding="utf-8") as handle:
        statuses = collections.Counter(row["evidence_status"] for row in csv.DictReader(handle))
    supported = sum(count for status, count in statuses.items() if status.startswith("exact_"))
    checker.expect("abstracts with retained source evidence", supported, 8525)
    checker.expect(
        "abstracts without retained source evidence",
        statuses["unresolved_no_retained_source_evidence"],
        1386,
    )
    checker.expect("records without an abstract in the audit", statuses["missing_abstract"], 14)

    pilot = json.loads(PILOT.read_text(encoding="utf-8"))
    checker.expect("snapshot digest recorded by the pilot", pilot["snapshot"]["sha256"][:8], "0f4dbaa9")
    checker.expect("Semantic Scholar abstracts on the sample", pilot["semantic_scholar"]["abstract_n"], 163)
    checker.expect("OpenAlex abstracts on the sample", pilot["openalex"]["abstract_n"], 161)
    checker.expect(
        "Semantic Scholar abstracts agreeing at Jaccard 0.95",
        pilot["semantic_scholar"]["abstract_jaccard_ge_0_95_n"],
        145,
    )
    checker.expect(
        "OpenAlex abstracts agreeing at Jaccard 0.95",
        pilot["openalex"]["abstract_jaccard_ge_0_95_n"],
        120,
    )
    checker.expect("OpenAlex logical requests", pilot["openalex"]["logical_http_requests"], 200)
    checker.expect("Semantic Scholar logical requests", pilot["semantic_scholar"]["logical_http_requests"], 58)

    for label, path in (("released sample", SAMPLE), ("adjudication sheet", ADJUDICATION)):
        with path.open(encoding="utf-8") as handle:
            checker.expect(f"rows in the {label}", sum(1 for _ in csv.DictReader(handle)), 200)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true", help="print only the summary line")
    args = parser.parse_args()

    if not DATABASE.exists():
        if not SNAPSHOT.exists():
            print(f"No corpus found at {DATABASE} and no snapshot at {SNAPSHOT}.", file=sys.stderr)
            return 1
        with gzip.open(SNAPSHOT, "rb") as source, DATABASE.open("wb") as target:
            target.write(source.read())

    checker = ClaimChecker()
    with sqlite3.connect(DATABASE) as conn:
        check_corpus_totals(conn, checker)
        check_corpus_structure(conn, checker)
        check_venue_coverage(conn, checker)
        check_case_studies(conn, checker)
    check_artifact_sizes(checker)
    check_evaluation_bundle(checker)

    if checker.failures:
        print(f"{len(checker.failures)} of {checker.checked} paper claims disagree with the artifact:")
        for failure in checker.failures:
            print(f"  - {failure}")
        return 1
    if not args.quiet:
        print(f"All {checker.checked} paper claims reproduce from the released snapshot.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
