"""SQLite database layer for complex paper queries."""

import sqlite3
from pathlib import Path

import pandas as pd

from .models import AbstractImportResult, Paper

MIN_ABSTRACT_LENGTH = 50


class DatabaseManager:
    """Manages an SQLite database of papers, supporting full-text search and export."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS papers (
                    score REAL,
                    paper_id TEXT PRIMARY KEY,
                    authors TEXT,
                    title TEXT,
                    venue TEXT,
                    pages TEXT,
                    year INTEGER,
                    paper_type TEXT,
                    access TEXT,
                    key TEXT,
                    ee TEXT,
                    url TEXT,
                    event TEXT,
                    abstract TEXT,
                    bibtex TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self._ensure_column(conn, "bibtex", "TEXT")
            for col in ("event", "year", "title", "abstract", "authors"):
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{col} ON papers({col})")

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, column: str, sql_type: str) -> None:
        """Add a column if missing — SQLite has no ``ALTER TABLE ADD COLUMN IF NOT EXISTS``."""
        existing = {row[1] for row in conn.execute("PRAGMA table_info(papers)").fetchall()}
        if column not in existing:
            conn.execute(f"ALTER TABLE papers ADD COLUMN {column} {sql_type}")

    _UPSERT_SQL = """
        INSERT INTO papers (
            score, paper_id, authors, title, venue, pages, year,
            paper_type, access, key, ee, url, event, abstract
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(paper_id) DO UPDATE SET
            score      = excluded.score,
            authors    = excluded.authors,
            title      = excluded.title,
            venue      = excluded.venue,
            pages      = excluded.pages,
            year       = excluded.year,
            paper_type = excluded.paper_type,
            access     = excluded.access,
            key        = excluded.key,
            ee         = excluded.ee,
            url        = excluded.url,
            event      = excluded.event,
            abstract   = COALESCE(papers.abstract, excluded.abstract),
            updated_at = CURRENT_TIMESTAMP
    """

    @staticmethod
    def _paper_row(paper: Paper) -> tuple:
        return (
            paper.score,
            paper.paper_id,
            paper.authors,
            paper.title,
            paper.venue,
            paper.pages,
            paper.year,
            paper.paper_type.value if paper.paper_type else None,
            paper.access,
            paper.key,
            paper.ee,
            paper.url,
            paper.event,
            paper.abstract,
        )

    def upsert_paper(self, paper: Paper) -> None:
        """Insert or update a single paper, preserving any existing abstract."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(self._UPSERT_SQL, self._paper_row(paper))

    def upsert_papers(self, papers: list[Paper]) -> int:
        """Insert or update papers in a single bulk transaction, preserving existing abstracts."""
        rows = [self._paper_row(p) for p in papers]
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(self._UPSERT_SQL, rows)
        return len(rows)

    _PAPER_TYPE_MAP: dict[str, str] = {
        "article": "article",
        "conference and workshop papers": "article",
        "inproceedings": "article",
        "proceedings": "proceedings",
        "editorship": "editorship",
    }

    def migrate_from_csv(self, csv_path: Path) -> int:
        """Migrate papers from a CSV file into the DB, preserving existing abstracts."""
        if not csv_path.exists():
            return 0

        df = pd.read_csv(csv_path)
        papers: list[Paper] = []
        for _, row in df.iterrows():
            title = row.get("Title") if pd.notna(row.get("Title")) else None
            year_raw = row.get("Year")
            if not title or not pd.notna(year_raw):
                continue
            paper_type_raw = str(row.get("Type", "")).lower() if pd.notna(row.get("Type")) else ""
            papers.append(Paper(
                score=row.get("Score") if pd.notna(row.get("Score")) else None,
                paper_id=str(row.get("ID", "")) if pd.notna(row.get("ID")) else "",
                authors=row.get("Authors") if pd.notna(row.get("Authors")) else None,
                title=title,
                venue=row.get("Venue") if pd.notna(row.get("Venue")) else None,
                pages=row.get("Pages") if pd.notna(row.get("Pages")) else None,
                year=int(year_raw),
                paper_type=self._PAPER_TYPE_MAP.get(paper_type_raw, "unknown"),
                access=row.get("Access") if pd.notna(row.get("Access")) else None,
                key=row.get("Key") if pd.notna(row.get("Key")) else None,
                ee=row.get("EE") if pd.notna(row.get("EE")) else None,
                url=row.get("URL") if pd.notna(row.get("URL")) else None,
                event=row.get("Event") if pd.notna(row.get("Event")) else None,
                abstract=row.get("Abstract") if pd.notna(row.get("Abstract")) else None,
            ))
        return self.upsert_papers(papers)

    def get_all_papers(self) -> list[dict]:
        """Return all papers as dicts with field names matching the Paper model."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM papers ORDER BY year DESC, event, title"
            ).fetchall()
        return [dict(row) for row in rows]

    def search(
        self,
        title_contains: str | None = None,
        abstract_contains: str | None = None,
        author_contains: str | None = None,
        event: str | None = None,
        year: int | None = None,
        technology: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        query = "SELECT * FROM papers WHERE 1=1"
        params: list = []

        if title_contains:
            query += " AND title LIKE ?"
            params.append(f"%{title_contains}%")
        if abstract_contains:
            query += " AND abstract LIKE ?"
            params.append(f"%{abstract_contains}%")
        if author_contains:
            query += " AND authors LIKE ?"
            params.append(f"%{author_contains}%")
        if event:
            query += " AND event = ?"
            params.append(event)
        if year:
            query += " AND year = ?"
            params.append(year)
        if technology:
            query += " AND (title LIKE ? OR abstract LIKE ?)"
            params.extend([f"%{technology}%", f"%{technology}%"])

        query += " ORDER BY year DESC, event, title"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, params).fetchall()]

    def get_statistics(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
            with_abstracts = conn.execute(
                "SELECT COUNT(*) FROM papers WHERE abstract IS NOT NULL AND abstract != ''"
            ).fetchone()[0]
            with_bibtex = conn.execute(
                "SELECT COUNT(*) FROM papers WHERE bibtex IS NOT NULL AND bibtex != ''"
            ).fetchone()[0]
            event_stats = conn.execute(
                "SELECT event, COUNT(*) FROM papers GROUP BY event ORDER BY COUNT(*) DESC"
            ).fetchall()
            year_stats = conn.execute(
                "SELECT year, COUNT(*) FROM papers GROUP BY year ORDER BY year DESC"
            ).fetchall()

        return {
            "total_papers": total,
            "with_abstracts": with_abstracts,
            "without_abstracts": total - with_abstracts,
            "with_bibtex": with_bibtex,
            "by_event": dict(event_stats),
            "by_year": dict(year_stats),
        }

    def export_to_csv(self, csv_path: Path) -> None:
        with sqlite3.connect(self.db_path) as conn:
            pd.read_sql_query("SELECT * FROM papers", conn).to_csv(
                csv_path, index=False, encoding="utf-8"
            )

    def get_paper_by_id(self, paper_id: str) -> dict | None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM papers WHERE paper_id = ?", (paper_id,)
            ).fetchone()
        return dict(row) if row else None

    def update_abstract(self, paper_id: str, abstract: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE papers SET abstract = ?, updated_at = CURRENT_TIMESTAMP WHERE paper_id = ?",
                (abstract, paper_id),
            )
        return cursor.rowcount > 0

    def update_bibtex(self, paper_id: str, bibtex: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE papers SET bibtex = ?, updated_at = CURRENT_TIMESTAMP WHERE paper_id = ?",
                (bibtex, paper_id),
            )
        return cursor.rowcount > 0

    def get_papers_without_bibtex(self, limit: int | None = None) -> list[dict]:
        query = ("SELECT * FROM papers WHERE (bibtex IS NULL OR bibtex = '') "
                 "AND key IS NOT NULL AND key != '' "
                 "ORDER BY year DESC, event, title")
        if limit:
            query += " LIMIT ?"
            params: tuple = (limit,)
        else:
            params = ()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, params).fetchall()]

    def import_abstracts_from_csv(self, csv_path: Path) -> AbstractImportResult:
        """Fill empty abstracts in the DB from a CSV. Existing abstracts are preserved.

        The CSV must expose at least an ``ID`` and ``Abstract`` column (the schema
        produced by the legacy R pipeline). Only rows whose abstract is at least
        ``MIN_ABSTRACT_LENGTH`` characters are considered. The operation is fully
        idempotent: re-running converges to the same state.
        """
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)

        df = pd.read_csv(csv_path, dtype={"ID": str})
        df = df[df["Abstract"].notna()]
        df = df[df["Abstract"].astype(str).str.len() >= MIN_ABSTRACT_LENGTH]
        candidates = list(zip(df["ID"], df["Abstract"], strict=True))

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DROP TABLE IF EXISTS _abstract_import")
            conn.execute(
                "CREATE TEMP TABLE _abstract_import "
                "(paper_id TEXT PRIMARY KEY, abstract TEXT NOT NULL)"
            )
            conn.executemany(
                "INSERT OR REPLACE INTO _abstract_import (paper_id, abstract) VALUES (?, ?)",
                candidates,
            )

            scanned = conn.execute("SELECT COUNT(*) FROM _abstract_import").fetchone()[0]
            matched = conn.execute(
                "SELECT COUNT(*) FROM _abstract_import i "
                "JOIN papers p ON p.paper_id = i.paper_id"
            ).fetchone()[0]
            already_full = conn.execute(
                "SELECT COUNT(*) FROM _abstract_import i "
                "JOIN papers p ON p.paper_id = i.paper_id "
                "WHERE p.abstract IS NOT NULL AND p.abstract != ''"
            ).fetchone()[0]

            cursor = conn.execute(
                """
                UPDATE papers
                   SET abstract = (SELECT abstract FROM _abstract_import
                                    WHERE paper_id = papers.paper_id),
                       updated_at = CURRENT_TIMESTAMP
                 WHERE (abstract IS NULL OR abstract = '')
                   AND paper_id IN (SELECT paper_id FROM _abstract_import)
                """
            )
            updated = cursor.rowcount

        return AbstractImportResult(
            scanned=scanned,
            matched=matched,
            updated=updated,
            skipped_existing=already_full,
            missing_in_db=scanned - matched,
        )

    def get_papers_without_abstracts(
        self,
        event: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        query = "SELECT * FROM papers WHERE (abstract IS NULL OR abstract = '')"
        params: list = []

        if event:
            query += " AND event = ?"
            params.append(event)
        query += " ORDER BY year DESC, event, title"
        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, params).fetchall()]
