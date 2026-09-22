"""Area-level analytics over the corpus: trends, prominent authors, award standouts.

Built on the deterministic venue-to-area registry (``areas.py``). Authors are parsed
from the stored author string; an author's per-area counts reflect the venues they
publish in. These functions support literature-review work: which areas are growing,
who the prominent authors are, and which papers were recognized with awards.
"""

from __future__ import annotations

import collections
import re
import sqlite3
from pathlib import Path

from src.areas import area_for
from src.awards import AwardMatch, load_award_records, match_awards_to_corpus
from src.sql_patterns import LIKE_ESCAPE_CLAUSE, contains_pattern
from src.tiers import TOP4, TOP4_REGIONAL, TOP_TIER, tier_for, weight_for

_AUTHOR_SPLIT = re.compile(r"\s*,\s*|\s+and\s+")
AUTHOR_POSITIONS = ("any", "first", "last")
PAPER_COUNT_METRIC = "paper_count"
TIER_WEIGHTED_METRIC = "tier_weighted"
TOP4_CONCENTRATION_METRIC = "top4_concentration"
AUTHOR_RANKING_METRICS = (
    PAPER_COUNT_METRIC,
    TIER_WEIGHTED_METRIC,
    TOP4_CONCENTRATION_METRIC,
)

# Concentration is a share of an author's whole corpus record, so neither the
# tier scope nor a thin record may distort it.
#
# The scope must not reach the denominator: counting inside a top-4-only scope
# makes every author 100% by construction, and counting inside any scope that
# excludes top-4 makes every author 0%. The scope decides who is eligible to
# appear; every paper still counts toward the ratio.
#
# A ratio over one paper is noise, and in this corpus 6,426 one-paper authors
# would otherwise score 100% and fill the ranking, so a body of work is required.
CONCENTRATION_MINIMUM_PAPERS = 10


def _split_authors(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [
        # Keep DBLP's numeric suffix. It distinguishes homonyms; removing it
        # silently merges different people and invalidates author rankings.
        name.strip()
        for name in _AUTHOR_SPLIT.split(raw)
        if name.strip()
    ]


def authors_at_position(raw: str | None, position: str) -> list[str]:
    """Return identities for one declared authorship-position view."""
    if position not in AUTHOR_POSITIONS:
        raise ValueError(f"unknown author position {position!r}")
    authors = _split_authors(raw)
    if position == "any":
        return authors
    if not authors:
        return []
    return [authors[0] if position == "first" else authors[-1]]


def top4_concentration(entry: dict) -> float:
    """Share of an author's corpus papers that appeared in a top-4 venue."""
    papers = entry["papers"]
    return entry["top4"] / papers if papers else 0.0


def _author_sort_key(entry: dict, ranking_metric: str) -> tuple:
    if ranking_metric == PAPER_COUNT_METRIC:
        return (-entry["papers"], -entry["top4"], -entry["score"], entry["author"])
    if ranking_metric == TOP4_CONCENTRATION_METRIC:
        return (-top4_concentration(entry), -entry["top4"], -entry["papers"], entry["author"])
    return (-entry["score"], -entry["top4"], -entry["papers"], entry["author"])


def area_year_counts(db_path: Path) -> dict[str, dict[int, int]]:
    """Papers per area per year: ``{area: {year: count}}`` (trend signal)."""
    counts: dict[str, dict[int, int]] = collections.defaultdict(
        lambda: collections.defaultdict(int)
    )
    connection = sqlite3.connect(db_path)
    try:
        for event, year in connection.execute(
            "select event, year from papers where year is not null"
        ):
            counts[area_for(event)][int(year)] += 1
    finally:
        connection.close()
    return {area: dict(sorted(years.items())) for area, years in counts.items()}


def top_authors(db_path: Path, area: str | None = None, limit: int = 15) -> list[tuple[str, int]]:
    """Most prolific authors, optionally restricted to a single area."""
    counter: collections.Counter[str] = collections.Counter()
    connection = sqlite3.connect(db_path)
    try:
        for authors, event in connection.execute("select authors, event from papers"):
            if area is not None and area_for(event) != area:
                continue
            counter.update(_split_authors(authors))
    finally:
        connection.close()
    return counter.most_common(limit)


def reference_authors(
    db_path: Path,
    topic: str | None = None,
    area: str | None = None,
    limit: int = 20,
    awards_dir: Path | None = None,
    position: str = "any",
    allowed_tiers: frozenset[str] | None = None,
    ranking_metric: str = PAPER_COUNT_METRIC,
) -> list[dict]:
    """Rank author visibility by paper count or an explicit tier heuristic.

    Paper count is the transparent default. The optional tier-weighted view
    contributes ``tiers.WEIGHT_BY_TIER`` for each paper, so recurring top-tier
    names dominate that view. The optional ``topic`` restricts scope with a
    case-insensitive match over title and abstract; ``area`` restricts by the
    venue-to-area registry. Deterministic: ties break by top-4 count, paper
    count, then name.

    This is a corpus-visibility heuristic, not a citation/quality/authority
    metric. Each entry exposes the evidence used by the heuristic so users do
    not have to interpret an opaque score.
    """
    if position not in AUTHOR_POSITIONS:
        raise ValueError(f"unknown author position {position!r}")
    if ranking_metric not in AUTHOR_RANKING_METRICS:
        raise ValueError(f"unknown author ranking metric {ranking_metric!r}")
    sql = "select paper_id, authors, event, year from papers where authors is not null"
    params: list = []
    if topic:
        sql += f" and (title like ? {LIKE_ESCAPE_CLAUSE} or abstract like ? {LIKE_ESCAPE_CLAUSE})"
        pattern = contains_pattern(topic)
        params.extend([pattern, pattern])

    awarded_ids: set[str] = set()
    if awards_dir is not None and awards_dir.exists():
        matched, _ = match_awards_to_corpus(load_award_records(awards_dir), db_path)
        awarded_ids = {match.paper_id for match in matched}

    counts_every_tier = ranking_metric == TOP4_CONCENTRATION_METRIC

    stats: dict[str, dict] = {}
    connection = sqlite3.connect(db_path)
    try:
        latest_year = connection.execute("SELECT MAX(year) FROM papers").fetchone()[0]
        recent_since = int(latest_year) - 2 if latest_year is not None else None
        for paper_id, authors, event, year in connection.execute(sql, params):
            if area is not None and area_for(event) != area:
                continue
            tier = tier_for(event)
            in_scope = allowed_tiers is None or tier in allowed_tiers
            if not in_scope and not counts_every_tier:
                continue
            weight = weight_for(tier)
            for author in authors_at_position(authors, position):
                entry = stats.setdefault(
                    author,
                    {
                        "author": author,
                        "score": 0.0,
                        "papers": 0,
                        "top4": 0,
                        "top_tier": 0,
                        "top4_regional": 0,
                        "awards": 0,
                        "recent_papers": 0,
                        "recent_since": recent_since,
                        "recent_through": latest_year,
                        "first_year": year,
                        "last_year": year,
                        "_venues": collections.Counter(),
                        "_in_scope": False,
                    },
                )
                entry["_in_scope"] = entry["_in_scope"] or in_scope
                entry["score"] += weight
                entry["papers"] += 1
                if tier == TOP4:
                    entry["top4"] += 1
                elif tier == TOP_TIER:
                    entry["top_tier"] += 1
                elif tier == TOP4_REGIONAL:
                    entry["top4_regional"] += 1
                if paper_id in awarded_ids:
                    entry["awards"] += 1
                if year is not None and recent_since is not None and year >= recent_since:
                    entry["recent_papers"] += 1
                if year is not None:
                    entry["first_year"] = min(entry["first_year"] or year, year)
                    entry["last_year"] = max(entry["last_year"] or year, year)
                if event:
                    entry["_venues"][event] += 1
    finally:
        connection.close()

    candidates = list(stats.values())
    if ranking_metric == TOP4_CONCENTRATION_METRIC:
        candidates = [
            entry
            for entry in candidates
            if entry["_in_scope"] and entry["papers"] >= CONCENTRATION_MINIMUM_PAPERS
        ]
    ranked = sorted(candidates, key=lambda entry: _author_sort_key(entry, ranking_metric))[:limit]
    for entry in ranked:
        entry.pop("_in_scope")
        entry["venues"] = [venue for venue, _ in entry.pop("_venues").most_common(3)]
        entry["score"] = round(entry["score"], 1)
        entry["top4_share"] = round(top4_concentration(entry), 3)
    return ranked


def topic_trend(
    db_path: Path,
    topic: str,
    area: str | None = None,
    year_start: int | None = None,
    allowed_tiers: frozenset[str] | None = None,
) -> dict:
    """Yearly volume and corpus share for a topic, plus its main venues.

    Answers two review questions at once: "is this topic growing?" and
    "where does it get published?". The topic matches title and abstract
    case-insensitively. ``share_pct`` normalizes each year's topic count by
    that year's corpus size (within the same ``area`` scope), so a growing
    corpus does not masquerade as a growing topic. Abstracts exist only where
    the curation policy backfills them, so counts are a lower bound for
    venues outside the enriched layers.
    """
    pattern = contains_pattern(topic)
    matched_by_year: dict[int, int] = collections.defaultdict(int)
    total_by_year: dict[int, int] = collections.defaultdict(int)
    venue_counter: collections.Counter[str] = collections.Counter()

    connection = sqlite3.connect(db_path)
    try:
        rows = connection.execute(
            f"select event, year, (title like ? {LIKE_ESCAPE_CLAUSE}"
            f" or abstract like ? {LIKE_ESCAPE_CLAUSE}) as matched "
            "from papers where year is not null",
            (pattern, pattern),
        )
        for event, year, matched in rows:
            if area is not None and area_for(event) != area:
                continue
            if allowed_tiers is not None and tier_for(event) not in allowed_tiers:
                continue
            year = int(year)
            if year_start is not None and year < year_start:
                continue
            total_by_year[year] += 1
            if matched:
                matched_by_year[year] += 1
                if event:
                    venue_counter[event] += 1
    finally:
        connection.close()

    by_year = [
        {
            "year": year,
            "papers": matched_by_year.get(year, 0),
            "share_pct": round(100.0 * matched_by_year.get(year, 0) / total, 2),
        }
        for year, total in sorted(total_by_year.items())
    ]
    return {
        "topic": topic,
        "total": sum(matched_by_year.values()),
        "by_year": by_year,
        "top_venues": venue_counter.most_common(5),
    }


def awarded_by_area(awards_dir: Path, db_path: Path) -> dict[str, list[AwardMatch]]:
    """Award-winning corpus papers grouped by research area."""
    matched, _ = match_awards_to_corpus(load_award_records(awards_dir), db_path)
    connection = sqlite3.connect(db_path)
    try:
        area_by_id = {
            paper_id: area_for(event)
            for paper_id, event in connection.execute("select paper_id, event from papers")
        }
    finally:
        connection.close()
    by_area: dict[str, list[AwardMatch]] = collections.defaultdict(list)
    for match in matched:
        by_area[area_by_id.get(match.paper_id, "unknown")].append(match)
    return dict(by_area)
