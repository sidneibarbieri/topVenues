"""Streamlit web interface for the bibliographic corpus explorer."""

import asyncio
import html
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# `streamlit run web/app.py` puts web/ on sys.path rather than the repository
# root, so every first-party import below depends on this running first.
ARTIFACT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ARTIFACT_ROOT))

from src.abstract_fetcher import AbstractFetcher
from src.analytics import CONCENTRATION_MINIMUM_PAPERS, authors_at_position
from src.areas import area_for
from src.awards import build_corpus_award_map
from src.chart_interactions import selected_chart_value
from src.collector import Collector
from src.database import require_corpus
from src.models import PaperClass, SearchFilters
from src.release_identity import identity_from_manifest
from src.reproduction_commands import SUPPORTED, command_for_profile, summary_line
from src.tiers import tier_for, tier_scope_options, tiers_in_scope
from web import charts


@dataclass(frozen=True)
class PageMovement:
    """One question a page answers, and the means it offers for answering it."""

    question: str
    answer: str


RESEARCH_QUESTIONS = (
    PageMovement(
        "Reference mapping",
        "Use Security top-4 to identify canonical venue papers and recurring authors.",
    ),
    PageMovement(
        "Review protocol",
        "Start with the declared full scope, then record any venue restriction as an "
        "inclusion decision.",
    ),
    PageMovement(
        "Research monitoring",
        "Use topic, year, tier, and author-position filters to decide which new work to inspect.",
    ),
)

# The order is the investigation: what the corpus holds, how a topic moves inside
# it, who produces that work, and where the corpus cannot answer at all.
INSIGHTS_MOVEMENTS = (
    PageMovement(
        "What is in it",
        "Papers per venue, year and record class. Click any bar to open those records in Search.",
    ),
    PageMovement(
        "How a topic moves",
        "Yearly volume and corpus share for one topic, normalized so corpus growth cannot read "
        "as topic growth.",
    ),
    PageMovement(
        "Who produces it",
        "Recurring authors by volume, tier weight or top-4 concentration, each row backed by "
        "its own trajectory and coauthorship evidence.",
    ),
    PageMovement(
        "Where it stays silent",
        "Abstract coverage per venue, so a gap is visible before it becomes an unrecorded "
        "exclusion.",
    ),
)

# Offered as one-click starting points. The trend section is otherwise an empty
# form, which hides the one view that normalizes a topic against corpus growth.
TREND_EXAMPLE_TOPICS = ("LLM", "ransomware", "fuzzing")

PAGE_SIZE_OPTIONS = (25, 50, 100, 200)
ABSTRACT_PREVIEW_CHARS = 280

BRAND_DIR = Path(__file__).resolve().parents[1] / "docs" / "brand"
BRAND_WORDMARK = BRAND_DIR / "topvenues-wordmark.svg"
BRAND_MARK = BRAND_DIR / "topvenues-mark.svg"
BRAND_ICON = BRAND_DIR / "mark-32.png"

st.set_page_config(
    page_title="TopVenues",
    page_icon=str(BRAND_ICON),
    layout="wide",
    initial_sidebar_state="expanded",
)
st.logo(str(BRAND_WORDMARK), size="large", icon_image=str(BRAND_MARK))


# ── Styles ────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
        :root {
            /* Brand tokens: docs/brand/BRAND.md. */
            --ink:     #10233F;
            --accent:  #2867B2;
            --slate:   #667085;
            --mist:    #E9EEF4;
            --surface: #F5F7FA;
            --border:  #D3DBE5;
            --bg:      #FFFFFF;
            --card:    #FFFFFF;
        }

        .stApp, [data-testid="stAppViewContainer"] {
            background: var(--bg);
            color: var(--ink);
        }
        [data-testid="stHeader"] { background: rgba(255, 255, 255, .88); }
        [data-testid="stMainBlockContainer"] { padding-top: 2.2rem; }
        [data-testid="stDeployButton"],
        [data-testid="stAppDeployButton"],
        [data-testid="stToolbarActions"],
        [data-testid="stMainMenu"],
        #MainMenu,
        footer {
            visibility: hidden;
            height: 0;
        }
        [data-testid="stExpandSidebarButton"],
        [data-testid="stSidebarCollapseButton"],
        [data-testid="stSidebarCollapseButton"] * {
            visibility: visible !important;
        }

        .app-header {
            background: var(--surface);
            border-left: 4px solid var(--accent);
            border-radius: 6px;
            padding: 1.35rem 1.6rem;
            margin-bottom: 1.4rem;
            border-top: 1px solid var(--border);
            border-right: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
        }
        .app-header h1 {
            color: var(--ink) !important; font-size: 1.75rem; font-weight: 650;
            margin: 0 0 .35rem; letter-spacing: -.01em;
        }
        .app-header p { color: var(--slate); font-size: .96rem; margin: 0; }


    .tag {
            display: inline-block; border-radius: 3px;
        padding: 2px 9px; font-size: .72rem; font-weight: 700;
        margin-right: 4px; letter-spacing: .3px;
    }
    .tag-sok      { background: #fff3cd; color: #7d5a00; }
    .tag-survey   { background: #d1ecf1; color: #0c5460; }
    .tag-poster   { background: #f8d7da; color: #721c24; }
    .tag-workshop { background: #e2d9f3; color: #3d2278; }
    .tag-short    { background: #e9ecef; color: #495057; }
    .tag-journal  { background: #d4edda; color: #155724; }
    .tag-article  { background: #f0f4f8; color: #0d1b2a; }

    section[data-testid="stSidebar"] {
        background: var(--surface);
        border-right: 1px solid var(--border);
    }
    section[data-testid="stSidebar"] * { color: var(--ink) !important; }
    section[data-testid="stSidebar"] h2 {
        color: var(--ink) !important; font-size: .95rem;
        letter-spacing: .4px; text-transform: uppercase;
        border-bottom: 1px solid var(--border);
        padding-bottom: .5rem; margin: .4rem 0 .8rem;
    }
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] p {
        color: var(--ink) !important;
    }

        .results-bar {
            display: flex; align-items: center; justify-content: space-between;
            background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
            padding: .7rem 1rem; margin-bottom: 1rem;
        }
        .results-bar .count { color: var(--ink); font-size: 1rem; font-weight: 700; }
    .results-bar .sub   { color: var(--slate); font-size: .85rem; }

        .paper-card {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 6px; padding: 1.35rem 1.55rem; margin-top: 1rem;
        }
        .paper-card h3 { color: var(--ink); margin: 0 0 .6rem; }
    .paper-meta {
        display: flex; gap: 1.5rem; color: var(--slate); font-size: .85rem;
        margin-bottom: .8rem; flex-wrap: wrap;
    }
    .paper-abstract {
        white-space: pre-wrap; line-height: 1.6;
        color: var(--ink); font-size: .95rem;
    }

        .claim-grid {
            display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
            gap: .8rem; margin: .9rem 0 1.2rem;
        }
        .claim {
            border: 1px solid var(--border); border-radius: 6px; background: var(--card);
            padding: .95rem 1rem;
        }
        .claim .name { color: var(--slate); text-transform: uppercase; font-size: .72rem; font-weight: 700; letter-spacing: .04em; }
        .claim .value { color: var(--ink); font-size: 1.6rem; font-weight: 700; line-height: 1.2; }
        .claim .note { color: var(--slate); font-size: .84rem; }
        .stDataFrame { border-radius: 6px; overflow: hidden; }
        div[data-testid="stExpander"] { border-radius: 6px; }
    .footer {
        color: var(--slate); font-size: .78rem; text-align: center;
        margin-top: 2rem; padding-top: 1rem; border-top: 1px solid var(--border);
    }
</style>
""",
    unsafe_allow_html=True,
)


# ── Helpers ────────────────────────────────────────────────────────────────


def _run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@st.cache_resource(show_spinner="Loading dataset…")
def _load_collector() -> Collector:
    """Open the corpus anchored at the artifact root, independent of the shell's
    working directory, and report a missing corpus instead of showing an empty one.
    """
    collector = Collector(base_dir=ARTIFACT_ROOT)
    require_corpus(collector.db.db_path, collector.db.snapshot_path)
    collector.papers = collector._load_papers_from_disk()
    return collector


@st.cache_data(show_spinner=False)
def _award_map() -> dict[str, list[str]]:
    """Cached map of corpus paper_id -> award labels (empty if no award tables)."""
    db_path = _load_collector().db.db_path
    awards_dir = db_path.parent.parent / "awards"
    if not awards_dir.exists():
        return {}
    return build_corpus_award_map(awards_dir, db_path)


@st.cache_data(show_spinner=False)
def _cached_topic_trend(db_path: str, topic: str, area: str | None, tier_scope: str) -> dict:
    from src.analytics import topic_trend

    return topic_trend(Path(db_path), topic, area=area, allowed_tiers=tiers_in_scope(tier_scope))


@st.cache_data(show_spinner=False)
def _cached_reference_authors(
    db_path: str,
    topic: str | None,
    area: str | None,
    position: str,
    tier_scope: str,
    ranking_metric: str,
    limit: int,
) -> list[dict]:
    from src.analytics import reference_authors

    return reference_authors(
        Path(db_path),
        topic=topic,
        area=area,
        position=position,
        limit=limit,
        awards_dir=Path(db_path).parent.parent / "awards",
        allowed_tiers=tiers_in_scope(tier_scope),
        ranking_metric=ranking_metric,
    )


@st.cache_data(show_spinner=False)
@st.cache_data(show_spinner=False)
def _cached_authorship_shifts(
    db_path: str, topic: str | None, area: str | None, tier_scope: str, limit: int
) -> list[dict]:
    from src.research_intelligence import authorship_shifts

    shifts = authorship_shifts(
        Path(db_path),
        topic=topic,
        area=area,
        allowed_tiers=tiers_in_scope(tier_scope),
        limit=limit,
    )
    return [shift.model_dump() for shift in shifts]


def _cached_emerging_researchers(
    db_path: str, topic: str | None, area: str | None, tier_scope: str, limit: int
) -> list[dict]:
    from src.research_intelligence import emerging_researchers

    return [
        signal.model_dump()
        for signal in emerging_researchers(
            Path(db_path),
            topic=topic,
            area=area,
            allowed_tiers=tiers_in_scope(tier_scope),
            limit=limit,
        )
    ]


@st.cache_data(show_spinner=False)
def _cached_audit_sample(db_path: str, sample_size: int) -> pd.DataFrame:
    from src.manual_audit import build_audit_sample

    return build_audit_sample(Path(db_path), sample_size=sample_size)


def _award_label(labels: list[str] | None) -> str:
    """Plain-text award label for the results table (no glyph, keeps sort/filter clean)."""
    return "; ".join(labels) if labels else ""


def _safe_html(value: object) -> str:
    if value is None:
        return "—"
    return html.escape(str(value), quote=True)


def _venue_options(collector: Collector, allowed_tiers: frozenset[str] | None = None) -> list[str]:
    venues = sorted(
        {
            paper.event
            for paper in collector.papers
            if paper.event and (allowed_tiers is None or tier_for(paper.event) in allowed_tiers)
        }
    )
    return ["All venues", *venues]


def _abstract_length_predicate(papers, choice: str):
    if choice == "Any":
        return papers
    if choice == "Has abstract":
        return [paper for paper in papers if paper.abstract]
    if choice == "Missing abstract":
        return [paper for paper in papers if not paper.abstract]
    if choice == "Short (≤ 150 words)":
        return [paper for paper in papers if 0 < paper.abstract_words <= 150]
    if choice == "Medium (151–300 words)":
        return [paper for paper in papers if 151 <= paper.abstract_words <= 300]
    if choice == "Long (> 300 words)":
        return [paper for paper in papers if paper.abstract_words > 300]
    return papers


def _bibtex_predicate(papers, only_with_bibtex: bool):
    return [paper for paper in papers if paper.bibtex] if only_with_bibtex else papers


def _truncate(text: str | None, max_chars: int = ABSTRACT_PREVIEW_CHARS) -> str:
    if not text:
        return "—"
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0] + "…"


def _class_badge(paper_class: PaperClass) -> str:
    return f'<span class="tag tag-{paper_class.value.lower()}">{paper_class.value}</span>'


def _render_header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="app-header"><h1>{title}</h1><p>{subtitle}</p></div>',
        unsafe_allow_html=True,
    )


@dataclass(frozen=True)
class HeadlineCard:
    """One figure in a page's headline row.

    Every page builds these from live corpus counts, so a card cannot state a
    number the corpus no longer holds.
    """

    name: str
    value: str
    note: str


def _percentage(part: int, whole: int) -> str:
    return f"{part / whole:.1%}" if whole else "n/a"


def _artifact_claims(stats: dict) -> tuple[HeadlineCard, ...]:
    """The headline row for the corpus as a whole."""
    total = stats["total_papers"]
    with_abstracts = stats["with_abstracts"]
    with_bibtex = stats.get("with_bibtex", 0)
    venues = len(stats.get("by_event", ()))
    years = sorted(year for year in stats.get("by_year", {}) if year)
    return (
        HeadlineCard("Corpus", f"{total:,}", f"cybersecurity papers across {venues} venues"),
        HeadlineCard(
            "Abstracts",
            _percentage(with_abstracts, total),
            f"{with_abstracts:,} searchable abstracts",
        ),
        HeadlineCard(
            "BibTeX", _percentage(with_bibtex, total), "records ready for citation export"
        ),
        HeadlineCard(
            "Coverage",
            f"{years[0]}\u2013{years[-1]}" if years else "n/a",
            "publication years in this snapshot",
        ),
    )


def _offer_example_topics() -> None:
    """Let a reader reach a live trend without inventing a query first.

    The topic field is a widget, so its session key cannot be written after it
    renders. The choice is parked instead and applied on the next run, the same
    way the search page receives a chart selection.
    """
    st.caption("No topic yet. Start from one of these, or type your own above.")
    for column, topic in zip(
        st.columns(len(TREND_EXAMPLE_TOPICS)), TREND_EXAMPLE_TOPICS, strict=True
    ):
        with column:
            if st.button(topic, key=f"trend_example_{topic}", use_container_width=True):
                st.session_state["pending_trend_topic"] = topic
                st.rerun()


def _render_movements(movements: tuple[PageMovement, ...]) -> None:
    """Name what a page is for before the reader meets its first chart."""
    for column, movement in zip(st.columns(len(movements)), movements, strict=True):
        with column:
            st.markdown(f"**{movement.question}**  ")
            st.caption(movement.answer)


def _render_card_row(cards: tuple[HeadlineCard, ...]) -> None:
    """Render a headline row. Every page uses this, so the rows stay identical.

    The HTML must stay flat: Streamlit runs markdown before inserting raw HTML,
    so any line indented four or more spaces becomes a code block.
    """
    markup = "".join(
        '<div class="claim">'
        f'<div class="name">{_safe_html(card.name)}</div>'
        f'<div class="value">{_safe_html(card.value)}</div>'
        f'<div class="note">{_safe_html(card.note)}</div>'
        "</div>"
        for card in cards
    )
    st.markdown(f'<div class="claim-grid">{markup}</div>', unsafe_allow_html=True)


def _corpus_cards(stats: dict, filtered_count: int | None = None) -> tuple[HeadlineCard, ...]:
    """The headline row for a page that filters the corpus.

    `filtered_count` adds a card naming how much of the corpus the current
    filters leave; it is omitted on pages that do not filter.
    """
    total = stats["total_papers"]
    with_abstracts = stats["with_abstracts"]
    with_bibtex = stats.get("with_bibtex", 0)
    venues = len(stats["by_event"])
    cards = (
        HeadlineCard("Papers indexed", f"{total:,}", f"across {venues} venues"),
        HeadlineCard(
            "With abstract", f"{with_abstracts:,}", f"{_percentage(with_abstracts, total)} coverage"
        ),
        HeadlineCard(
            "With BibTeX", f"{with_bibtex:,}", f"{_percentage(with_bibtex, total)} coverage"
        ),
    )
    if filtered_count is None:
        return cards
    return (
        *cards,
        HeadlineCard(
            "Currently shown",
            f"{filtered_count:,}",
            f"{_percentage(filtered_count, total)} of the corpus",
        ),
    )


def _reset_search_state() -> None:
    """Restore every search widget to a coherent default state."""
    defaults = {
        "search_ranked": "",
        "search_title": "",
        "search_abstract": "",
        "search_author": "",
        "search_author_position": "Any position",
        "search_topic": "",
        "search_area": "All areas",
        "search_tier_scope": "All declared venues",
        "search_venue": "All venues",
        "search_year": "All years",
        "search_class": [],
        "search_abstract_scope": "Any",
        "search_bibtex": False,
        "search_awards": False,
    }
    for key, value in defaults.items():
        st.session_state[key] = value
    st.session_state.pop("search_signature", None)
    st.session_state["page_no"] = 1


def _open_search_from_insight(
    venue: str | None = None,
    year: int | None = None,
    topic: str | None = None,
    author: str | None = None,
    author_position: str | None = None,
    area: str | None = None,
    paper_class: str | None = None,
    tier_scope: str | None = None,
) -> None:
    """Transfer one insight dimension into the search workflow."""
    _reset_search_state()
    st.session_state["search_venue"] = venue or "All venues"
    st.session_state["search_year"] = year or "All years"
    st.session_state["search_topic"] = topic or ""
    st.session_state["search_author"] = author or ""
    st.session_state["search_author_position"] = author_position or "Any position"
    st.session_state["search_area"] = area or "All areas"
    st.session_state["search_class"] = [paper_class] if paper_class else []
    st.session_state["search_tier_scope"] = tier_scope or "All declared venues"
    st.session_state["page"] = "Search"


def _queue_search_from_chart(**filters: str | int | None) -> None:
    """Navigate after a chart event without mutating an instantiated widget."""
    st.session_state["pending_search_navigation"] = filters
    st.rerun()


@st.cache_data(show_spinner=False)
def _release_identity(profile_id: str) -> dict:
    """Reader- and auditor-facing names for the active release."""
    manifest_path = ARTIFACT_ROOT / "data" / "profiles" / profile_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    identity = identity_from_manifest(profile_id, manifest)
    return {"reader": identity.reader_label, "auditor": identity.auditor_label}


def _count_with_share(counts: pd.Series) -> list[str]:
    """Label each bar with its count and its share, so rare rows still read.

    A class holding 86 of 14,859 records draws a bar under a pixel wide. The
    share is what tells a reader whether that class is worth a protocol
    decision, and no bar length can carry it at this range.
    """
    total = counts.sum()
    return [f"{count:,} ({count / total:.1%})" if total else f"{count:,}" for count in counts]


def _interactive_bar_chart(
    data: pd.DataFrame,
    category: str,
    value: str,
    key: str,
    height: int,
    *,
    horizontal: bool = True,
    sort: str | list | None = "-x",
    category_title: str | None = None,
    value_title: str | None = None,
    value_format: str = ",",
    color: str = charts.ACCENT,
    value_scale: alt.Scale | None = None,
    label_field: str | None = None,
) -> object | None:
    """Render a selectable bar chart and return the category the reader picked."""
    selection_name = f"{key}_selection"
    selection = alt.selection_point(selection_name, fields=[category], on="click", clear="dblclick")
    chart = charts.apply_theme(
        charts.bar_chart(
            data,
            category,
            value,
            selection,
            horizontal=horizontal,
            sort=sort,
            category_title=category_title,
            value_title=value_title,
            value_format=value_format,
            height=height,
            color=color,
            value_scale=value_scale,
            label_field=label_field,
        )
    )
    event = st.altair_chart(
        chart, key=key, on_select="rerun", selection_mode=selection_name, theme=None
    )
    return selected_chart_value(event, selection_name, category)


def _interactive_line_chart(
    data: pd.DataFrame,
    x_field: str,
    y_field: str,
    key: str,
    height: int,
    *,
    x_title: str | None = None,
    y_title: str | None = None,
    value_format: str = ",",
) -> object | None:
    """Render a selectable chronological chart and return the point picked."""
    selection_name = f"{key}_selection"
    selection = alt.selection_point(selection_name, fields=[x_field], on="click", clear="dblclick")
    chart = charts.apply_theme(
        charts.line_chart(
            data,
            x_field,
            y_field,
            selection,
            x_title=x_title,
            y_title=y_title,
            value_format=value_format,
            height=height,
        )
    )
    event = st.altair_chart(
        chart, key=key, on_select="rerun", selection_mode=selection_name, theme=None
    )
    return selected_chart_value(event, selection_name, x_field)


# ── Pages ──────────────────────────────────────────────────────────────────


def page_artifact() -> None:
    _render_header(
        "Reproducible corpus overview",
        "Reproduce the corpus, inspect coverage, and export ready-to-cite references from a local snapshot.",
    )
    _render_card_row(_artifact_claims(_load_collector().db.get_statistics()))

    st.subheader("Start from a research question")
    _render_movements(RESEARCH_QUESTIONS)

    st.subheader("Verification path")
    st.markdown(
        """
        1. Run the reproduction script to validate the headline claims.
        2. Use Search to inspect the corpus and export CSV, JSON or BibTeX.
        3. Use Insights to read scope, coverage, topic movement and author activity.
        4. Use Dataset lifecycle only when creating a new successor from live sources.
        """
    )

    active_profile = _load_collector().config.profile_id
    for tab, reproduction in zip(
        st.tabs([item.platform for item in SUPPORTED]), SUPPORTED, strict=True
    ):
        with tab:
            st.code(
                command_for_profile(reproduction, active_profile),
                language=reproduction.shell,
            )

    st.subheader("Reproducibility evidence")
    evidence = pd.DataFrame(
        [
            {
                "Criterion": "Availability",
                "Evidence": "Source code, compressed SQLite snapshot, manifest, and reviewer documentation.",
            },
            {
                "Criterion": "Functionality",
                "Evidence": "CLI, Streamlit interface, search, statistics and CSV/JSON/BibTeX export.",
            },
            {
                "Criterion": "Reproducibility",
                "Evidence": "One command validates the manifest, materializes the snapshot, runs tests, exercises search, and exports BibTeX.",
            },
            {
                "Criterion": "Sustainability",
                "Evidence": "Small Python/SQLite stack, typed models and configuration-driven corpus scope.",
            },
        ]
    )
    st.dataframe(evidence, width="stretch", hide_index=True)

    st.subheader("Release evidence")
    findings = pd.DataFrame(
        [
            {
                "Check": "Snapshot identity",
                "Result": "The manifest records counts and SHA-256 for the compressed SQLite release.",
                "Use": summary_line(),
            },
            {
                "Check": "Search and export",
                "Result": "FTS5 ranking, filters, and CSV/JSON/BibTeX exports run against the verified local copy.",
                "Use": "Search page or python -m src.cli",
            },
        ]
    )
    st.dataframe(findings, width="stretch", hide_index=True)


# A venue whose abstracts were never harvested looks like a venue without
# research on the topic. The coverage table lives on the Insights page, far
# from where the risk is actually taken, so the gap is also stated here at the
# moment an abstract query is run.
ABSTRACT_COVERAGE_FLOOR = 0.95


@st.cache_data(show_spinner=False)
def _abstract_coverage_by_venue(db_path: str) -> dict[str, tuple[int, int]]:
    """Per venue: records carrying an abstract, and records in total."""
    coverage: dict[str, tuple[int, int]] = {}
    with sqlite3.connect(db_path) as conn:
        for venue, total, with_abstract in conn.execute(
            "SELECT event, COUNT(*), "
            "SUM(CASE WHEN abstract IS NOT NULL AND TRIM(abstract) <> '' THEN 1 ELSE 0 END) "
            "FROM papers GROUP BY event"
        ):
            if total:
                coverage[venue] = (int(with_abstract or 0), int(total))
    return coverage


def _warn_about_abstract_coverage(db_path: str, venue_choice: str) -> None:
    """Say plainly which venues an abstract query cannot speak for."""
    coverage = _abstract_coverage_by_venue(db_path)
    if not coverage:
        return

    if venue_choice != "All venues":
        entry = coverage.get(venue_choice)
        if not entry:
            return
        with_abstract, total = entry
        if with_abstract / total < ABSTRACT_COVERAGE_FLOOR:
            st.warning(
                f"**{venue_choice}** stores an abstract for {with_abstract:,} of "
                f"{total:,} records ({with_abstract / total:.1%}). This query cannot "
                f"reach the other {total - with_abstract:,}, so a small result set "
                "here means missing text, not absent research."
            )
        return

    weak = sorted(
        (
            (venue, hit, total)
            for venue, (hit, total) in coverage.items()
            if hit / total < ABSTRACT_COVERAGE_FLOOR
        ),
        key=lambda item: item[1] / item[2],
    )
    if not weak:
        return
    unreachable = sum(total - hit for _, hit, total in weak)
    listed = ", ".join(f"{venue} {hit / total:.0%}" for venue, hit, total in weak[:4])
    more = f", and {len(weak) - 4} more" if len(weak) > 4 else ""
    st.warning(
        f"Abstract coverage is uneven: {listed}{more}. This query cannot reach "
        f"{unreachable:,} records that carry no abstract, so venue counts are not "
        "comparable without accounting for that gap."
    )


def page_search() -> None:
    collector = _load_collector()
    stats = collector.db.get_statistics()

    with st.sidebar:
        st.markdown("## Filters")

        with st.expander("Text", expanded=True):
            rank_query = st.text_input(
                "Ranked search (BM25)",
                placeholder="e.g., memory corruption mitigations",
                help="Relevance-ranked full-text search over title, abstract and "
                "authors. Overrides the substring filters below.",
                key="search_ranked",
            )
            title_query = st.text_input(
                "Title contains", placeholder="e.g., authentication", key="search_title"
            )
            abstract_query = st.text_input(
                "Abstract contains", placeholder="e.g., LLM, SGX", key="search_abstract"
            )
            author_query = st.text_input(
                "Author contains", placeholder="e.g., Sekar", key="search_author"
            )
            author_position = st.selectbox(
                "Author position",
                ["Any position", "First author", "Last author"],
                key="search_author_position",
                help="Applied only when Author contains is set.",
            )
            tech_query = st.text_input(
                "Topic / tech", placeholder="e.g., blockchain, 5G", key="search_topic"
            )

        with st.expander("Venue & year", expanded=True):
            area_choice = st.selectbox(
                "Research area",
                ["All areas", "security", "ai", "networks", "mobile", "systems", "cross-area"],
                key="search_area",
            )
            tier_scope = st.selectbox(
                "Venue tier scope",
                tier_scope_options(),
                help="Security top-4: ACM CCS, IEEE S&P, USENIX Security, and NDSS.",
                key="search_tier_scope",
            )
            allowed_tiers = tiers_in_scope(tier_scope)
            venue_options = _venue_options(collector, allowed_tiers)
            if st.session_state.get("search_venue") not in venue_options:
                st.session_state["search_venue"] = "All venues"
            venue_choice = st.selectbox("Venue", venue_options, key="search_venue")
            year_choice = st.selectbox(
                "Year", ["All years", *sorted(stats["by_year"], reverse=True)], key="search_year"
            )

        with st.expander("Paper class", expanded=False):
            class_choices = st.multiselect(
                "Include",
                [c.value for c in PaperClass],
                help="Filter by SoK, Survey, Poster, Workshop, Short, Journal or Article.",
                key="search_class",
            )

        with st.expander("Abstract & citation", expanded=False):
            abstract_length = st.selectbox(
                "Abstract availability / length",
                [
                    "Any",
                    "Has abstract",
                    "Missing abstract",
                    "Short (≤ 150 words)",
                    "Medium (151–300 words)",
                    "Long (> 300 words)",
                ],
                key="search_abstract_scope",
            )
            only_with_bibtex = st.checkbox(
                "Has BibTeX",
                help="Only include papers whose BibTeX entry has been fetched.",
                key="search_bibtex",
            )

        with st.expander("Awards", expanded=False):
            awards_only = st.checkbox(
                "Award winners only",
                help="Only papers with a recorded Best or Distinguished Paper award.",
                key="search_awards",
            )

        st.button("Reset all filters", on_click=_reset_search_state, width="stretch")

        st.markdown("## Display")
        page_size = st.select_slider(
            "Results per page", options=PAGE_SIZE_OPTIONS, value=50, key="search_page_size"
        )
        sort_choice = st.selectbox(
            "Sort by",
            ["Relevance", "Year (newest first)", "Year (oldest first)", "Title (A–Z)", "Venue"],
            help="Relevance follows the ranked-search order and falls back to "
            "newest-first when no ranked query is set.",
            key="search_sort",
        )

    if abstract_query:
        _warn_about_abstract_coverage(str(collector.db.db_path), venue_choice)

    filters = SearchFilters()
    if title_query:
        filters.title_contains = title_query
    if abstract_query:
        filters.abstract_contains = abstract_query
    if author_query:
        filters.author_contains = author_query
    if tech_query:
        filters.technology = tech_query
    if venue_choice != "All venues":
        filters.event = venue_choice
    if year_choice != "All years":
        filters.year = int(year_choice)
    allowed_tiers = tiers_in_scope(tier_scope)

    award_map = _award_map()
    if rank_query:
        # Relevance mode: BM25 over the FTS index, best match first. Venue and
        # year go into the SQL; the remaining filters apply below as usual.
        from src.models import Paper

        ranked_rows = collector.db.search_ranked(
            rank_query,
            event=filters.event,
            year=filters.year,
            limit=None,
        )
        results = [
            Paper(
                **{
                    field: row[field]
                    for field in Paper.model_fields
                    if field in row and row[field] is not None
                }
            )
            for row in ranked_rows
        ]
    else:
        results = collector.search(filters, limit=None)
    if allowed_tiers is not None:
        results = [paper for paper in results if tier_for(paper.event) in allowed_tiers]
    if area_choice != "All areas":
        results = [paper for paper in results if area_for(paper.event) == area_choice]
    if author_query and author_position != "Any position":
        position_key = {"First author": "first", "Last author": "last"}[author_position]
        normalized_query = author_query.casefold()
        results = [
            paper
            for paper in results
            if any(
                normalized_query in author.casefold()
                for author in authors_at_position(paper.authors, position_key)
            )
        ]
    if class_choices:
        wanted = {PaperClass(value) for value in class_choices}
        results = [p for p in results if p.paper_class in wanted]
    results = _abstract_length_predicate(results, abstract_length)
    results = _bibtex_predicate(results, only_with_bibtex)
    if awards_only:
        results = [paper for paper in results if paper.paper_id in award_map]

    if rank_query and sort_choice == "Relevance":
        pass  # search_ranked already returns best-first order
    elif sort_choice == "Year (newest first)":
        results.sort(key=lambda p: (-(p.year or 0), p.title or ""))
    elif sort_choice == "Year (oldest first)":
        results.sort(key=lambda p: (p.year or 0, p.title or ""))
    elif sort_choice == "Title (A–Z)":
        results.sort(key=lambda p: (p.title or "").lower())
    elif sort_choice == "Venue":
        results.sort(key=lambda p: (p.event or "", -(p.year or 0)))

    _render_header(
        "Security Paper Explorer",
        "Search a curated dataset from the configured security literature scope.",
    )
    _render_card_row(_corpus_cards(stats, filtered_count=len(results)))
    active_venue = venue_choice if venue_choice != "All venues" else tier_scope
    release = _release_identity(collector.config.profile_id or "")
    st.caption(f"Active venue scope: {active_venue}. Corpus: {release['reader']}.")

    if not results:
        st.info("No papers match the current filters. Try widening the search.")
        return

    total_pages = max(1, (len(results) + page_size - 1) // page_size)
    search_signature = (
        rank_query,
        title_query,
        abstract_query,
        author_query,
        author_position,
        tech_query,
        area_choice,
        venue_choice,
        year_choice,
        tier_scope,
        tuple(class_choices),
        abstract_length,
        only_with_bibtex,
        awards_only,
        page_size,
        sort_choice,
    )
    if st.session_state.get("search_signature") != search_signature:
        st.session_state["page_no"] = 1
        st.session_state["search_signature"] = search_signature
    st.session_state["page_no"] = min(
        max(1, int(st.session_state.get("page_no", 1))),
        total_pages,
    )

    col_count, col_page = st.columns([3, 1])
    with col_page:
        if total_pages > 1:
            nav_prev, nav_next = st.columns(2)
            with nav_prev:
                if st.button("‹", disabled=st.session_state["page_no"] <= 1, width="stretch"):
                    st.session_state["page_no"] -= 1
                    st.rerun()
            with nav_next:
                if st.button(
                    "›", disabled=st.session_state["page_no"] >= total_pages, width="stretch"
                ):
                    st.session_state["page_no"] += 1
                    st.rerun()
            page = int(st.number_input("Page", 1, total_pages, key="page_no"))
        else:
            page = 1

    start = (page - 1) * page_size
    end = min(len(results), start + page_size)
    page_slice = results[start : start + page_size]
    with col_count:
        st.markdown(
            f'<div class="results-bar">'
            f'<span class="count">{len(results):,} papers found</span>'
            f'<span class="sub">showing {start + 1:,}–{end:,} · '
            f"page {page} of {total_pages} · {page_size} per page</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

    table_rows = [
        {
            "Title": paper.title or "—",
            "Authors": (paper.authors or "—")[:90]
            + ("…" if paper.authors and len(paper.authors) > 90 else ""),
            "Venue": paper.event or "—",
            "Tier": tier_for(paper.event),
            "Year": paper.year,
            "Award": _award_label(award_map.get(paper.paper_id)),
            "Class": paper.paper_class.value,
            "Words": paper.abstract_words,
            "Abstract": _truncate(paper.abstract),
            "Cite": paper.cite_command or "—",
            "Link": paper.ee or paper.url or "",
        }
        for paper in page_slice
    ]
    df = pd.DataFrame(table_rows)

    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        height=min(700, 70 + len(df) * 56),
        column_config={
            "Title": st.column_config.TextColumn("Title", width="medium"),
            "Authors": st.column_config.TextColumn("Authors", width="small"),
            "Venue": st.column_config.TextColumn("Venue", width="small"),
            "Tier": st.column_config.TextColumn("Tier", width="small"),
            "Year": st.column_config.NumberColumn("Year", format="%d", width="small"),
            "Award": st.column_config.TextColumn("Award", width="small"),
            "Class": st.column_config.TextColumn("Class", width="small"),
            "Words": st.column_config.NumberColumn("Words", format="%d", width="small"),
            "Abstract": st.column_config.TextColumn("Abstract preview", width="large"),
            "Cite": st.column_config.TextColumn("\\cite{…}", width="small"),
            "Link": st.column_config.LinkColumn("DOI / URL", width="small", display_text="open"),
        },
    )

    full_rows = [
        {
            "title": paper.title,
            "authors": paper.authors,
            "first_author": paper.first_author,
            "venue": paper.event,
            "tier": tier_for(paper.event),
            "year": paper.year,
            "class": paper.paper_class.value,
            "abstract_words": paper.abstract_words,
            "doi": paper.doi,
            "ee": paper.ee,
            "url": paper.url,
            "abstract": paper.abstract,
            "cite_key": paper.cite_key,
            "bibtex": paper.bibtex,
        }
        for paper in results
    ]
    full_df = pd.DataFrame(full_rows)
    bib_text = "\n\n".join(paper.bibtex for paper in results if paper.bibtex)
    col_csv, col_json, col_bib, _ = st.columns([1, 1, 1, 3])
    with col_csv:
        st.download_button(
            "Export CSV",
            full_df.to_csv(index=False).encode("utf-8"),
            "topvenues_results.csv",
            "text/csv",
            width="stretch",
        )
    with col_json:
        st.download_button(
            "Export JSON",
            full_df.to_json(orient="records", indent=2),
            "topvenues_results.json",
            "application/json",
            width="stretch",
        )
    with col_bib:
        st.download_button(
            "Export BibTeX",
            bib_text or "% no BibTeX entries available",
            "topvenues_results.bib",
            "application/x-bibtex",
            width="stretch",
            disabled=not bib_text,
            help="LaTeX bibliography file with one BibTeX entry per result."
            if bib_text
            else "BibTeX not yet fetched for any paper in this result set.",
        )

    st.divider()
    st.subheader("Paper details")
    title_options = [f"[{paper.year}] {paper.title}" for paper in page_slice]
    selected_label = st.selectbox(
        "Select a paper from this page", title_options, label_visibility="collapsed"
    )
    if selected_label:
        idx = title_options.index(selected_label)
        paper = page_slice[idx]
        link = paper.ee or paper.url
        link_html = (
            f'<a href="{_safe_html(link)}" target="_blank">{_safe_html(link)}</a>' if link else "—"
        )
        doi_html = (
            f'<a href="https://doi.org/{_safe_html(paper.doi)}" target="_blank">{_safe_html(paper.doi)}</a>'
            if paper.doi
            else "—"
        )
        abstract_html = (
            _safe_html(paper.abstract) if paper.abstract else "<i>No abstract available.</i>"
        )
        st.markdown(
            '<div class="paper-card">'
            f"<h3>{_safe_html(paper.title)}</h3>"
            '<div class="paper-meta">'
            f"<span><b>Authors:</b> {_safe_html(paper.authors)}</span>"
            f"<span><b>Venue:</b> {_safe_html(paper.event)}</span>"
            f"<span><b>Tier:</b> {_safe_html(tier_for(paper.event))}</span>"
            f"<span><b>Year:</b> {_safe_html(paper.year)}</span>"
            f"<span><b>Words:</b> {paper.abstract_words:,}</span>"
            f"<span><b>DOI:</b> {doi_html}</span>"
            f"<span><b>Link:</b> {link_html}</span>"
            "</div>"
            f"<div>{_class_badge(paper.paper_class)}</div>"
            '<hr style="border:none; border-top:1px solid var(--border); margin:1rem 0">'
            f'<div class="paper-abstract">{abstract_html}</div>'
            "</div>",
            unsafe_allow_html=True,
        )

        if paper.bibtex:
            st.markdown("**BibTeX**")
            st.code(paper.bibtex, language="bibtex")
            col_cite, _ = st.columns([1, 3])
            with col_cite:
                st.code(paper.cite_command or "", language="latex")
        else:
            st.caption("BibTeX not yet fetched. Run `python -m src.cli bibtex` to populate.")


def page_insights() -> None:
    pending_topic = st.session_state.pop("pending_trend_topic", None)
    if pending_topic:
        st.session_state["trend_topic"] = pending_topic
    collector = _load_collector()
    stats = collector.db.get_statistics()
    _render_header(
        "Dataset insights",
        "Read the corpus in four passes: what it holds, how a topic moves, who produces "
        "that work, and where it cannot answer.",
    )
    _render_card_row(_corpus_cards(stats))
    _render_movements(INSIGHTS_MOVEMENTS)
    st.divider()

    st.subheader("Papers by venue")
    venue_df = pd.DataFrame(
        [
            {"Venue": k, "Papers": v}
            for k, v in sorted(stats["by_event"].items(), key=lambda x: x[1], reverse=True)
        ]
    )
    selected_venue = _interactive_bar_chart(venue_df, "Venue", "Papers", "venue_chart", 520)
    st.caption("Click a bar to open that venue's records. Double-click clears the selection.")
    if selected_venue:
        _queue_search_from_chart(venue=str(selected_venue))

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Papers by year")
        year_df = pd.DataFrame(
            [{"Year": k, "Papers": v} for k, v in sorted(stats["by_year"].items())]
        )
        selected_year = _interactive_bar_chart(
            year_df,
            "Year",
            "Papers",
            "year_chart",
            460,
            horizontal=False,
            sort="ascending",
        )
        st.caption("Click a bar to open that year's records. Double-click clears the selection.")
        partial_years = sorted(set(collector.config.partial_years) & set(stats["by_year"]))
        if partial_years:
            partial_year_labels = ", ".join(str(year) for year in partial_years)
            st.caption(
                f"Partial publication year(s) in this frozen release: {partial_year_labels}. "
                "Compare completed years before inferring annual growth."
            )
        if selected_year is not None:
            _queue_search_from_chart(year=int(selected_year))

    with col2:
        st.subheader("Papers by class")
        class_counts = {}
        for paper in collector.papers:
            class_counts[paper.paper_class.value] = class_counts.get(paper.paper_class.value, 0) + 1
        class_df = pd.DataFrame(
            [
                {"Class": k, "Papers": v}
                for k, v in sorted(class_counts.items(), key=lambda x: x[1], reverse=True)
            ]
        )
        class_df["Label"] = _count_with_share(class_df["Papers"])
        selected_class = _interactive_bar_chart(
            class_df, "Class", "Papers", "class_chart", 320, label_field="Label"
        )
        st.caption(
            "Full articles dominate the corpus; the rare classes are what a protocol "
            "usually decides to include or exclude. Click a bar to open records in that class."
        )
        if selected_class:
            _queue_search_from_chart(paper_class=str(selected_class))

    st.divider()
    st.subheader("Topic trend")
    st.caption(
        "Yearly volume and corpus share for a topic (title/abstract match), "
        "with the venues that publish it most. Share normalizes by each "
        "year's corpus size, so corpus growth does not masquerade as topic "
        "growth. Counts are a lower bound outside the abstract-enriched "
        "layers."
    )
    col_trend_topic, col_trend_area, col_trend_tier = st.columns([2, 1, 1.4])
    with col_trend_topic:
        trend_topic = st.text_input(
            "Topic",
            placeholder="e.g., LLM, ransomware, fuzzing",
            key="trend_topic",
        )
    with col_trend_area:
        trend_area = st.selectbox(
            "Area",
            ["All areas", "security", "ai", "networks", "mobile", "systems", "cross-area"],
            key="trend_area",
        )
    with col_trend_tier:
        trend_tier_scope = st.selectbox(
            "Venue tier scope",
            tier_scope_options(),
            key="trend_tier_scope",
        )
    if trend_topic:
        trend = _cached_topic_trend(
            str(collector.db.db_path),
            trend_topic,
            None if trend_area == "All areas" else trend_area,
            trend_tier_scope,
        )
        if trend["total"]:
            trend_df = pd.DataFrame(trend["by_year"]).set_index("year")
            col_abs, col_share = st.columns(2)
            with col_abs:
                st.caption(f"Papers per year — {trend['total']:,} total")
                selected_trend_year = _interactive_bar_chart(
                    trend_df.reset_index().rename(columns={"year": "Year", "papers": "Papers"}),
                    "Year",
                    "Papers",
                    "trend_chart",
                    280,
                    horizontal=False,
                    sort="ascending",
                )
            with col_share:
                st.caption("Share of the year's corpus (%)")
                selected_share_year = _interactive_line_chart(
                    trend_df.reset_index().rename(
                        columns={"year": "Year", "share_pct": "Share (%)"}
                    ),
                    "Year",
                    "Share (%)",
                    "trend_share_chart",
                    280,
                    value_format=".1f",
                )
            venues = " · ".join(f"{event} ({count:,})" for event, count in trend["top_venues"])
            st.markdown(f"**Main venues:** {venues}")
            partial_trend_years = sorted(
                set(collector.config.partial_years) & {row["year"] for row in trend["by_year"]}
            )
            if partial_trend_years:
                partial_year_labels = ", ".join(str(year) for year in partial_trend_years)
                st.caption(
                    f"Interpret {partial_year_labels} as partial-year observations, not as "
                    "completed annual trends."
                )
            selected_topic_year = (
                selected_trend_year if selected_trend_year is not None else selected_share_year
            )
            if selected_topic_year is not None:
                _queue_search_from_chart(
                    year=int(selected_topic_year),
                    topic=trend_topic,
                    tier_scope=trend_tier_scope,
                )
        else:
            st.info("No papers match this topic in the selected scope.")
    else:
        _offer_example_topics()

    st.divider()
    st.subheader("Researcher Radar")
    st.caption(
        "Discover researchers who recur in the selected corpus. Paper count is the "
        "transparent default; the optional tier-weighted view gives top-4 papers "
        "more weight. Neither view measures citations, quality, seniority, or authority. "
        "DBLP identity suffixes are preserved to avoid merging homonyms."
    )
    st.caption(
        "Choose all, first, or last authorship position to answer different "
        "literature-review questions; none is a proxy for citation impact or seniority."
    )
    col_topic, col_area, col_tier, col_position, col_metric, col_n = st.columns(
        [2, 1, 1.35, 1, 1.25, 0.75]
    )
    with col_topic:
        author_topic = st.text_input(
            "Topic (title/abstract contains)",
            placeholder="e.g., LLM, fuzzing",
            key="authors_topic",
        )
    with col_area:
        author_area = st.selectbox(
            "Area",
            ["All areas", "security", "ai", "networks", "mobile", "systems", "cross-area"],
            key="authors_area",
        )
    with col_tier:
        author_tier_scope = st.selectbox(
            "Venue tier scope",
            tier_scope_options(),
            key="authors_tier_scope",
            help="Use Security top-4 to identify recurring authors in CCS, S&P, USENIX Security, and NDSS only.",
        )
    with col_position:
        author_position = st.selectbox(
            "Authorship",
            ["Any author", "First author", "Last author"],
            key="authors_position",
            help="Rank all appearances, first-author appearances, or last-author appearances.",
        )
    with col_metric:
        author_metric = st.selectbox(
            "Ranking metric",
            ["Paper count", "Tier-weighted visibility", "Top-4 concentration"],
            key="authors_metric",
            help=(
                "Paper count answers the frequency ranking. Tier-weighted visibility "
                "favours volume at strong venues. Top-4 concentration asks a different "
                "question: what share of an author's work appears in ACM CCS, IEEE S&P, "
                "USENIX Security or NDSS. It considers only authors with at least "
                f"{CONCENTRATION_MINIMUM_PAPERS} papers, because a ratio over one paper "
                "is noise. The share always spans the author's whole record in this "
                "corpus, so the venue scope above selects which authors are listed "
                "without changing the number."
            ),
        )
    with col_n:
        author_limit = st.number_input("Authors", 5, 50, 15, key="authors_limit")

    ranked_authors = _cached_reference_authors(
        str(collector.db.db_path),
        author_topic or None,
        None if author_area == "All areas" else author_area,
        {"Any author": "any", "First author": "first", "Last author": "last"}[author_position],
        author_tier_scope,
        {
            "Paper count": "paper_count",
            "Tier-weighted visibility": "tier_weighted",
            "Top-4 concentration": "top4_concentration",
        }[author_metric],
        int(author_limit),
    )
    if ranked_authors:
        author_table = pd.DataFrame(
            [
                {
                    "#": position,
                    "Author": entry["author"],
                    "Tier-weighted score": entry["score"],
                    "Papers": entry["papers"],
                    "Top-4": entry["top4"],
                    "Top-4 share": f"{entry.get('top4_share', 0):.0%}",
                    "Other top-tier": entry["top_tier"],
                    "Top-4 regional": entry["top4_regional"],
                    "Awards": entry["awards"],
                    f"Recent ({entry['recent_since']}–{entry['recent_through']})": entry[
                        "recent_papers"
                    ],
                    "Active": f"{entry['first_year']}–{entry['last_year']}",
                    "Main venues": ", ".join(entry["venues"]),
                    "Tier scope": author_tier_scope,
                    "Topic": author_topic or "",
                    "Area": "" if author_area == "All areas" else author_area,
                    "Authorship": author_position,
                    "Ranking metric": author_metric,
                }
                for position, entry in enumerate(ranked_authors, start=1)
            ]
        )
        st.caption(
            f"Active scope: {author_tier_scope} · {author_position.lower()} · "
            f"{author_area} · {author_metric.lower()} · topic: {author_topic or 'any'}"
        )
        st.dataframe(
            author_table.drop(
                columns=["Tier scope", "Topic", "Area", "Authorship", "Ranking metric"]
            ),
            width="stretch",
            hide_index=True,
        )
        selected_author = st.selectbox(
            "Inspect an author's corpus records", [entry["author"] for entry in ranked_authors]
        )
        open_col, export_col = st.columns([1, 1])
        with open_col:
            st.button(
                "Open author records",
                key="open_author_records",
                on_click=_open_search_from_insight,
                kwargs={
                    "author": selected_author,
                    "author_position": author_position,
                    "topic": author_topic or None,
                    "area": None if author_area == "All areas" else author_area,
                    "tier_scope": author_tier_scope,
                },
                width="stretch",
            )
        with export_col:
            st.download_button(
                "Download author shortlist (CSV)",
                author_table.to_csv(index=False).encode("utf-8"),
                file_name="topvenues-author-shortlist.csv",
                mime="text/csv",
                width="stretch",
            )

        st.markdown("#### Follow the evidence forward")
        st.caption(
            "Trajectory and coauthorship use exact identities in this snapshot. The arXiv link "
            "is an external name search, not a verified cross-source identity match."
        )
        from src.research_intelligence import (
            ResearchWatchlist,
            arxiv_author_search_url,
            collaboration_network,
            researcher_trajectory,
            watchlist_matching_ids,
        )

        trajectory = researcher_trajectory(collector.db.db_path, selected_author)
        trajectory_frame = pd.DataFrame(
            [
                {
                    "Year": point.year,
                    "Papers": point.papers,
                    "First author": point.first_author_papers,
                    "Last author": point.last_author_papers,
                    "Venues": ", ".join(point.venues),
                }
                for point in trajectory
            ]
        )
        trajectory_col, collaboration_col = st.columns(2)
        with trajectory_col:
            st.caption(f"Publication trajectory — {selected_author}")
            if not trajectory_frame.empty:
                trajectory_long = trajectory_frame.melt(
                    id_vars=["Year", "Venues"],
                    value_vars=["Papers", "First author", "Last author"],
                    var_name="Measure",
                    value_name="Count",
                )
                trajectory_chart = (
                    alt.Chart(trajectory_long)
                    .mark_line(point=True, strokeWidth=2.3)
                    .encode(
                        x=alt.X(
                            "Year:O",
                            sort="ascending",
                            axis=alt.Axis(labelAngle=0),
                            title="Year",
                        ),
                        y=alt.Y("Count:Q", title="Papers", scale=alt.Scale(zero=True)),
                        color=alt.Color(
                            "Measure:N",
                            scale=alt.Scale(
                                domain=["Papers", "First author", "Last author"],
                                range=list(charts.SERIES),
                            ),
                            title=None,
                            legend=charts.series_legend(),
                        ),
                        strokeDash=alt.StrokeDash(
                            "Measure:N",
                            scale=alt.Scale(
                                domain=["Papers", "First author", "Last author"],
                                range=[list(dash) for dash in charts.SERIES_DASH],
                            ),
                            title=None,
                            legend=charts.series_legend(),
                        ),
                        tooltip=["Year:O", "Measure:N", "Count:Q", "Venues:N"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(
                    trajectory_chart,
                    width="stretch",
                    theme=None,
                )
                with st.expander("Trajectory evidence"):
                    st.dataframe(trajectory_frame, width="stretch", hide_index=True)
        with collaboration_col:
            collaborations = collaboration_network(collector.db.db_path, selected_author)
            collaboration_frame = pd.DataFrame(
                [
                    {
                        "Collaborator": item.collaborator,
                        "Joint papers": item.joint_papers,
                        "Active": f"{item.first_year}–{item.last_year}",
                        "Main venues": ", ".join(item.venues),
                    }
                    for item in collaborations
                ]
            )
            st.caption("Direct collaboration evidence")
            if collaboration_frame.empty:
                st.info("No direct coauthors in the selected corpus.")
            else:
                st.dataframe(collaboration_frame.head(10), width="stretch", hide_index=True)

        arxiv_col, watch_col = st.columns(2)
        with arxiv_col:
            st.link_button(
                "Search this name on arXiv",
                arxiv_author_search_url(selected_author),
                width="stretch",
            )
        with watch_col:
            watchlist = ResearchWatchlist(
                profile_id=collector.config.profile_id or "unknown",
                name=f"Research watch — {author_topic or selected_author}",
                authors=[selected_author],
                topics=[author_topic] if author_topic else [],
                tier_scope=author_tier_scope,
            )
            watchlist.known_paper_ids = watchlist_matching_ids(collector.db.db_path, watchlist)
            st.download_button(
                "Download portable watchlist",
                watchlist.model_dump_json(indent=2).encode("utf-8"),
                file_name="topvenues-watchlist.json",
                mime="application/json",
                width="stretch",
            )

        st.markdown("#### Newly leading a group")
        st.caption(
            "Authors who used to publish in the first byline position and now publish in the "
            "last one, within the windows shown. Use it to shortlist names to look into, not "
            "as a finding: it reads byline position only, and cannot see appointments, "
            "seniority, group size, or a group's own authorship conventions."
        )
        shifts = _cached_authorship_shifts(
            str(collector.db.db_path),
            author_topic or None,
            None if author_area == "All areas" else author_area,
            author_tier_scope,
            int(author_limit),
        )
        if shifts:
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Author": shift["author"],
                            f"First author ({shift['early_window']})": shift["early_first"],
                            f"Last author ({shift['recent_window']})": shift["recent_last"],
                            "Leads at": ", ".join(shift["venues"]),
                        }
                        for shift in shifts
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
        else:
            st.caption("No authorship shift meets the declared thresholds in this scope.")

        st.markdown("#### Emerging activity")
        st.caption(
            "Ranks the increase in annual paper rate during the latest three corpus years versus "
            "earlier years. This is a descriptive monitoring signal, not a forecast of impact."
        )
        emerging = _cached_emerging_researchers(
            str(collector.db.db_path),
            author_topic or None,
            None if author_area == "All areas" else author_area,
            author_tier_scope,
            int(author_limit),
        )
        emerging_frame = pd.DataFrame(
            [
                {
                    "Author": item["author"],
                    f"Recent papers ({item['recent_since']}–{item['through_year']})": item[
                        "recent_papers"
                    ],
                    "Earlier papers": item["prior_papers"],
                    "Recent papers/year": item["recent_rate"],
                    "Earlier papers/year": item["prior_rate"],
                    "Rate change": item["rate_change"],
                    "Observed": f"{item['first_year']}–{item['last_year']}",
                }
                for item in emerging
            ]
        )
        st.dataframe(emerging_frame, width="stretch", hide_index=True)
        if not emerging_frame.empty:
            emerging_author = st.selectbox(
                "Inspect emerging-activity evidence",
                emerging_frame["Author"].tolist(),
                key="emerging_author",
            )
            st.button(
                "Open emerging-author records",
                key="open_emerging_author_records",
                on_click=_open_search_from_insight,
                kwargs={
                    "author": emerging_author,
                    "topic": author_topic or None,
                    "area": None if author_area == "All areas" else author_area,
                    "tier_scope": author_tier_scope,
                },
            )
    else:
        st.info("No authors match the current topic/area scope.")

    st.divider()
    st.subheader("Abstract coverage by venue")
    rows = []
    with sqlite3.connect(collector.db.db_path) as conn:
        for venue, total in stats["by_event"].items():
            with_abs = conn.execute(
                "SELECT COUNT(*) FROM papers "
                "WHERE event=? AND abstract IS NOT NULL AND abstract!=''",
                (venue,),
            ).fetchone()[0]
            rows.append(
                {
                    "Venue": venue,
                    "Total": total,
                    "With abstract": with_abs,
                    "Coverage": f"{with_abs / total * 100:.1f}%" if total else "—",
                }
            )
    coverage_df = pd.DataFrame(rows)
    coverage_df["Coverage (%)"] = coverage_df["Coverage"].str.rstrip("%").astype(float)
    selected_coverage_venue = _interactive_bar_chart(
        coverage_df,
        "Venue",
        "Coverage (%)",
        "coverage_chart",
        460,
        color=charts.COVERAGE,
    )
    st.caption("Click a coverage bar to inspect the venue's records and missing abstracts.")
    if selected_coverage_venue:
        _queue_search_from_chart(venue=str(selected_coverage_venue))
    st.dataframe(coverage_df.drop(columns="Coverage (%)"), width="stretch", hide_index=True)


def _audit_choice(value: object) -> str:
    normalized = str(value).strip().casefold()
    if normalized in {"yes", "true", "1", "y"}:
        return "Yes"
    if normalized in {"no", "false", "0", "n"}:
        return "No"
    return "Unlabelled"


def _render_manual_audit(sample: pd.DataFrame, profile_id: str) -> None:
    from src.manual_audit import (
        append_audit_decision,
        load_audit_progress,
        save_audit_progress,
        summarize_audit,
    )

    progress_path = (
        ARTIFACT_ROOT / "output" / "manual_audit" / f"{profile_id}-{len(sample)}-progress.csv"
    )
    decision_log_path = progress_path.with_name(f"{profile_id}-{len(sample)}-decisions.jsonl")
    audit_frame = load_audit_progress(sample, progress_path)
    summary = summarize_audit(audit_frame)
    completion = summary.labelled / summary.sampled if summary.sampled else 0.0
    st.progress(completion, text=f"{summary.labelled}/{summary.sampled} records completed")
    metric_columns = st.columns(3)
    metric_columns[0].metric("Completed", summary.labelled)
    metric_columns[1].metric("Remaining", summary.sampled - summary.labelled)
    metric_columns[2].metric(
        "Usable among completed",
        f"{summary.usable_rate:.1%}" if summary.usable_rate is not None else "—",
    )
    st.caption(
        f"Progress is saved atomically to `{progress_path.relative_to(ARTIFACT_ROOT)}`; "
        f"decision provenance is append-only in "
        f"`{decision_log_path.relative_to(ARTIFACT_ROOT)}`."
    )

    if "audit_position" not in st.session_state:
        labelled_mask = audit_frame[
            ["label_complete", "label_uncontaminated", "label_matches_paper"]
        ].apply(lambda column: column.map(_audit_choice).ne("Unlabelled"))
        incomplete = labelled_mask.all(axis=1).loc[lambda values: ~values].index.tolist()
        st.session_state.audit_position = int(incomplete[0]) if incomplete else 0

    position = min(max(int(st.session_state.audit_position), 0), len(audit_frame) - 1)
    navigation = st.columns([1, 2, 1])
    if navigation[0].button("← Previous", disabled=position == 0, width="stretch"):
        st.session_state.audit_position = position - 1
        st.rerun()
    navigation[1].markdown(
        f"<div style='text-align:center;padding:.45rem'><strong>Record "
        f"{position + 1} of {len(audit_frame)}</strong></div>",
        unsafe_allow_html=True,
    )
    if navigation[2].button("Next →", disabled=position == len(audit_frame) - 1, width="stretch"):
        st.session_state.audit_position = position + 1
        st.rerun()

    row = audit_frame.iloc[position]
    st.markdown(f"#### {html.escape(str(row['title']))}")
    st.caption(
        f"{row['venue']} · {row['year']} · paper_id {row['paper_id']} · "
        f"abstract present: {'yes' if row['abstract_present'] else 'no'}"
    )
    if str(row["source_url"]).strip():
        st.link_button("Open publisher/source record", str(row["source_url"]), width="stretch")
    st.text_area(
        "Extracted abstract",
        value=str(row["abstract"]),
        height=240,
        disabled=True,
        key=f"audit_abstract_{row['sample_id']}",
    )
    st.caption(
        "Complete = not truncated. Uncontaminated = no navigation, captions, or unrelated text. "
        "Matches paper = source title and abstract refer to this exact work."
    )

    choices = ("Unlabelled", "Yes", "No")
    decision_mode_labels = {
        "Human only": "human_only",
        "Human-supervised, Codex-assisted": "human_supervised_codex_assisted",
    }
    prior_decisions = audit_frame.iloc[:position].loc[
        lambda frame: frame["decision_mode"].astype(str).str.strip().ne("")
    ]
    prior_reviewer = (
        str(prior_decisions.iloc[-1]["reviewer"]).strip()
        if not prior_decisions.empty
        else "Sidnei Barbieri"
    )
    # Provenance is a claim about who judged THIS record, so it is never carried
    # forward from the previous one. Letting it persist silently attributed 141
    # of 200 records to an assistant the operator had not selected for them.
    stored_mode = str(row["decision_mode"]).strip() or "human_only"
    selected_mode_label = next(
        label for label, value in decision_mode_labels.items() if value == stored_mode
    )
    with st.form(f"audit_form_{row['sample_id']}"):
        decision_mode_label = st.selectbox(
            "Decision mode",
            tuple(decision_mode_labels),
            index=tuple(decision_mode_labels).index(selected_mode_label),
            help="Describes who judged this record. It resets to human-only for each record and is never inherited from the previous one.",
        )
        reviewer = st.text_input(
            "Reviewer",
            value=str(row["reviewer"]).strip()
            or st.session_state.get("audit_reviewer")
            or prior_reviewer,
        )
        label_complete = st.radio(
            "Is the abstract complete?",
            choices,
            index=choices.index(_audit_choice(row["label_complete"])),
            horizontal=True,
        )
        label_uncontaminated = st.radio(
            "Is the abstract uncontaminated?",
            choices,
            index=choices.index(_audit_choice(row["label_uncontaminated"])),
            horizontal=True,
        )
        label_matches_paper = st.radio(
            "Does the abstract match this exact paper?",
            choices,
            index=choices.index(_audit_choice(row["label_matches_paper"])),
            horizontal=True,
        )
        notes = st.text_area("Notes (optional)", value=str(row["notes"]), height=90)
        save_and_next = st.form_submit_button("Save decision and open next", width="stretch")

    if save_and_next:
        selected_labels = (label_complete, label_uncontaminated, label_matches_paper)
        if not reviewer.strip():
            st.error("Enter the human reviewer's name before saving.")
        elif "Unlabelled" in selected_labels:
            st.error("Answer all three questions before saving this record.")
        else:
            audit_frame.loc[position, "label_complete"] = label_complete.casefold()
            audit_frame.loc[position, "label_uncontaminated"] = label_uncontaminated.casefold()
            audit_frame.loc[position, "label_matches_paper"] = label_matches_paper.casefold()
            audit_frame.loc[position, "reviewer"] = reviewer.strip()
            audit_frame.loc[position, "decision_mode"] = decision_mode_labels[decision_mode_label]
            audit_frame.loc[position, "notes"] = notes.strip()
            save_audit_progress(audit_frame, progress_path)
            append_audit_decision(
                audit_frame.loc[position],
                profile_id=profile_id,
                sample_size=len(audit_frame),
                progress_path=progress_path.relative_to(ARTIFACT_ROOT),
                decision_log_path=decision_log_path,
            )
            st.session_state.audit_reviewer = reviewer.strip()
            st.session_state.audit_decision_mode = decision_mode_labels[decision_mode_label]
            st.session_state.audit_position = min(position + 1, len(audit_frame) - 1)
            st.rerun()

    st.download_button(
        "Download current audit progress (CSV)",
        audit_frame.to_csv(index=False).encode("utf-8"),
        file_name=progress_path.name,
        mime="text/csv",
        width="stretch",
    )
    if summary.labelled:
        st.caption(
            f"{summary.usable}/{summary.labelled} completed records currently satisfy all three "
            f"criteria. Partial rows are excluded from the estimate."
        )


def _render_audit_workbench() -> None:
    """Render the optional annotation and import controls."""
    st.markdown(
        "TopVenues generates a deterministic, venue-stratified sample. Each extracted abstract "
        "must be compared with the linked source and labelled for completeness, contamination, "
        "and paper identity. Human-only and human-supervised assisted decisions are recorded "
        "separately; unsupervised automated labels do not satisfy this protocol."
    )
    collector = _load_collector()
    audit_size = st.number_input(
        "Audit sample size", min_value=20, max_value=500, value=200, step=20
    )
    sample = _cached_audit_sample(str(collector.db.db_path), int(audit_size))
    _render_manual_audit(sample, collector.config.profile_id)
    with st.expander("Import an externally completed audit sheet"):
        uploaded_audit = st.file_uploader(
            "Upload completed annotation sheet",
            type=["csv"],
            help="Accepted labels: yes/no, true/false, 1/0. Partially labelled rows are excluded.",
        )
        if uploaded_audit is None:
            return

        from src.manual_audit import summarize_audit

        uploaded_frame = pd.read_csv(uploaded_audit, keep_default_na=False)
        uploaded_summary = summarize_audit(uploaded_frame)
        if uploaded_summary.labelled == 0:
            st.warning("The uploaded sheet contains no fully labelled rows.")
            return
        st.metric(
            "Usable abstract rate among labelled records",
            f"{uploaded_summary.usable_rate:.1%}",
        )
        st.caption(
            f"{uploaded_summary.usable}/{uploaded_summary.labelled} usable; 95% Wilson "
            f"interval {uploaded_summary.ci95_low:.1%}–{uploaded_summary.ci95_high:.1%}."
        )


def page_evidence() -> None:
    """Keep released-profile claims separate from companion-study evidence."""
    _render_header(
        "Evidence and claim boundaries",
        "What this snapshot verifies, and what requires a separate empirical protocol.",
    )
    release = _release_identity(_load_collector().config.profile_id or "")
    st.subheader(f"Current release: {release['reader']}")
    st.caption(f"Snapshot identifier for citation and audit: `{release['auditor']}`")
    st.markdown(
        "This interface verifies the manifest, exact-resource identity policy, coverage, search, "
        "exports, and platform reproduction for the selected snapshot. Abstract quality was "
        "evaluated separately with a deterministic, venue-stratified manual audit."
    )

    audit_summary_path = (
        ARTIFACT_ROOT / "evaluation" / "security-20-v3" / "manual_abstract_audit_summary.json"
    )
    audit_transfer_path = ARTIFACT_ROOT / "evaluation" / "security-20-v4" / "audit_transfer.json"
    audit_summary = json.loads(audit_summary_path.read_text(encoding="utf-8"))
    audit_transfer = json.loads(audit_transfer_path.read_text(encoding="utf-8"))

    st.subheader("Manual abstract audit")
    metric_columns = st.columns(4)
    metric_columns[0].metric("Human-reviewed records", audit_summary["labelled"])
    metric_columns[1].metric("Usable abstracts", audit_summary["usable"])
    metric_columns[2].metric("Usable rate", f"{audit_summary['usable_rate']:.1%}")
    interval_low, interval_high = audit_summary["wilson_95_ci"]
    metric_columns[3].metric(
        "95% Wilson interval",
        f"{interval_low * 100:.1f}–{interval_high:.1%}",
    )
    st.markdown(
        "All 200 final decisions were recorded as **human-only** by Sidnei Barbieri. A record was "
        "counted as usable only when its abstract was complete, uncontaminated, and matched the "
        "sampled paper. The append-only decision log retains superseded provenance events."
    )
    if audit_transfer["transfer_valid"]:
        st.info(
            "The audit was executed on the v3 snapshot and remains valid for this release: v3 and "
            "v4 have the same 14,859 paper IDs and identical abstract text. The ten v4 changes are "
            "title repairs, and none belongs to the audit sample."
        )
    with st.expander("Inspect audit criteria and provenance"):
        criteria = audit_summary["criteria"]
        st.dataframe(
            pd.DataFrame(
                [
                    {"Criterion": "Complete", "Yes": criteria["complete_yes"]},
                    {"Criterion": "Uncontaminated", "Yes": criteria["uncontaminated_yes"]},
                    {"Criterion": "Matches sampled paper", "Yes": criteria["matches_paper_yes"]},
                ]
            ),
            hide_index=True,
            width="stretch",
        )
        st.caption(
            "Primary evidence: evaluation/security-20-v3/manual_abstract_audit.csv and "
            "manual_abstract_audit_decisions.jsonl. Transfer verification: "
            "evaluation/security-20-v4/audit_transfer.json."
        )

    st.subheader("Companion full-paper evaluation")
    st.markdown(
        "The published full-paper protocol is bound to a different frozen 9,925-record snapshot "
        "(SHA-256 `0f4dbaa9…ef64cd`): a venue-stratified 200-record live comparison and a 200-record "
        "manual publisher-source audit. Its data, instrument, labels, and offline summarizer are "
        "available in the [frozen evaluation package](https://github.com/sidneibarbieri/topVenues/tree/07674480ff3172f4b195387438ab3af3c9c5655f/evaluation/baseline_validation)."
    )
    st.warning(
        "The companion paper's baseline-comparison results remain bound to its 9,925-record "
        "snapshot. They are not transferred to this release."
    )
    with st.expander("Repeat or extend the manual audit"):
        _render_audit_workbench()
    st.subheader("Identity policy")
    st.markdown(
        "This release applies exact-resource deduplication, enforces the declared 2019–2026 "
        "window, and merges DOI aliases confirmed by Crossref. Same-metadata pairs stay distinct "
        "where the publisher resources are distinct. Ten titles truncated at inline markup were "
        "repaired against their DBLP records. Every decision is disclosed in the adjudication and "
        "repair logs shipped with the release."
    )


def page_pipeline() -> None:
    collector = _load_collector()
    _render_header(
        "Pipeline",
        "Run the data collection pipeline. Each step is incremental and safe to repeat.",
    )

    if collector.config.immutable_snapshot:
        st.warning(
            "This is a released immutable profile. Interactive refresh controls are disabled so "
            "a live API run cannot alter the corpus shown in this release. Create, validate, and "
            "publish a separate successor profile for any refresh."
        )
        st.markdown(
            "Refreshes are created as a new named profile through the "
            "[profile refresh procedure](https://github.com/sidneibarbieri/"
            "topVenues/blob/main/docs/PROFILE_REFRESH.md). The current snapshot is never "
            "modified in place."
        )
        lifecycle = pd.DataFrame(
            [
                {
                    "Gate": "1. Declare",
                    "Evidence": "New profile ID, venues, years, and source policy",
                },
                {"Gate": "2. Collect", "Evidence": "Timestamped source logs and field provenance"},
                {
                    "Gate": "3. Identify",
                    "Evidence": "Exact-resource merges and manual adjudication queue",
                },
                {
                    "Gate": "4. Enrich",
                    "Evidence": "Abstract/BibTeX coverage and missing-data report",
                },
                {
                    "Gate": "5. Compare",
                    "Evidence": "Added, removed, retained, coverage, venue, and year deltas",
                },
                {
                    "Gate": "6. Audit",
                    "Evidence": "Automated tests plus a new snapshot-bound manual sample",
                },
                {
                    "Gate": "7. Freeze",
                    "Evidence": "Manifest, SHA-256, clean reviewer reproduction, and release tag",
                },
            ]
        )
        st.dataframe(lifecycle, width="stretch", hide_index=True)
        st.code(
            "python scripts/compare_profiles.py PREVIOUS SUCCESSOR --output profile-diff.json",
            language="bash",
        )
        st.caption(
            "Historical snapshot binaries are fetched explicitly before comparison; they are "
            "not duplicated in every current release."
        )
        return

    tab_dl, tab_cons, tab_extr = st.tabs(["Download", "Consolidate", "Extract abstracts"])

    with tab_dl:
        st.write(
            "Fetches DBLP JSON files for every configured venue and year. "
            "Skips files that already exist and validate cleanly."
        )
        if st.button("Run download", type="primary", width="stretch", key="dl_btn"):
            with st.spinner("Downloading…"):
                _run_async(Collector().run_download())
                st.success("Download complete.")
                st.cache_resource.clear()

    with tab_cons:
        st.write(
            "Merges downloaded JSON into the SQLite database. Existing abstracts "
            "are preserved (idempotent upsert with `COALESCE`)."
        )
        if st.button("Run consolidate", type="primary", width="stretch", key="cons_btn"):
            with st.spinner("Consolidating…"):
                _run_async(Collector().run_consolidate())
                st.success("Consolidation complete.")
                st.cache_resource.clear()

    with tab_extr:
        st.warning(
            "Rate-limited. Open APIs (Semantic Scholar / OpenAlex / CrossRef) run in "
            "parallel; publisher scrapers run sequentially with throttling."
        )
        col_a, col_b = st.columns(2)
        with col_a:
            batch_size = st.number_input("Batch size", 1, 100, 10)
        with col_b:
            max_papers = st.number_input("Max papers (0 = all)", 0, 10000, 0)

        if st.button("Run extraction", type="primary", width="stretch", key="ext_btn"):
            collector = Collector()
            collector.config.batch_size = batch_size
            collector.papers = collector._load_papers_from_disk()
            to_process = [p for p in collector.papers if not p.abstract]
            if max_papers > 0:
                to_process = to_process[:max_papers]
            if not to_process:
                st.info("All papers already have abstracts.")
                return
            progress = st.progress(0.0)
            status = st.empty()
            total = len(to_process)
            status.text(f"0 / {total} papers processed…")

            async def run_extraction():
                fetcher = AbstractFetcher(collector)
                for idx, paper in enumerate(to_process, 1):
                    await collector._extract_single_abstract(paper, fetcher)
                    progress.progress(idx / total)
                    status.text(f"{idx} / {total} papers processed…")
                    if idx % collector.config.batch_size == 0:
                        collector._save_dataset()
                        await asyncio.sleep(60)
                await fetcher.close()
                collector._save_dataset()

            _run_async(run_extraction())
            st.success("Extraction complete.")
            st.cache_resource.clear()


# ── Main ───────────────────────────────────────────────────────────────────


def main() -> None:
    pending = st.session_state.pop("pending_search_navigation", None)
    if pending:
        _open_search_from_insight(**pending)
    pages = {
        "Overview": page_artifact,
        "Search": page_search,
        "Insights": page_insights,
        "Evidence": page_evidence,
        "Dataset lifecycle": page_pipeline,
    }
    with st.sidebar:
        page = st.radio("Navigate", list(pages.keys()), label_visibility="collapsed", key="page")
        st.markdown("<br>", unsafe_allow_html=True)

    pages[page]()

    st.markdown(
        '<div class="footer">TopVenues — bibliographic explorer · '
        "data sourced from DBLP, Semantic Scholar, OpenAlex, CrossRef</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
