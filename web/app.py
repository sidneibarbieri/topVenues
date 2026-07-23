"""Streamlit web interface for the bibliographic corpus explorer."""

import asyncio
import html
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ARTIFACT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ARTIFACT_ROOT))

from src.abstract_fetcher import AbstractFetcher
from src.collector import Collector
from src.database import require_corpus
from src.models import PaperClass, SearchFilters

PAGE_SIZE_OPTIONS = (25, 50, 100, 200)
ABSTRACT_PREVIEW_CHARS = 280
ARTIFACT_CLAIMS = (
    ("Corpus", "9,925", "papers across 11 configured sources"),
    ("Abstracts", "99.86%", "9,911 records with searchable abstracts"),
    ("BibTeX", "99.99%", "9,924 records ready for citation export"),
    ("Triage filter", "16.5x", "precision lift at 90% recall"),
)

st.set_page_config(
    page_title="TopVenues - Security Paper Explorer",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Styles ────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
        :root {
            --ink:    #18212f;
            --navy:   #243247;
            --slate:  #3d4b5f;
            --teal:   #2f6f73;
            --amber:  #b36b2c;
            --green:  #4f7d4a;
            --rose:   #a84646;
            --muted:  #d8e2e3;
            --border: #d8dde3;
            --bg:     #f8f8f5;
            --card:   #ffffff;
        }

        .stApp, [data-testid="stAppViewContainer"] {
            background: var(--bg);
            color: var(--ink);
        }
        [data-testid="stHeader"] { background: rgba(248, 248, 245, .86); }
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
            background: #eef4f5;
            border-left: 5px solid var(--teal);
            border-radius: 6px;
            padding: 1.35rem 1.6rem;
            margin-bottom: 1.4rem;
            border-top: 1px solid var(--border);
            border-right: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
        }
        .app-header h1 {
            color: var(--ink); font-size: 1.75rem; font-weight: 700;
            margin: 0 0 .35rem; letter-spacing: 0;
        }
        .app-header p { color: #4d5f71; font-size: .96rem; margin: 0; }

        .metric-row { display: flex; gap: .9rem; margin-bottom: 1.2rem; flex-wrap: wrap; }
        .metric {
            flex: 1; min-width: 160px;
            background: var(--card); border: 1px solid var(--border);
            border-left: 4px solid var(--teal);
            border-radius: 6px; padding: .95rem 1.1rem;
        }
    .metric.amber { border-left-color: var(--amber); }
    .metric.green { border-left-color: var(--green); }
    .metric.rose  { border-left-color: var(--rose); }
    .metric .lbl { color: #6b7c8d; font-size: .72rem; text-transform: uppercase;
                   letter-spacing: .8px; margin-bottom: .35rem; }
        .metric .val { color: var(--ink); font-size: 1.7rem; font-weight: 700; line-height: 1; }
    .metric .sub { color: #8b97a3; font-size: .72rem; margin-top: .25rem; }

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
        background: #f1f4f3;
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
        color: #334155 !important;
    }

        .results-bar {
            display: flex; align-items: center; justify-content: space-between;
            background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
            padding: .7rem 1rem; margin-bottom: 1rem;
        }
        .results-bar .count { color: var(--ink); font-size: 1rem; font-weight: 700; }
    .results-bar .sub   { color: #6b7c8d; font-size: .85rem; }

        .paper-card {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 6px; padding: 1.35rem 1.55rem; margin-top: 1rem;
        }
        .paper-card h3 { color: var(--ink); margin: 0 0 .6rem; }
    .paper-meta {
        display: flex; gap: 1.5rem; color: #6b7c8d; font-size: .85rem;
        margin-bottom: .8rem; flex-wrap: wrap;
    }
    .paper-abstract {
        white-space: pre-wrap; line-height: 1.6;
        color: #2c3e50; font-size: .95rem;
    }

        .claim-grid {
            display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
            gap: .8rem; margin: .9rem 0 1.2rem;
        }
        .claim {
            border: 1px solid var(--border); border-radius: 6px; background: var(--card);
            padding: .95rem 1rem;
        }
        .claim .name { color: #607084; text-transform: uppercase; font-size: .72rem; font-weight: 700; }
        .claim .value { color: var(--ink); font-size: 1.6rem; font-weight: 700; line-height: 1.2; }
        .claim .note { color: #637184; font-size: .84rem; }
        .stDataFrame { border-radius: 6px; overflow: hidden; }
        div[data-testid="stExpander"] { border-radius: 6px; }
    .footer {
        color: #94a3b8; font-size: .78rem; text-align: center;
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


def _safe_html(value: object) -> str:
    if value is None:
        return "—"
    return html.escape(str(value), quote=True)


def _venue_options(collector: Collector) -> list[str]:
    venues = sorted({paper.event for paper in collector.papers if paper.event})
    return ["All venues", *venues]


def _abstract_length_predicate(papers, choice: str):
    if choice == "Any":
        return papers
    if choice == "Has abstract":
        return [paper for paper in papers if paper.abstract]
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


def _render_claims() -> None:
    # The HTML must stay flat: Streamlit runs markdown before inserting raw
    # HTML, so any line indented four or more spaces becomes a code block.
    cards = "".join(
        '<div class="claim">'
        f'<div class="name">{_safe_html(name)}</div>'
        f'<div class="value">{_safe_html(value)}</div>'
        f'<div class="note">{_safe_html(note)}</div>'
        "</div>"
        for name, value, note in ARTIFACT_CLAIMS
    )
    st.markdown(f'<div class="claim-grid">{cards}</div>', unsafe_allow_html=True)


def _render_metrics(stats: dict, filtered_count: int | None = None) -> None:
    total = stats["total_papers"]
    with_abs = stats["with_abstracts"]
    with_bib = stats.get("with_bibtex", 0)
    abs_pct = (with_abs / total * 100) if total else 0
    bib_pct = (with_bib / total * 100) if total else 0
    venues = len(stats["by_event"])
    extra = (
        f'<div class="metric green"><div class="lbl">Currently shown</div>'
        f'<div class="val">{filtered_count:,}</div></div>'
        if filtered_count is not None else ""
    )
    st.markdown(
        '<div class="metric-row">'
        f'<div class="metric"><div class="lbl">Papers indexed</div>'
        f'<div class="val">{total:,}</div>'
        f'<div class="sub">across {venues} venues</div></div>'
        f'<div class="metric amber"><div class="lbl">With abstract</div>'
        f'<div class="val">{with_abs:,}</div>'
        f'<div class="sub">{abs_pct:.2f}% coverage</div></div>'
        f'<div class="metric rose"><div class="lbl">With BibTeX</div>'
        f'<div class="val">{with_bib:,}</div>'
        f'<div class="sub">{bib_pct:.2f}% coverage</div></div>'
        f'{extra}'
        '</div>',
        unsafe_allow_html=True,
    )


# ── Pages ──────────────────────────────────────────────────────────────────


def page_artifact() -> None:
    _render_header(
        "Reproducible corpus overview",
        "Reproduce the corpus, inspect coverage, and export ready-to-cite references from a local snapshot.",
    )
    _render_claims()

    st.subheader("Verification path")
    st.markdown(
        """
        1. Run the reproduction script to validate the headline claims.
        2. Use Search to inspect the corpus and export CSV, JSON or BibTeX.
        3. Use Insights to verify scope, coverage and temporal distribution.
        4. Use Pipeline only when refreshing the corpus from live sources.
        """
    )

    st.code("bash reproduce.sh", language="bash")

    st.subheader("Reproducibility evidence")
    evidence = pd.DataFrame(
        [
            {
                "Criterion": "Availability",
                "Evidence": "Source code, compressed SQLite snapshot, arXiv snapshot and documentation.",
            },
            {
                "Criterion": "Functionality",
                "Evidence": "CLI, Streamlit interface, search, statistics and CSV/JSON/BibTeX export.",
            },
            {
                "Criterion": "Reproducibility",
                "Evidence": "Single-command script validates counts, tests, latency, export and triage results.",
            },
            {
                "Criterion": "Sustainability",
                "Evidence": "Small Python/SQLite stack, typed models and configuration-driven corpus scope.",
            },
        ]
    )
    st.dataframe(evidence, width="stretch", hide_index=True)

    st.subheader("Scientific findings")
    findings = pd.DataFrame(
        [
            {
                "Finding": "Early signal",
                "Result": "29.2% of 2024-2025 core security papers have a matching arXiv preprint.",
                "Reproduce": ".venv/bin/python scripts/early_signal_study.py",
            },
            {
                "Finding": "Triage filter",
                "Result": "Prior-scope authorship gives 16.5x precision lift at 90% recall and 64% volume cut.",
                "Reproduce": ".venv/bin/python scripts/readiness_study.py",
            },
        ]
    )
    st.dataframe(findings, width="stretch", hide_index=True)


def page_search() -> None:
    collector = _load_collector()
    stats = collector.db.get_statistics()

    with st.sidebar:
        st.markdown("## Filters")

        with st.expander("Text", expanded=True):
            title_query = st.text_input("Title contains", placeholder="e.g., authentication")
            abstract_query = st.text_input("Abstract contains", placeholder="e.g., LLM, SGX")
            author_query = st.text_input("Author contains", placeholder="e.g., Sekar")
            tech_query = st.text_input("Topic / tech", placeholder="e.g., blockchain, 5G")

        with st.expander("Venue & year", expanded=True):
            venue_choice = st.selectbox("Venue", _venue_options(collector))
            year_choice = st.selectbox(
                "Year", ["All years", *sorted(stats["by_year"], reverse=True)]
            )

        with st.expander("Paper class", expanded=False):
            class_choices = st.multiselect(
                "Include", [c.value for c in PaperClass],
                help="Filter by SoK, Survey, Poster, Workshop, Short, Journal or Article.",
            )

        with st.expander("Abstract & citation", expanded=False):
            abstract_length = st.selectbox(
                "Abstract length",
                ["Any", "Has abstract", "Short (≤ 150 words)",
                 "Medium (151–300 words)", "Long (> 300 words)"],
            )
            only_with_bibtex = st.checkbox(
                "Has BibTeX", help="Only include papers whose BibTeX entry has been fetched."
            )

        st.markdown("## Display")
        page_size = st.select_slider(
            "Results per page", options=PAGE_SIZE_OPTIONS, value=50
        )
        sort_choice = st.selectbox(
            "Sort by", ["Year (newest first)", "Year (oldest first)", "Title (A–Z)", "Venue"],
        )

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

    results = collector.search(filters, limit=None)
    if class_choices:
        wanted = {PaperClass(value) for value in class_choices}
        results = [p for p in results if p.paper_class in wanted]
    results = _abstract_length_predicate(results, abstract_length)
    results = _bibtex_predicate(results, only_with_bibtex)

    if sort_choice == "Year (newest first)":
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
    _render_metrics(stats, filtered_count=len(results))

    if not results:
        st.info("No papers match the current filters. Try widening the search.")
        return

    total_pages = max(1, (len(results) + page_size - 1) // page_size)
    search_signature = (
        title_query,
        abstract_query,
        author_query,
        tech_query,
        venue_choice,
        year_choice,
        tuple(class_choices),
        abstract_length,
        only_with_bibtex,
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
                if st.button("›", disabled=st.session_state["page_no"] >= total_pages, width="stretch"):
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
            f'page {page} of {total_pages} · {page_size} per page</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    table_rows = [
        {
            "Title": paper.title or "—",
            "Authors": (paper.authors or "—")[:90]
                       + ("…" if paper.authors and len(paper.authors) > 90 else ""),
            "Venue": paper.event or "—",
            "Year": paper.year,
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
            "Title":    st.column_config.TextColumn("Title", width="medium"),
            "Authors":  st.column_config.TextColumn("Authors", width="small"),
            "Venue":    st.column_config.TextColumn("Venue", width="small"),
            "Year":     st.column_config.NumberColumn("Year", format="%d", width="small"),
            "Class":    st.column_config.TextColumn("Class", width="small"),
            "Words":    st.column_config.NumberColumn("Words", format="%d", width="small"),
            "Abstract": st.column_config.TextColumn("Abstract preview", width="large"),
            "Cite":     st.column_config.TextColumn("\\cite{…}", width="small"),
            "Link":     st.column_config.LinkColumn("DOI / URL", width="small", display_text="open"),
        },
    )

    full_rows = [
        {
            "title": paper.title,
            "authors": paper.authors,
            "first_author": paper.first_author,
            "venue": paper.event,
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
                 if bib_text else "BibTeX not yet fetched for any paper in this result set.",
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
            f'<a href="{_safe_html(link)}" target="_blank">{_safe_html(link)}</a>'
            if link else "—"
        )
        doi_html = (
            f'<a href="https://doi.org/{_safe_html(paper.doi)}" target="_blank">{_safe_html(paper.doi)}</a>'
            if paper.doi else "—"
        )
        abstract_html = (
            _safe_html(paper.abstract) if paper.abstract
            else "<i>No abstract available.</i>"
        )
        st.markdown(
            '<div class="paper-card">'
            f'<h3>{_safe_html(paper.title)}</h3>'
            '<div class="paper-meta">'
            f'<span><b>Authors:</b> {_safe_html(paper.authors)}</span>'
            f'<span><b>Venue:</b> {_safe_html(paper.event)}</span>'
            f'<span><b>Year:</b> {_safe_html(paper.year)}</span>'
            f'<span><b>Words:</b> {paper.abstract_words:,}</span>'
            f'<span><b>DOI:</b> {doi_html}</span>'
            f'<span><b>Link:</b> {link_html}</span>'
            '</div>'
            f'<div>{_class_badge(paper.paper_class)}</div>'
            '<hr style="border:none; border-top:1px solid var(--border); margin:1rem 0">'
            f'<div class="paper-abstract">{abstract_html}</div>'
            '</div>',
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
    collector = _load_collector()
    stats = collector.db.get_statistics()
    _render_header(
        "Dataset insights",
        "Distribution of papers across venues, years, paper classes and abstract coverage.",
    )
    _render_metrics(stats)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Papers by venue")
        venue_df = pd.DataFrame(
            [{"Venue": k, "Papers": v} for k, v in
             sorted(stats["by_event"].items(), key=lambda x: x[1], reverse=True)]
        ).set_index("Venue")
        st.bar_chart(venue_df, height=380)

    with col2:
        st.subheader("Papers by year")
        year_df = pd.DataFrame(
            [{"Year": k, "Papers": v} for k, v in sorted(stats["by_year"].items())]
        ).set_index("Year")
        st.bar_chart(year_df, height=380)

    st.divider()
    st.subheader("Papers by class")
    class_counts = {}
    for paper in collector.papers:
        class_counts[paper.paper_class.value] = class_counts.get(paper.paper_class.value, 0) + 1
    class_df = pd.DataFrame(
        [{"Class": k, "Papers": v} for k, v in
         sorted(class_counts.items(), key=lambda x: x[1], reverse=True)]
    ).set_index("Class")
    st.bar_chart(class_df, height=320)

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
            rows.append({
                "Venue": venue,
                "Total": total,
                "With abstract": with_abs,
                "Coverage": f"{with_abs / total * 100:.1f}%" if total else "—",
            })
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)


def page_pipeline() -> None:
    _render_header(
        "Pipeline",
        "Run the data collection pipeline. Each step is incremental and safe to repeat.",
    )

    tab_dl, tab_cons, tab_extr = st.tabs(["Download", "Consolidate", "Extract abstracts"])

    with tab_dl:
        st.write("Fetches DBLP JSON files for every configured venue and year. "
                 "Skips files that already exist and validate cleanly.")
        if st.button("Run download", type="primary", width="stretch", key="dl_btn"):
            with st.spinner("Downloading…"):
                _run_async(Collector().run_download())
                st.success("Download complete.")
                st.cache_resource.clear()

    with tab_cons:
        st.write("Merges downloaded JSON into the SQLite database. Existing abstracts "
                 "are preserved (idempotent upsert with `COALESCE`).")
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
    pages = {
        "Overview": page_artifact,
        "Search": page_search,
        "Insights": page_insights,
        "Pipeline": page_pipeline,
    }
    with st.sidebar:
        st.markdown(
            '<h2 style="color:#fff !important; border:none !important;'
            'font-size:1.15rem !important; text-transform:none !important;'
            'letter-spacing:0 !important; margin-bottom:1rem !important">'
            'TopVenues</h2>',
            unsafe_allow_html=True,
        )
        page = st.radio("Navigate", list(pages.keys()), label_visibility="collapsed")
        st.markdown("<br>", unsafe_allow_html=True)

    pages[page]()

    st.markdown(
        '<div class="footer">TopVenues — bibliographic explorer · '
        'data sourced from DBLP, Semantic Scholar, OpenAlex, CrossRef</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
