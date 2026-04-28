"""Streamlit web interface — bibliographic explorer for top-tier security papers."""

import asyncio
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.abstract_fetcher import AbstractFetcher
from src.collector import Collector
from src.models import PaperClass, SearchFilters

PAGE_SIZE_OPTIONS = (25, 50, 100, 200)
ABSTRACT_PREVIEW_CHARS = 280

st.set_page_config(
    page_title="topVenues — Security Paper Explorer",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Styles ────────────────────────────────────────────────────────────────

st.markdown(
    """
<style>
    :root {
        --navy:   #0d1b2a;
        --slate:  #2d3f50;
        --teal:   #00b4d8;
        --amber:  #f4a261;
        --green:  #2ec4b6;
        --rose:   #e63946;
        --muted:  #8ecae6;
        --border: #dde3ea;
        --bg:     #f7f9fc;
        --card:   #ffffff;
    }

    .app-header {
        background: linear-gradient(135deg, var(--navy) 0%, var(--slate) 100%);
        border-radius: 14px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.4rem;
        box-shadow: 0 4px 14px rgba(13, 27, 42, .08);
    }
    .app-header h1 {
        color: #ffffff; font-size: 1.85rem; font-weight: 700;
        margin: 0 0 .35rem; letter-spacing: -.4px;
    }
    .app-header p { color: var(--muted); font-size: .95rem; margin: 0; }

    .metric-row { display: flex; gap: .9rem; margin-bottom: 1.2rem; flex-wrap: wrap; }
    .metric {
        flex: 1; min-width: 160px;
        background: var(--card); border: 1px solid var(--border);
        border-left: 4px solid var(--teal);
        border-radius: 10px; padding: 1rem 1.2rem;
    }
    .metric.amber { border-left-color: var(--amber); }
    .metric.green { border-left-color: var(--green); }
    .metric.rose  { border-left-color: var(--rose); }
    .metric .lbl { color: #6b7c8d; font-size: .72rem; text-transform: uppercase;
                   letter-spacing: .8px; margin-bottom: .35rem; }
    .metric .val { color: var(--navy); font-size: 1.7rem; font-weight: 700; line-height: 1; }
    .metric .sub { color: #8b97a3; font-size: .72rem; margin-top: .25rem; }

    .tag {
        display: inline-block; border-radius: 4px;
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

    section[data-testid="stSidebar"] { background: var(--navy); }
    section[data-testid="stSidebar"] * { color: #cdd9e5 !important; }
    section[data-testid="stSidebar"] h2 {
        color: #ffffff !important; font-size: .95rem;
        letter-spacing: .4px; text-transform: uppercase;
        border-bottom: 1px solid var(--slate);
        padding-bottom: .5rem; margin: .4rem 0 .8rem;
    }

    .results-bar {
        display: flex; align-items: center; justify-content: space-between;
        background: var(--bg); border: 1px solid var(--border); border-radius: 10px;
        padding: .7rem 1rem; margin-bottom: 1rem;
    }
    .results-bar .count { color: var(--navy); font-size: 1rem; font-weight: 700; }
    .results-bar .sub   { color: #6b7c8d; font-size: .85rem; }

    .paper-card {
        background: var(--card); border: 1px solid var(--border);
        border-radius: 10px; padding: 1.4rem 1.6rem; margin-top: 1rem;
    }
    .paper-card h3 { color: var(--navy); margin: 0 0 .6rem; }
    .paper-meta {
        display: flex; gap: 1.5rem; color: #6b7c8d; font-size: .85rem;
        margin-bottom: .8rem; flex-wrap: wrap;
    }
    .paper-abstract {
        white-space: pre-wrap; line-height: 1.6;
        color: #2c3e50; font-size: .95rem;
    }

    .stDataFrame { border-radius: 8px; overflow: hidden; }
    div[data-testid="stExpander"] { border-radius: 8px; }
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
    collector = Collector()
    collector.papers = collector._load_papers_from_disk()
    return collector


def _venue_options(collector: Collector) -> list[str]:
    venues = sorted({p.event for p in collector.papers if p.event})
    return ["All venues", *venues]


def _abstract_length_predicate(papers, choice: str):
    if choice == "Any":
        return papers
    if choice == "Has abstract":
        return [p for p in papers if p.abstract]
    if choice == "Short (≤ 150 words)":
        return [p for p in papers if 0 < p.abstract_words <= 150]
    if choice == "Medium (151–300 words)":
        return [p for p in papers if 151 <= p.abstract_words <= 300]
    if choice == "Long (> 300 words)":
        return [p for p in papers if p.abstract_words > 300]
    return papers


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


def _render_metrics(stats: dict, filtered_count: int | None = None) -> None:
    total = stats["total_papers"]
    with_abs = stats["with_abstracts"]
    pct = (with_abs / total * 100) if total else 0
    venues = len(stats["by_event"])
    extra = (
        f'<div class="metric green"><div class="lbl">Currently shown</div>'
        f'<div class="val">{filtered_count:,}</div></div>'
        if filtered_count is not None else ""
    )
    years = stats["by_year"]
    year_range = f'{min(years)}–{max(years)}' if years else '—'
    st.markdown(
        f"""
        <div class="metric-row">
          <div class="metric">
            <div class="lbl">Papers indexed</div>
            <div class="val">{total:,}</div>
            <div class="sub">across {venues} venues</div>
          </div>
          <div class="metric amber">
            <div class="lbl">With abstract</div>
            <div class="val">{with_abs:,}</div>
            <div class="sub">{pct:.1f}% coverage</div>
          </div>
          <div class="metric rose">
            <div class="lbl">Year coverage</div>
            <div class="val">{len(years)}</div>
            <div class="sub">{year_range}</div>
          </div>
          {extra}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Pages ──────────────────────────────────────────────────────────────────


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

        with st.expander("Abstract", expanded=False):
            abstract_length = st.selectbox(
                "Length",
                ["Any", "Has abstract", "Short (≤ 150 words)",
                 "Medium (151–300 words)", "Long (> 300 words)"],
            )

        st.markdown("## Display")
        page_size = st.select_slider(
            "Results per page", options=PAGE_SIZE_OPTIONS, value=50
        )
        sort_choice = st.selectbox(
            "Sort by", ["Year (newest first)", "Year (oldest first)", "Title (A–Z)", "Venue"],
        )

    filters = SearchFilters()
    if title_query:    filters.title_contains    = title_query
    if abstract_query: filters.abstract_contains = abstract_query
    if author_query:   filters.author_contains   = author_query
    if tech_query:     filters.technology        = tech_query
    if venue_choice != "All venues": filters.event = venue_choice
    if year_choice != "All years":   filters.year  = int(year_choice)

    results = collector.search(filters, limit=None)
    if class_choices:
        wanted = {PaperClass(value) for value in class_choices}
        results = [p for p in results if p.paper_class in wanted]
    results = _abstract_length_predicate(results, abstract_length)

    if sort_choice == "Year (newest first)":
        results.sort(key=lambda p: (-(p.year or 0), p.title or ""))
    elif sort_choice == "Year (oldest first)":
        results.sort(key=lambda p: (p.year or 0, p.title or ""))
    elif sort_choice == "Title (A–Z)":
        results.sort(key=lambda p: (p.title or "").lower())
    elif sort_choice == "Venue":
        results.sort(key=lambda p: (p.event or "", -(p.year or 0)))

    _render_header(
        "🔐 Security Paper Explorer",
        "Search a curated dataset of papers from the leading security research venues.",
    )
    _render_metrics(stats, filtered_count=len(results))

    if not results:
        st.info("🔍 No papers match the current filters. Try widening the search.")
        return

    total_pages = max(1, (len(results) + page_size - 1) // page_size)
    col_count, col_page = st.columns([3, 1])
    with col_count:
        st.markdown(
            f'<div class="results-bar">'
            f'<span class="count">{len(results):,} papers found</span>'
            f'<span class="sub">{total_pages} page(s) · {page_size} per page</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with col_page:
        page = st.number_input("Page", 1, total_pages, 1, key="page_no") if total_pages > 1 else 1

    start = (page - 1) * page_size
    page_slice = results[start : start + page_size]

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
            "Link": paper.ee or paper.url or "",
        }
        for paper in page_slice
    ]
    df = pd.DataFrame(table_rows)

    st.dataframe(
        df,
        use_container_width=True,
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
        }
        for paper in results
    ]
    full_df = pd.DataFrame(full_rows)
    col_csv, col_json, _ = st.columns([1, 1, 4])
    with col_csv:
        st.download_button(
            "⬇ Export CSV",
            full_df.to_csv(index=False).encode("utf-8"),
            "topvenues_results.csv",
            "text/csv",
            use_container_width=True,
        )
    with col_json:
        st.download_button(
            "⬇ Export JSON",
            full_df.to_json(orient="records", indent=2),
            "topvenues_results.json",
            "application/json",
            use_container_width=True,
        )

    st.divider()
    st.subheader("📄 Paper details")
    title_options = [f"[{p.year}] {p.title}" for p in page_slice]
    selected_label = st.selectbox(
        "Select a paper from this page", title_options, label_visibility="collapsed"
    )
    if selected_label:
        idx = title_options.index(selected_label)
        paper = page_slice[idx]
        link = paper.ee or paper.url
        link_html = f'<a href="{link}" target="_blank">{link}</a>' if link else "—"
        doi_html = (
            f'<a href="https://doi.org/{paper.doi}" target="_blank">{paper.doi}</a>'
            if paper.doi else "—"
        )
        st.markdown(
            f"""
            <div class="paper-card">
              <h3>{paper.title}</h3>
              <div class="paper-meta">
                <span><b>Authors:</b> {paper.authors or "—"}</span>
                <span><b>Venue:</b> {paper.event or "—"}</span>
                <span><b>Year:</b> {paper.year}</span>
                <span><b>Words:</b> {paper.abstract_words:,}</span>
                <span><b>DOI:</b> {doi_html}</span>
                <span><b>Link:</b> {link_html}</span>
              </div>
              <div>{_class_badge(paper.paper_class)}</div>
              <hr style="border:none; border-top:1px solid var(--border); margin:1rem 0">
              <div class="paper-abstract">{paper.abstract or "<i>No abstract available.</i>"}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def page_insights() -> None:
    collector = _load_collector()
    stats = collector.db.get_statistics()
    _render_header(
        "📊 Dataset insights",
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
    import sqlite3
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
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def page_pipeline() -> None:
    _render_header(
        "⚙ Pipeline",
        "Run the data collection pipeline. Each step is incremental and safe to repeat.",
    )

    tab_dl, tab_cons, tab_extr = st.tabs(["📥 Download", "🔗 Consolidate", "📝 Extract abstracts"])

    with tab_dl:
        st.write("Fetches DBLP JSON files for every configured venue and year. "
                 "Skips files that already exist and validate cleanly.")
        if st.button("Run download", type="primary", use_container_width=True, key="dl_btn"):
            with st.spinner("Downloading…"):
                _run_async(Collector().run_download())
                st.success("Download complete.")
                st.cache_resource.clear()

    with tab_cons:
        st.write("Merges downloaded JSON into the SQLite database. Existing abstracts "
                 "are preserved (idempotent upsert with `COALESCE`).")
        if st.button("Run consolidate", type="primary", use_container_width=True, key="cons_btn"):
            with st.spinner("Consolidating…"):
                _run_async(Collector().run_consolidate())
                st.success("Consolidation complete.")
                st.cache_resource.clear()

    with tab_extr:
        st.warning(
            "⚠ Rate-limited. Open APIs (Semantic Scholar / OpenAlex / CrossRef) run in "
            "parallel; publisher scrapers run sequentially with throttling."
        )
        col_a, col_b = st.columns(2)
        with col_a:
            batch_size = st.number_input("Batch size", 1, 100, 10)
        with col_b:
            max_papers = st.number_input("Max papers (0 = all)", 0, 10000, 0)

        if st.button("Run extraction", type="primary", use_container_width=True, key="ext_btn"):
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
        "🔍 Search": page_search,
        "📊 Insights": page_insights,
        "⚙ Pipeline": page_pipeline,
    }
    with st.sidebar:
        st.markdown(
            '<h2 style="color:#fff !important; border:none !important;'
            'font-size:1.15rem !important; text-transform:none !important;'
            'letter-spacing:0 !important; margin-bottom:1rem !important">'
            '🔐 topVenues</h2>',
            unsafe_allow_html=True,
        )
        page = st.radio("Navigate", list(pages.keys()), label_visibility="collapsed")
        st.markdown("<br>", unsafe_allow_html=True)

    pages[page]()

    st.markdown(
        '<div class="footer">topVenues — bibliographic explorer · '
        'data sourced from DBLP, Semantic Scholar, OpenAlex, CrossRef</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
