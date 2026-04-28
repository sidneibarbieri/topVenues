"""Streamlit web interface for the Top Security Venues paper collector."""

import asyncio
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.abstract_fetcher import AbstractFetcher
from src.collector import Collector
from src.models import SearchFilters

PAGE_SIZE_OPTIONS = (25, 50, 100, 200)

st.set_page_config(
    page_title="Top Security Venues",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Styles ────────────────────────────────────────────────────────────────

st.markdown(
    """
<style>
    /* Colour tokens — premium navy + teal palette */
    :root {
        --navy:    #0d1b2a;
        --slate:   #2d3f50;
        --teal:    #00b4d8;
        --amber:   #f4a261;
        --green:   #2ec4b6;
        --muted:   #8ecae6;
        --border:  #dde3ea;
        --bg-card: #ffffff;
    }

    /* Top header */
    .app-header {
        background: linear-gradient(135deg, var(--navy) 0%, var(--slate) 100%);
        border-radius: 14px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.4rem;
        box-shadow: 0 4px 14px rgba(13,27,42,.08);
    }
    .app-header h1 {
        color: #ffffff;
        font-size: 1.8rem;
        font-weight: 700;
        margin: 0 0 .3rem;
        letter-spacing: -.4px;
    }
    .app-header p { color: var(--muted); font-size: .95rem; margin: 0; }

    /* Metric strip */
    .metric-row { display: flex; gap: .9rem; margin-bottom: 1.2rem; flex-wrap: wrap; }
    .metric {
        flex: 1; min-width: 160px;
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-left: 4px solid var(--teal);
        border-radius: 10px;
        padding: 1rem 1.2rem;
    }
    .metric.amber { border-left-color: var(--amber); }
    .metric.green { border-left-color: var(--green); }
    .metric .lbl  { color: #6b7c8d; font-size: .72rem; text-transform: uppercase;
                    letter-spacing: .8px; margin-bottom: .35rem; }
    .metric .val  { color: var(--navy); font-size: 1.7rem; font-weight: 700; line-height: 1; }
    .metric .sub  { color: #8b97a3; font-size: .72rem; margin-top: .25rem; }

    /* Tag badges */
    .tag {
        display: inline-block; border-radius: 4px;
        padding: 2px 8px; font-size: .72rem; font-weight: 600;
        margin-right: 4px;
    }
    .tag-sok      { background:#fff3cd; color:#7d5a00; }
    .tag-survey   { background:#d1ecf1; color:#0c5460; }
    .tag-poster   { background:#f8d7da; color:#721c24; }
    .tag-workshop { background:#e2d9f3; color:#3d2278; }

    /* Sidebar */
    section[data-testid="stSidebar"] { background: var(--navy); }
    section[data-testid="stSidebar"] * { color: #cdd9e5 !important; }
    section[data-testid="stSidebar"] .stRadio label { font-weight: 500; }
    section[data-testid="stSidebar"] h2 {
        color: #ffffff !important;
        font-size: .95rem;
        letter-spacing: .4px;
        text-transform: uppercase;
        border-bottom: 1px solid var(--slate);
        padding-bottom: .5rem;
        margin: .4rem 0 .8rem;
    }

    /* Results header */
    .results-bar {
        display: flex; align-items: center; justify-content: space-between;
        background: #f7f9fc; border: 1px solid var(--border); border-radius: 10px;
        padding: .7rem 1rem; margin-bottom: 1rem;
    }
    .results-bar .count { color: var(--navy); font-size: 1rem; font-weight: 700; }
    .results-bar .sub   { color: #6b7c8d; font-size: .85rem; }

    /* Paper detail card */
    .paper-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 1.4rem 1.6rem;
        margin-top: 1rem;
    }
    .paper-card h3 { color: var(--navy); margin: 0 0 .6rem; }
    .paper-meta {
        display: flex; gap: 1.5rem; color: #6b7c8d; font-size: .85rem;
        margin-bottom: .8rem; flex-wrap: wrap;
    }

    /* Misc */
    .stDataFrame { border-radius: 8px; overflow: hidden; }
    div[data-testid="stExpander"] { border-radius: 8px; }
    .footer { color: #94a3b8; font-size: .78rem; text-align: center;
              margin-top: 2rem; padding-top: 1rem; border-top: 1px solid var(--border); }
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
    c = Collector()
    c.papers = c._load_papers_from_disk()
    return c


def _event_options(collector: Collector) -> list[str]:
    events = sorted({p.event for p in collector.papers if p.event})
    return ["All venues"] + events


def _paper_tags_html(title: str | None) -> str:
    if not title:
        return ""
    tl = title.lower()
    tags = []
    if "sok" in tl or "systematization of knowledge" in tl:
        tags.append('<span class="tag tag-sok">SoK</span>')
    if "survey" in tl or "systematic review" in tl or "literature review" in tl:
        tags.append('<span class="tag tag-survey">Survey</span>')
    if "poster" in tl:
        tags.append('<span class="tag tag-poster">Poster</span>')
    if "workshop" in tl:
        tags.append('<span class="tag tag-workshop">Workshop</span>')
    return "".join(tags)


def _apply_post_filters(papers, only_sok, only_survey, only_poster, only_abstract):
    out = papers
    if only_sok:
        out = [p for p in out if p.title and (
            "sok" in p.title.lower() or "systematization of knowledge" in p.title.lower()
        )]
    if only_survey:
        out = [p for p in out if p.title and any(
            kw in p.title.lower() for kw in ("survey", "systematic review", "literature review")
        )]
    if only_poster:
        out = [p for p in out if p.title and "poster" in p.title.lower()]
    if only_abstract:
        out = [p for p in out if p.abstract]
    return out


def _header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="app-header"><h1>{title}</h1><p>{subtitle}</p></div>',
        unsafe_allow_html=True,
    )


def _metric_strip(stats: dict, filtered_count: int | None = None) -> None:
    total = stats["total_papers"]
    with_abs = stats["with_abstracts"]
    pct = (with_abs / total * 100) if total else 0
    venues = len(stats["by_event"])
    extra = (
        f'<div class="metric green"><div class="lbl">Currently shown</div>'
        f'<div class="val">{filtered_count:,}</div></div>'
        if filtered_count is not None else ""
    )
    st.markdown(
        f"""
        <div class="metric-row">
          <div class="metric">
            <div class="lbl">Total papers</div>
            <div class="val">{total:,}</div>
            <div class="sub">across {venues} venues</div>
          </div>
          <div class="metric amber">
            <div class="lbl">With abstract</div>
            <div class="val">{with_abs:,}</div>
            <div class="sub">{pct:.1f}% coverage</div>
          </div>
          <div class="metric">
            <div class="lbl">Years covered</div>
            <div class="val">{len(stats["by_year"])}</div>
            <div class="sub">{min(stats["by_year"], default="—")}–{max(stats["by_year"], default="—")}</div>
          </div>
          {extra}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Pages ──────────────────────────────────────────────────────────────────

def page_search():
    collector = _load_collector()
    stats = collector.db.get_statistics()

    # ── Sidebar filters ───────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## Filters")

        with st.expander("Text", expanded=True):
            title_f = st.text_input("Title contains", placeholder="e.g., SOC, authentication")
            abstract_f = st.text_input("Abstract contains", placeholder="e.g., LLM, adversarial")
            author_f = st.text_input("Author contains", placeholder="e.g., Sekar, Paxson")
            tech_f = st.text_input("Topic / tech", placeholder="e.g., blockchain, 5G")

        with st.expander("Venue & year", expanded=True):
            event_f = st.selectbox("Venue", _event_options(collector))
            year_options = ["All years"] + sorted(stats["by_year"].keys(), reverse=True)
            year_f = st.selectbox("Year", year_options)

        with st.expander("Paper type", expanded=False):
            only_sok = st.checkbox("SoK / Systematization of Knowledge")
            only_survey = st.checkbox("Survey / systematic review")
            only_poster = st.checkbox("Poster")
            only_abstract = st.checkbox("Has abstract")

        st.markdown("## Display")
        page_size = st.select_slider("Results per page", options=PAGE_SIZE_OPTIONS, value=50)

    # ── Build filters and run query ───────────────────────────────────
    filters = SearchFilters()
    if title_f:    filters.title_contains    = title_f
    if abstract_f: filters.abstract_contains = abstract_f
    if author_f:   filters.author_contains   = author_f
    if tech_f:     filters.technology        = tech_f
    if event_f != "All venues": filters.event = event_f
    if year_f != "All years":   filters.year  = int(year_f)

    # Pull all matching papers (no artificial cap), then apply post-filters
    results = collector.search(filters, limit=None)
    results = _apply_post_filters(results, only_sok, only_survey, only_poster, only_abstract)

    # ── Header + metrics ───────────────────────────────────────────────
    _header("🔒 Security Research Explorer",
            "Search 9 900+ papers from the world's top security venues — "
            "CCS, S&amp;P, USENIX, NDSS, and the leading survey journals.")
    _metric_strip(stats, filtered_count=len(results))

    if not results:
        st.info("🔍 No papers match the current filters. Try widening the search.")
        return

    # ── Pagination ─────────────────────────────────────────────────────
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

    # ── Results table ──────────────────────────────────────────────────
    df = pd.DataFrame([
        {
            "Title": p.title or "—",
            "Authors": (p.authors or "—")[:80] + ("…" if p.authors and len(p.authors) > 80 else ""),
            "Venue": p.event or "—",
            "Year": p.year,
            "Abstract": "✓" if p.abstract else "—",
            "Link": p.ee or p.url or "",
        }
        for p in page_slice
    ])

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Title":    st.column_config.TextColumn("Title", width="large"),
            "Authors":  st.column_config.TextColumn("Authors", width="medium"),
            "Venue":    st.column_config.TextColumn("Venue", width="small"),
            "Year":     st.column_config.NumberColumn("Year", format="%d", width="small"),
            "Abstract": st.column_config.TextColumn("Abs.", width="small"),
            "Link":     st.column_config.LinkColumn("DOI / URL", width="small", display_text="open"),
        },
    )

    # ── Export ─────────────────────────────────────────────────────────
    col_exp1, col_exp2, _ = st.columns([1, 1, 4])
    full_rows = [
        {
            "title": p.title, "authors": p.authors, "venue": p.event,
            "year": p.year, "abstract": p.abstract, "ee": p.ee, "url": p.url,
        }
        for p in results
    ]
    full_df = pd.DataFrame(full_rows)
    with col_exp1:
        st.download_button(
            "⬇ Export CSV", full_df.to_csv(index=False).encode("utf-8"),
            "topvenues_results.csv", "text/csv", use_container_width=True,
        )
    with col_exp2:
        st.download_button(
            "⬇ Export JSON", full_df.to_json(orient="records", indent=2),
            "topvenues_results.json", "application/json", use_container_width=True,
        )

    # ── Detail view ────────────────────────────────────────────────────
    st.divider()
    st.subheader("📄 Paper details")
    titles = [p.title for p in page_slice]
    selected = st.selectbox("Select a paper from this page", titles, label_visibility="collapsed")
    if selected:
        paper = next(p for p in page_slice if p.title == selected)
        link = paper.ee or paper.url
        link_html = f'<a href="{link}" target="_blank">{link}</a>' if link else "—"
        st.markdown(
            f"""
            <div class="paper-card">
              <h3>{paper.title}</h3>
              <div class="paper-meta">
                <span><b>Authors:</b> {paper.authors or "—"}</span>
                <span><b>Venue:</b> {paper.event or "—"}</span>
                <span><b>Year:</b> {paper.year}</span>
                <span><b>Link:</b> {link_html}</span>
              </div>
              <div>{_paper_tags_html(paper.title)}</div>
              <hr style="border:none;border-top:1px solid var(--border);margin:1rem 0">
              <div style="white-space:pre-wrap">{paper.abstract or "<i>No abstract available.</i>"}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def page_insights():
    collector = _load_collector()
    stats = collector.db.get_statistics()
    _header("📊 Dataset insights", "Distribution of papers across venues, years, and abstract coverage.")
    _metric_strip(stats)

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
    st.subheader("Abstract coverage by venue")
    import sqlite3
    coverage_rows = []
    with sqlite3.connect(collector.db.db_path) as conn:
        for venue, total in stats["by_event"].items():
            with_abs = conn.execute(
                "SELECT COUNT(*) FROM papers "
                "WHERE event=? AND abstract IS NOT NULL AND abstract!=''",
                (venue,),
            ).fetchone()[0]
            coverage_rows.append({
                "Venue": venue,
                "Total": total,
                "With abstract": with_abs,
                "Coverage": f"{with_abs / total * 100:.1f}%" if total else "—",
            })
    st.dataframe(pd.DataFrame(coverage_rows), use_container_width=True, hide_index=True)


def page_pipeline():
    _header("⚙ Pipeline", "Run the data collection pipeline. Each step is incremental and safe to repeat.")

    tab_dl, tab_cons, tab_extr = st.tabs(["📥 Download", "🔗 Consolidate", "📝 Extract abstracts"])

    with tab_dl:
        st.write("Fetches DBLP JSON files for every configured venue and year. "
                 "Skips files that already exist and validate cleanly.")
        if st.button("Run download", type="primary", use_container_width=True, key="dl_btn"):
            with st.spinner("Downloading…"):
                try:
                    _run_async(Collector().run_download())
                    st.success("Download complete.")
                    st.cache_resource.clear()
                except Exception as exc:
                    st.error(f"Download failed: {exc}")

    with tab_cons:
        st.write("Merges downloaded JSON into the SQLite database. "
                 "Existing abstracts are preserved (idempotent upsert with `COALESCE`).")
        if st.button("Run consolidate", type="primary", use_container_width=True, key="cons_btn"):
            with st.spinner("Consolidating…"):
                try:
                    _run_async(Collector().run_consolidate())
                    st.success("Consolidation complete.")
                    st.cache_resource.clear()
                except Exception as exc:
                    st.error(f"Consolidation failed: {exc}")

    with tab_extr:
        st.warning("⚠ Rate-limited. Venue scraping (ACM, IEEE, USENIX, NDSS) runs sequentially; "
                   "the open-API fallback (Semantic Scholar / OpenAlex / CrossRef) runs in parallel.")
        col1, col2 = st.columns(2)
        with col1:
            batch_size = st.number_input("Batch size", 1, 100, 10)
        with col2:
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

            async def _run():
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

            try:
                _run_async(_run())
                st.success("Extraction complete.")
                st.cache_resource.clear()
            except Exception as exc:
                st.error(f"Extraction failed: {exc}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    pages = {
        "🔍 Search": page_search,
        "📊 Insights": page_insights,
        "⚙ Pipeline": page_pipeline,
    }
    with st.sidebar:
        st.markdown(
            '<h2 style="color:#fff !important;border:none !important;'
            'font-size:1.15rem !important;text-transform:none !important;'
            'letter-spacing:0 !important;margin-bottom:1rem !important">'
            '🔒 Top Security Venues</h2>',
            unsafe_allow_html=True,
        )
        page = st.radio("Navigate", list(pages.keys()), label_visibility="collapsed")
        st.markdown("<br>", unsafe_allow_html=True)

    pages[page]()

    st.markdown(
        '<div class="footer">Top Security Venues · open-source research tool · '
        'data sourced from DBLP, Semantic Scholar, OpenAlex, CrossRef</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
