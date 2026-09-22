"""Command-line interface for Top Venues Collector."""

import asyncio
import csv
import json
import re
import sys
from io import StringIO
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .analytics import (
    area_year_counts,
    awarded_by_area,
    reference_authors,
    top_authors,
    topic_trend,
)
from .awards import build_corpus_award_map
from .collector import Collector
from .config import load_configuration, set_configuration_path
from .models import DownloadStatus, SearchFilters
from .profiles import PROFILE_IDS, profile_config_path
from .tiers import (
    ALL_TIERS_SCOPE,
    tier_for,
    tier_scope_options,
    tiers_in_scope,
)

console = Console()


def _load_award_map(collector: Collector) -> dict[str, list[str]]:
    """Map corpus paper_id to award labels from data/awards, or empty if absent."""
    awards_dir = collector.base_dir / "data" / "awards"
    if not awards_dir.exists():
        return {}
    return build_corpus_award_map(awards_dir, collector.db.db_path)


def _build_filters(
    title: str | None,
    abstract: str | None,
    author: str | None,
    event: str | None,
    year: int | None,
    tech: str | None,
) -> SearchFilters:
    """Build a SearchFilters object from CLI options."""
    filters = SearchFilters()
    if title:
        filters.title_contains = title
    if abstract:
        filters.abstract_contains = abstract
    if author:
        filters.author_contains = author
    if event:
        filters.event = event
    if year:
        filters.year = year
    if tech:
        filters.technology = tech
    return filters


def _paper_in_tier_scope(event: str | None, tier_scope: str) -> bool:
    """Return whether an event belongs to the requested declared tier scope."""
    allowed_tiers = tiers_in_scope(tier_scope)
    return allowed_tiers is None or tier_for(event) in allowed_tiers


def _materialized_json_years(json_dir: Path) -> dict[str, set[int]]:
    """Return downloaded DBLP JSON years keyed by configured event id."""
    years_by_event: dict[str, set[int]] = {}
    pattern = re.compile(r"data_(?P<event>.+?)(?P<year>\d{4})\.json$")
    for path in json_dir.glob("data_*.json"):
        match = pattern.match(path.name)
        if not match:
            continue
        event = match.group("event")
        year = int(match.group("year"))
        years_by_event.setdefault(event, set()).add(year)
    return years_by_event


@click.group()
@click.option("--base-dir", type=click.Path(), help="Base directory for data")
@click.option(
    "--profile",
    type=click.Choice(PROFILE_IDS),
    envvar="TOPVENUES_PROFILE",
    help="Use a named immutable profile; omit it to use mutable config.yaml.",
)
@click.pass_context
def cli(ctx: click.Context, base_dir: str | None, profile: str | None) -> None:
    """Bibliographic corpus command-line interface."""
    ctx.ensure_object(dict)
    resolved_base = Path(base_dir) if base_dir else Path.cwd()
    ctx.obj["base_dir"] = resolved_base
    ctx.obj["profile"] = profile
    config_path = (
        profile_config_path(profile, resolved_base) if profile else resolved_base / "config.yaml"
    )
    if not config_path.is_file():
        raise click.UsageError(f"configuration not found: {config_path}")
    set_configuration_path(config_path)


@cli.command()
@click.option(
    "--event",
    "events",
    multiple=True,
    help="Download only the selected configured event key. Repeat for multiple events.",
)
@click.option(
    "--year",
    "years",
    multiple=True,
    type=int,
    help="Download only the selected year. Repeat for multiple years.",
)
@click.pass_context
def download(ctx: click.Context, events: tuple[str, ...], years: tuple[int, ...]) -> None:
    """Download JSON files from DBLP."""
    base_dir = ctx.obj["base_dir"]

    with console.status("[bold green]Downloading papers..."):
        collector = Collector(base_dir=base_dir)
        if events:
            collector.config.events = list(events)
        if years:
            collector.config.years = list(years)
        asyncio.run(collector.run_download())

    console.print("[bold green]✓[/bold green] Download complete!")


@cli.command("materialization-status")
@click.pass_context
def materialization_status(ctx: click.Context) -> None:
    """Show which configured events have DBLP JSON files on disk."""
    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    expected_years = set(collector.config.effective_years())
    years_by_event = _materialized_json_years(collector.json_dir)

    table = Table(title="Configured Event Materialization")
    table.add_column("Event", style="cyan")
    table.add_column("JSON years", style="green")
    table.add_column("Missing years", style="yellow")

    complete = 0
    for event in collector.config.events:
        present = sorted(years_by_event.get(event, set()))
        missing = sorted(expected_years - set(present))
        if not missing:
            complete += 1
        table.add_row(
            event,
            ", ".join(str(year) for year in present) or "-",
            ", ".join(str(year) for year in missing) or "-",
        )

    console.print(table)
    console.print(f"[bold]Complete events:[/bold] {complete}/{len(collector.config.events)}")


@cli.command("materialize-from-dump")
@click.option(
    "--event",
    "events",
    multiple=True,
    help="Materialize only the selected configured event key. Repeat for multiple events.",
)
@click.option(
    "--year",
    "years",
    multiple=True,
    type=int,
    help="Materialize only the selected year. Repeat for multiple years.",
)
@click.option(
    "--dump",
    "dump_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to dblp.xml or dblp.xml.gz (default: data/dblp/dblp.xml.gz).",
)
@click.option("--overwrite", is_flag=True, help="Regenerate existing valid JSON files.")
@click.pass_context
def materialize_from_dump(
    ctx: click.Context,
    events: tuple[str, ...],
    years: tuple[int, ...],
    dump_path: Path | None,
    overwrite: bool,
) -> None:
    """Materialize DBLP JSON files from the local XML dump."""
    from .dblp_dump_materializer import DblpDumpMaterializer

    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    dump_path = dump_path or (base_dir / "data" / "dblp" / "dblp.xml.gz")

    with console.status("[bold green]Materializing DBLP records from local dump..."):
        entries = DblpDumpMaterializer(
            config=collector.config,
            json_dir=collector.json_dir,
            log_dir=collector.log_dir,
            dump_path=dump_path,
        ).materialize(
            events=list(events) if events else None,
            years=list(years) if years else None,
            overwrite=overwrite,
        )

    downloaded = sum(1 for entry in entries if entry.status == DownloadStatus.DOWNLOADED)
    valid = sum(1 for entry in entries if entry.status == DownloadStatus.VALID)
    failed = sum(1 for entry in entries if entry.status == DownloadStatus.FAILED)
    console.print("[bold green]✓[/bold green] Local dump materialization complete!")
    console.print(f"  Downloaded: {downloaded}")
    console.print(f"  Already valid: {valid}")
    console.print(f"  Failed: {failed}")


@cli.command()
@click.pass_context
def consolidate(ctx: click.Context) -> None:
    """Consolidate JSON files into dataset."""
    base_dir = ctx.obj["base_dir"]

    with console.status("[bold green]Consolidating data..."):
        collector = Collector(base_dir=base_dir)
        asyncio.run(collector.run_consolidate())

    console.print("[bold green]✓[/bold green] Consolidation complete!")


@cli.command()
@click.pass_context
def extract(ctx: click.Context) -> None:
    """Extract abstracts from papers."""
    base_dir = ctx.obj["base_dir"]

    console.print("[bold yellow]This may take a while due to rate limiting.[/bold yellow]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("[green]Extracting abstracts...", total=None)

        collector = Collector(base_dir=base_dir)
        asyncio.run(collector.run_extract())

        progress.update(task, completed=True)

    console.print("[bold green]✓[/bold green] Extraction complete!")


@cli.command("backfill-abstracts")
@click.option("--event", "-e", help="Canonical event name to backfill, e.g. ACSAC.")
@click.option("--limit", type=int, default=None, help="Maximum number of missing abstracts to try.")
@click.option(
    "--concurrency", type=int, default=4, show_default=True, help="Concurrent DOI API requests."
)
@click.pass_context
def backfill_abstracts(
    ctx: click.Context,
    event: str | None,
    limit: int | None,
    concurrency: int,
) -> None:
    """Backfill missing abstracts from DOI metadata APIs."""
    from .database import write_gzipped_snapshot

    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)

    with console.status("[bold green]Backfilling missing abstracts from DOI APIs..."):
        result = asyncio.run(
            collector.run_backfill_abstracts(event=event, limit=limit, concurrency=concurrency)
        )
        collector.export_dataset_csv()
        write_gzipped_snapshot(collector.db.db_path)

    console.print("[bold green]✓[/bold green] Abstract backfill complete")
    console.print(f"  Scanned: {result['scanned']}")
    console.print(f"  [green]Updated:[/green] {result['updated']}")
    console.print(f"  Missing after DOI APIs: {result['missing']}")
    console.print(f"  Without DOI: {result['no_doi']}")


@cli.command("refresh-db")
@click.pass_context
def refresh_db(ctx: click.Context) -> None:
    """Force-refresh papers.db from the tracked papers.db.gz snapshot."""
    from .database import CorpusBusyError, refresh_from_gzipped_snapshot

    base_dir = ctx.obj["base_dir"]
    profile = ctx.obj.get("profile")
    config_path = profile_config_path(profile, base_dir) if profile else base_dir / "config.yaml"
    configuration = load_configuration(config_path)
    db_path = base_dir / configuration.data_dir / "papers.db"
    gz = Path(configuration.snapshot_path or db_path.with_suffix(".db.gz"))
    if not gz.is_absolute():
        gz = base_dir / gz
    if not gz.exists():
        console.print(f"[bold red]No snapshot found at {gz}.[/bold red]")
        raise click.ClickException("refresh aborted: required snapshot is missing")
    try:
        refresh_from_gzipped_snapshot(db_path, gz)
    except CorpusBusyError as error:
        raise click.ClickException(str(error)) from error
    console.print(f"[bold green]✓[/bold green] Refreshed from {gz.name}")


@cli.command("write-snapshot")
@click.pass_context
def write_snapshot(ctx: click.Context) -> None:
    """Compress papers.db → papers.db.gz for distribution."""
    from .database import write_gzipped_snapshot

    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    if not collector.db.db_path.exists():
        console.print(f"[bold red]No DB found at {collector.db.db_path}.[/bold red]")
        return
    csv_path = collector.export_dataset_csv()
    gz = write_gzipped_snapshot(collector.db.db_path)
    mb = gz.stat().st_size / 1024 / 1024
    console.print(f"[bold green]✓[/bold green] Wrote {gz.name} ({mb:.1f} MB)")
    console.print(f"[bold green]✓[/bold green] Exported {csv_path.name} from the same DB")


@cli.command()
@click.option("--concurrency", default=4, show_default=True, help="Concurrent DBLP requests.")
@click.pass_context
def bibtex(ctx: click.Context, concurrency: int) -> None:
    """Fetch missing BibTeX entries via the DBLP per-record API."""
    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    with console.status("[bold green]Fetching BibTeX…"):
        asyncio.run(collector.run_bibtex(concurrency=concurrency))
    console.print("[bold green]✓[/bold green] BibTeX backfill complete!")


@cli.command("bibtex-from-dump")
@click.option(
    "--dump-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Where to store dblp.xml.gz and dblp.dtd (default: <base>/data/dblp).",
)
@click.option(
    "--force-download", is_flag=True, help="Re-download even if the dump is already on disk."
)
@click.pass_context
def bibtex_from_dump(ctx: click.Context, dump_dir: Path | None, force_download: bool) -> None:
    """Fill BibTeX entries from a single download of the DBLP XML dump."""
    from .bibtex_dump import download_dump, parse_dump_for_keys
    from .sqlite_connection import managed_sqlite_connection

    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    dump_dir = dump_dir or (base_dir / "data" / "dblp")

    rows = collector.db.get_papers_without_bibtex()
    target_keys = {row["key"] for row in rows if row.get("key")}
    if not target_keys:
        console.print("[bold green]Nothing to do — every paper already has BibTeX.[/bold green]")
        return

    console.print(f"[bold]Targets:[/bold] {len(target_keys):,} papers without BibTeX.")

    with console.status("[bold green]Downloading DBLP dump (~600 MB)…"):
        xml_path, _ = download_dump(dump_dir, force=force_download)

    with console.status("[bold green]Parsing dump and matching keys…"):
        bibtex_by_key = parse_dump_for_keys(xml_path, target_keys)

    console.print(f"  Matched {len(bibtex_by_key):,}/{len(target_keys):,} keys in dump.")

    with managed_sqlite_connection(collector.db.db_path) as conn:
        conn.executemany(
            "UPDATE papers SET bibtex=?, updated_at=CURRENT_TIMESTAMP WHERE key=?",
            [(bib, key) for key, bib in bibtex_by_key.items()],
        )
    console.print(f"[bold green]✓[/bold green] Populated {len(bibtex_by_key):,} BibTeX entries.")


@cli.command("bibtex-local")
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing BibTeX entries instead of only filling blanks.",
)
@click.pass_context
def bibtex_local(ctx: click.Context, overwrite: bool) -> None:
    """Generate BibTeX offline from fields already in the database."""
    from .bibtex_local import paper_to_bibtex
    from .models import Paper
    from .sqlite_connection import managed_sqlite_connection

    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)

    rows = collector.db.get_all_papers() if overwrite else collector.db.get_papers_without_bibtex()
    populated = 0
    with managed_sqlite_connection(collector.db.db_path) as conn:
        for row in rows:
            paper = Paper(**row)
            entry = paper_to_bibtex(paper)
            if not entry:
                continue
            conn.execute(
                "UPDATE papers SET bibtex=?, updated_at=CURRENT_TIMESTAMP WHERE paper_id=?",
                (entry, paper.paper_id),
            )
            populated += 1
    console.print(f"[bold green]✓[/bold green] Generated {populated:,} BibTeX entries locally.")


@cli.command()
@click.pass_context
def run_all(ctx: click.Context) -> None:
    """Run complete workflow (download + consolidate + extract + bibtex)."""
    base_dir = ctx.obj["base_dir"]

    collector = Collector(base_dir=base_dir)
    asyncio.run(collector.run_full())


@cli.command()
@click.option("--title", "-t", help="Search in title")
@click.option("--abstract", "-a", help="Search in abstract")
@click.option("--author", "-A", help="Search in author names")
@click.option("--event", "-e", help="Filter by configured event name")
@click.option("--year", "-y", type=int, help="Filter by year")
@click.option("--tech", "-T", help="Search technology/topic")
@click.option(
    "--rank", "-r", "rank_query", help="BM25-ranked full-text search over title/abstract/authors"
)
@click.option("--award", "-w", is_flag=True, help="Only papers with a recorded paper award")
@click.option(
    "--tier-scope",
    type=click.Choice(tier_scope_options()),
    default=ALL_TIERS_SCOPE,
    show_default=True,
    help="Restrict results to a declared venue-tier scope.",
)
@click.option("--limit", "-l", type=int, default=50, help="Limit results")
@click.pass_context
def search(
    ctx: click.Context,
    title: str | None,
    abstract: str | None,
    author: str | None,
    event: str | None,
    year: int | None,
    tech: str | None,
    rank_query: str | None,
    award: bool,
    tier_scope: str,
    limit: int,
) -> None:
    """Search papers with filters, or rank by relevance with --rank."""
    base_dir = ctx.obj["base_dir"]

    collector = Collector(base_dir=base_dir)
    award_map = _load_award_map(collector)

    # When filtering to award winners, search without the row cap first so the
    # cap applies after the award filter rather than truncating beforehand.
    with console.status("[bold green]Searching..."):
        if rank_query:
            rows = collector.db.search_ranked(
                rank_query,
                event=event,
                year=year,
                limit=None,
            )
            rows = [row for row in rows if _paper_in_tier_scope(row["event"], tier_scope)]
            if award:
                rows = [row for row in rows if row["paper_id"] in award_map]
            rows = rows[:limit]
            _print_ranked_results(rank_query, rows, award_map)
            return
        filters = _build_filters(title, abstract, author, event, year, tech)
        results = collector.search(filters, limit=None)
    results = [paper for paper in results if _paper_in_tier_scope(paper.event, tier_scope)]
    if award:
        results = [paper for paper in results if paper.paper_id in award_map]
    results = results[:limit]

    table = Table(title=f"Search Results ({len(results)} papers)")
    table.add_column("Title", style="cyan", no_wrap=False)
    table.add_column("Authors", style="green")
    table.add_column("Conference", style="yellow")
    table.add_column("Tier", style="magenta")
    table.add_column("Year", style="blue")
    table.add_column("Award", style="gold1")

    for paper in results:
        labels = award_map.get(paper.paper_id)
        table.add_row(
            paper.title or "N/A",
            (paper.authors or "N/A")[:50],
            paper.event or "Unknown",
            tier_for(paper.event),
            str(paper.year),
            "; ".join(labels) if labels else "—",
        )

    console.print(table)


def _print_ranked_results(query: str, rows: list[dict], award_map: dict[str, list[str]]) -> None:
    table = Table(title=f"Ranked Results for “{query}” ({len(rows)} papers)")
    table.add_column("Score", style="magenta", justify="right")
    table.add_column("Title", style="cyan", no_wrap=False)
    table.add_column("Authors", style="green")
    table.add_column("Conference", style="yellow")
    table.add_column("Tier", style="magenta")
    table.add_column("Year", style="blue")
    table.add_column("Award", style="gold1")

    for row in rows:
        labels = award_map.get(row["paper_id"])
        # SQLite BM25 is negative (lower = better); show a positive score.
        table.add_row(
            f"{-row['rank']:.2f}",
            row["title"] or "N/A",
            (row["authors"] or "N/A")[:50],
            row["event"] or "Unknown",
            tier_for(row["event"]),
            str(row["year"]),
            "; ".join(labels) if labels else "—",
        )

    console.print(table)


@cli.command()
@click.option(
    "--topic",
    "-T",
    help="Restrict to papers matching this topic "
    "(case-insensitive over title and abstract), e.g. 'LLM'",
)
@click.option(
    "--area",
    "-a",
    help="Restrict to a research area: security, ai, networks, mobile, systems, cross-area",
)
@click.option(
    "--position",
    type=click.Choice(["any", "first", "last"]),
    default="any",
    show_default=True,
    help="Count appearances in any, first, or last authorship position.",
)
@click.option(
    "--limit", "-l", type=int, default=15, show_default=True, help="Number of authors to show"
)
@click.option(
    "--tier-scope",
    type=click.Choice(tier_scope_options()),
    default=ALL_TIERS_SCOPE,
    show_default=True,
    help="Restrict the author ranking to a declared venue-tier scope.",
)
@click.option(
    "--metric",
    type=click.Choice(["paper_count", "tier_weighted"]),
    default="paper_count",
    show_default=True,
    help="Order by raw paper appearances or the declared venue-tier heuristic.",
)
@click.pass_context
def authors(
    ctx: click.Context,
    topic: str | None,
    area: str | None,
    position: str,
    limit: int,
    tier_scope: str,
    metric: str,
) -> None:
    """Rank author visibility in this corpus with a declared metric."""
    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    awards_dir = collector.db.db_path.parent.parent / "awards"

    with console.status("[bold green]Ranking reference authors..."):
        ranked = reference_authors(
            collector.db.db_path,
            topic=topic,
            area=area,
            limit=limit,
            awards_dir=awards_dir,
            position=position,
            allowed_tiers=tiers_in_scope(tier_scope),
            ranking_metric=metric,
        )

    scope = (
        " · ".join(
            filter(
                None,
                [
                    f"topic: {topic}" if topic else None,
                    f"area: {area}" if area else None,
                    f"position: {position}",
                    f"tier scope: {tier_scope}",
                    f"metric: {metric}",
                ],
            )
        )
        or "whole corpus"
    )
    table = Table(title=f"Author visibility in corpus — {scope}")
    table.add_column("#", style="dim", justify="right")
    table.add_column("Author", style="cyan")
    table.add_column("Score", style="magenta", justify="right")
    table.add_column("Papers", justify="right")
    table.add_column("Top-4", style="bold", justify="right")
    table.add_column("Other top-tier", justify="right")
    table.add_column("Awards", style="gold1", justify="right")
    table.add_column("Recent", justify="right")
    table.add_column("Active", style="blue")
    table.add_column("Main venues", style="yellow")

    for position, entry in enumerate(ranked, start=1):
        table.add_row(
            str(position),
            entry["author"],
            f"{entry['score']:.1f}",
            str(entry["papers"]),
            str(entry["top4"]),
            str(entry["top_tier"]),
            str(entry["awards"]) if entry["awards"] else "—",
            str(entry["recent_papers"]),
            f"{entry['first_year']}–{entry['last_year']}",
            ", ".join(entry["venues"]),
        )

    console.print(table)


@cli.command()
@click.option(
    "--topic", "-T", required=True, help="Topic to trace (case-insensitive over title and abstract)"
)
@click.option(
    "--area",
    "-a",
    help="Restrict to a research area: security, ai, networks, mobile, systems, cross-area",
)
@click.option("--since", type=int, default=None, help="First year to include, e.g. 2019")
@click.option(
    "--tier-scope",
    type=click.Choice(tier_scope_options()),
    default=ALL_TIERS_SCOPE,
    show_default=True,
    help="Restrict the trend denominator and matches to a declared venue-tier scope.",
)
@click.pass_context
def trends(
    ctx: click.Context,
    topic: str,
    area: str | None,
    since: int | None,
    tier_scope: str,
) -> None:
    """Trace a topic's yearly volume, corpus share, and main venues."""
    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)

    with console.status("[bold green]Computing topic trend..."):
        trend = topic_trend(
            collector.db.db_path,
            topic,
            area=area,
            year_start=since,
            allowed_tiers=tiers_in_scope(tier_scope),
        )

    scope = f"topic: {topic} · tier scope: {tier_scope}"
    if area:
        scope += f" · area: {area}"
    table = Table(title=f"Topic trend — {scope} ({trend['total']:,} papers)")
    table.add_column("Year", style="blue")
    table.add_column("Papers", justify="right")
    table.add_column("% of corpus", justify="right", style="magenta")
    table.add_column("Volume", style="cyan")

    peak = max((row["papers"] for row in trend["by_year"]), default=0)
    for row in trend["by_year"]:
        bar = "█" * round(30 * row["papers"] / peak) if peak else ""
        table.add_row(str(row["year"]), f"{row['papers']:,}", f"{row['share_pct']:.2f}%", bar)

    console.print(table)
    partial_years = sorted(
        set(collector.config.partial_years) & {row["year"] for row in trend["by_year"]}
    )
    if partial_years:
        labels = ", ".join(str(year) for year in partial_years)
        console.print(
            f"[yellow]Partial publication year(s): {labels}. "
            "Do not interpret them as completed annual trends.[/yellow]"
        )
    if trend["top_venues"]:
        venues = ", ".join(f"{event} ({count:,})" for event, count in trend["top_venues"])
        console.print(f"[bold]Main venues:[/bold] {venues}")


@cli.command("build-fts")
@click.pass_context
def build_fts(ctx: click.Context) -> None:
    """Build (or rebuild) the BM25 full-text index used by search --rank."""
    import time

    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    with console.status("[bold green]Building FTS5 index..."):
        started = time.perf_counter()
        collector.db.build_fts_index()
        elapsed = time.perf_counter() - started
    console.print(f"[bold green]✓[/bold green] FTS5 index built in {elapsed:.1f}s")


@cli.command("export")
@click.option("--format", "fmt", type=click.Choice(["bibtex", "csv", "json"]), required=True)
@click.option("--output", "-o", type=click.Path(path_type=Path), default=None)
@click.option("--title", "-t", help="Search in title")
@click.option("--abstract", "-a", help="Search in abstract")
@click.option("--author", "-A", help="Search in author names")
@click.option("--event", "-e", help="Filter by configured event name")
@click.option("--year", "-y", type=int, help="Filter by year")
@click.option("--tech", "-T", help="Search technology/topic")
@click.option(
    "--tier-scope",
    type=click.Choice(tier_scope_options()),
    default=ALL_TIERS_SCOPE,
    show_default=True,
    help="Restrict exported rows to a declared venue-tier scope.",
)
@click.option("--limit", "-l", type=int, default=None, help="Limit exported rows")
@click.pass_context
def export_results(
    ctx: click.Context,
    fmt: str,
    output: Path | None,
    title: str | None,
    abstract: str | None,
    author: str | None,
    event: str | None,
    year: int | None,
    tech: str | None,
    tier_scope: str,
    limit: int | None,
) -> None:
    """Export filtered results as BibTeX, CSV, or JSON."""
    base_dir = ctx.obj["base_dir"]
    filters = _build_filters(title, abstract, author, event, year, tech)
    collector = Collector(base_dir=base_dir)
    rows = collector.search(filters, limit=None)
    rows = [paper for paper in rows if _paper_in_tier_scope(paper.event, tier_scope)]
    if limit is not None:
        rows = rows[:limit]

    if fmt == "bibtex":
        payload = "\n\n".join(paper.bibtex for paper in rows if paper.bibtex)
    elif fmt == "json":
        payload = json.dumps(
            [paper.model_dump(mode="json") for paper in rows],
            ensure_ascii=False,
            indent=2,
        )
    else:
        buffer = StringIO()
        fieldnames = list(rows[0].model_dump(mode="json").keys()) if rows else []
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(paper.model_dump(mode="json") for paper in rows)
        payload = buffer.getvalue()

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload, encoding="utf-8")
        console.print(f"[bold green]✓[/bold green] Exported {len(rows):,} papers to {output}")
    else:
        click.echo(payload)


@cli.command("export-hf")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("data/hf-dataset"),
    show_default=True,
    help="Directory that receives the Hugging Face dataset files",
)
@click.option(
    "--repo-id",
    default="sidneibarbieri/topvenues",
    show_default=True,
    help="Hub dataset id used in the card's usage example",
)
@click.option(
    "--release-tag",
    default=None,
    help="Immutable source-release tag recorded in the dataset card.",
)
@click.pass_context
def export_hf(ctx: click.Context, output: Path, repo_id: str, release_tag: str | None) -> None:
    """Export the corpus as a Hugging Face dataset (Parquet + dataset card)."""
    from .hf_export import export_hf_dataset

    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    with console.status("[bold green]Exporting Hugging Face dataset..."):
        stats = export_hf_dataset(
            collector.db.db_path,
            output,
            repo_id=repo_id,
            profile_id=ctx.obj["profile"],
            release_tag=release_tag,
            base_dir=base_dir,
        )

    console.print(
        f"[bold green]✓[/bold green] Wrote {stats['total']:,} papers "
        f"({stats['n_abstracts']:,} abstracts, "
        f"{stats['security_pct']:.1f}% coverage on the security core) to {output}"
    )
    console.print(
        f"  Upload with: [bold]hf upload-large-folder {repo_id} --repo-type dataset {output}[/bold]"
    )


@cli.command()
@click.pass_context
def stats(ctx: click.Context) -> None:
    """Show dataset statistics from the database."""
    base_dir = ctx.obj["base_dir"]

    collector = Collector(base_dir=base_dir)
    data = collector.db.get_statistics()

    total = data["total_papers"]
    if total == 0:
        console.print("[bold red]No papers in database. Run 'consolidate' first.[/bold red]")
        return

    with_abstracts = data["with_abstracts"]
    with_bibtex = data.get("with_bibtex", 0)
    console.print(f"\n[bold]Total Papers:[/bold] {total}")
    console.print(
        f"[bold]With Abstracts:[/bold] {with_abstracts} ({with_abstracts / total * 100:.2f}%)"
    )
    console.print(f"[bold]Without Abstracts:[/bold] {data['without_abstracts']}")
    console.print(f"[bold]With BibTeX:[/bold]    {with_bibtex} ({with_bibtex / total * 100:.2f}%)")

    console.print("\n[bold]By Conference:[/bold]")
    for event, count in sorted(data["by_event"].items(), key=lambda x: x[1], reverse=True):
        console.print(f"  {event}: {count}")

    console.print("\n[bold]By Year:[/bold]")
    for year, count in sorted(data["by_year"].items()):
        console.print(f"  {year}: {count}")


@cli.command("db-migrate")
@click.pass_context
def db_migrate(ctx: click.Context) -> None:
    """Migrate existing master_dataset.csv into the database."""
    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    csv_path = collector.data_dir / "master_dataset.csv"

    if not csv_path.exists():
        console.print(f"[bold red]File not found: {csv_path}[/bold red]")
        return

    with console.status(f"[bold green]Migrating {csv_path.name}..."):
        count = collector.db.migrate_from_csv(csv_path)

    console.print(f"[bold green]✓[/bold green] Migrated {count} papers to database.")
    data = collector.db.get_statistics()
    console.print(
        f"  Total in DB: {data['total_papers']}  |  With abstract: {data['with_abstracts']}"
    )


@cli.command("db-recover-abstracts")
@click.option(
    "--source",
    "source",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="CSV with abstracts to import (defaults to data/dataset/old_master_dataset.csv).",
)
@click.pass_context
def db_recover_abstracts(ctx: click.Context, source: Path | None) -> None:
    """Fill empty abstracts in the DB from a legacy CSV. Idempotent and non-destructive."""
    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    csv_path = source or collector.data_dir / "old_master_dataset.csv"

    if not csv_path.exists():
        console.print(f"[bold red]File not found: {csv_path}[/bold red]")
        return

    with console.status(f"[bold green]Importing abstracts from {csv_path.name}..."):
        result = collector.db.import_abstracts_from_csv(csv_path)

    console.print(f"[bold green]✓[/bold green] Recovered abstracts from {csv_path.name}")
    console.print(f"  Scanned (CSV rows with abstract): {result.scanned}")
    console.print(f"  Matched in DB:                    {result.matched}")
    console.print(f"  [green]Updated:[/green]                          {result.updated}")
    console.print(f"  Skipped (DB already had one):     {result.skipped_existing}")
    console.print(f"  Missing in DB:                    {result.missing_in_db}")


@cli.command()
@click.option("--area", help="Restrict the author ranking to one area (e.g. security).")
@click.option(
    "--authors",
    "author_limit",
    type=int,
    default=10,
    show_default=True,
    help="How many prominent authors to list.",
)
@click.pass_context
def analytics(ctx: click.Context, area: str | None, author_limit: int) -> None:
    """Area-level analytics: trends, prominent authors, and award standouts."""
    base_dir = ctx.obj["base_dir"]
    collector = Collector(base_dir=base_dir)
    db_path = collector.db.db_path

    year_counts = area_year_counts(db_path)
    years = sorted({year for counts in year_counts.values() for year in counts})
    recent = years[-5:]
    trend = Table(title="Papers per area per year (recent)")
    trend.add_column("Area", style="cyan")
    for year in recent:
        trend.add_column(str(year), justify="right", style="blue")
    trend.add_column("total", justify="right", style="green")
    for area_name in sorted(year_counts, key=lambda a: -sum(year_counts[a].values())):
        counts = year_counts[area_name]
        trend.add_row(
            area_name, *[str(counts.get(y, 0)) for y in recent], str(sum(counts.values()))
        )
    console.print(trend)

    scope = f" in {area}" if area else ""
    ranking = Table(title=f"Prominent authors{scope}")
    ranking.add_column("Author", style="cyan")
    ranking.add_column("Papers", justify="right", style="green")
    for name, count in top_authors(db_path, area=area, limit=author_limit):
        ranking.add_row(name, str(count))
    console.print(ranking)

    awards_dir = db_path.parent.parent / "awards"
    if awards_dir.exists():
        by_area = awarded_by_area(awards_dir, db_path)
        standout = Table(title="Award-winning papers by area")
        standout.add_column("Area", style="cyan")
        standout.add_column("Awards", justify="right", style="gold1")
        for area_name in sorted(by_area, key=lambda a: -len(by_area[a])):
            standout.add_row(area_name, str(len(by_area[area_name])))
        console.print(standout)


@cli.command()
def web() -> None:
    """Launch web interface."""
    console.print("[bold green]Starting web interface...[/bold green]")
    console.print("Open http://localhost:8501 in your browser")

    import subprocess

    web_dir = Path(__file__).parent.parent / "web"
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(web_dir / "app.py")])


def main() -> None:
    """Entry point."""
    cli()


if __name__ == "__main__":
    main()
