"""Command-line interface for Top Venues Collector."""

import asyncio
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .collector import Collector
from .models import SearchFilters

console = Console()


@click.group()
@click.option("--base-dir", type=click.Path(), help="Base directory for data")
@click.pass_context
def cli(ctx: click.Context, base_dir: str | None) -> None:
    """Top Venues Paper Collector CLI."""
    ctx.ensure_object(dict)
    ctx.obj["base_dir"] = Path(base_dir) if base_dir else Path.cwd()


@cli.command()
@click.pass_context
def download(ctx: click.Context) -> None:
    """Download JSON files from DBLP."""
    base_dir = ctx.obj["base_dir"]

    with console.status("[bold green]Downloading papers..."):
        collector = Collector(base_dir=base_dir)
        asyncio.run(collector.run_download())

    console.print("[bold green]✓[/bold green] Download complete!")


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

    console.print("[bold yellow]⚠ This may take a while due to rate limiting...[/bold yellow]")

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


@cli.command()
@click.pass_context
def run_all(ctx: click.Context) -> None:
    """Run complete workflow (download + consolidate + extract)."""
    base_dir = ctx.obj["base_dir"]

    collector = Collector(base_dir=base_dir)
    asyncio.run(collector.run_full())


@cli.command()
@click.option("--title", "-t", help="Search in title")
@click.option("--abstract", "-a", help="Search in abstract")
@click.option("--author", "-A", help="Search in author names")
@click.option("--event", "-e", help="Filter by conference (e.g., 'ACM CCS')")
@click.option("--year", "-y", type=int, help="Filter by year")
@click.option("--tech", "-T", help="Search technology/topic")
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
    limit: int,
) -> None:
    """Search papers with filters."""
    base_dir = ctx.obj["base_dir"]

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

    collector = Collector(base_dir=base_dir)

    with console.status("[bold green]Searching..."):
        results = collector.search(filters, limit=limit)

    table = Table(title=f"Search Results ({len(results)} papers)")
    table.add_column("Title", style="cyan", no_wrap=False)
    table.add_column("Authors", style="green")
    table.add_column("Conference", style="yellow")
    table.add_column("Year", style="blue")

    for paper in results:
        table.add_row(
            paper.title or "N/A",
            (paper.authors or "N/A")[:50],
            paper.event or "Unknown",
            str(paper.year),
        )

    console.print(table)


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
    console.print(f"\n[bold]Total Papers:[/bold] {total}")
    console.print(
        f"[bold]With Abstracts:[/bold] {with_abstracts} ({with_abstracts / total * 100:.1f}%)"
    )
    console.print(f"[bold]Without Abstracts:[/bold] {data['without_abstracts']}")

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
