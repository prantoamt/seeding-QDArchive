"""CLI entry point for the pipeline."""

import click
from rich.console import Console

from pipeline.config import EXPORTS_DIR, ensure_dirs
from pipeline.db.connection import get_session, init_db
from pipeline.db.export import export_to_csv
from pipeline.db.models import File
from pipeline.utils.logging import setup_logging

console = Console()


@click.group()
def cli() -> None:
    """Seeding QDArchive — data acquisition pipeline."""
    ensure_dirs()
    init_db()
    setup_logging()


@cli.command()
@click.argument("source")
@click.option("--query", "-q", default=None, help="Search query string.")
@click.option("--file-type", "-t", default=None, help="Filter by file type extension.")
def search(source: str, query: str | None, file_type: str | None) -> None:
    """Search a data source for qualitative data."""
    console.print(f"[bold]Search[/bold] source={source} query={query} file_type={file_type}")
    console.print("[yellow]Not yet implemented — connectors coming soon.[/yellow]")


@cli.command()
@click.argument("source")
@click.option("--limit", "-n", default=None, type=int, help="Max records to scrape.")
def scrape(source: str, limit: int | None) -> None:
    """Scrape and download data from a source."""
    console.print(f"[bold]Scrape[/bold] source={source} limit={limit}")
    console.print("[yellow]Not yet implemented — connectors coming soon.[/yellow]")


@cli.command("export")
@click.option("--format", "fmt", default="csv", type=click.Choice(["csv"]), help="Export format.")
@click.option("--output", "-o", default=None, help="Output file path.")
def export_cmd(fmt: str, output: str | None) -> None:
    """Export the metadata database."""
    if output is None:
        output = str(EXPORTS_DIR / f"metadata.{fmt}")

    from pathlib import Path

    count = export_to_csv(Path(output))
    console.print(f"Exported {count} records to {output}")


@cli.command()
def status() -> None:
    """Show pipeline status and record counts."""
    session = get_session()
    try:
        total = session.query(File).count()
        qda = session.query(File).filter(File.is_qda_file.is_(True)).count()
        downloaded = session.query(File).filter(File.local_path.isnot(None)).count()

        console.print(f"Total records:    {total}")
        console.print(f"QDA files:        {qda}")
        console.print(f"Downloaded files: {downloaded}")
    finally:
        session.close()


@cli.command("list-sources")
def list_sources() -> None:
    """List available data source connectors."""
    sources = [
        ("zenodo", "Zenodo API", "planned"),
        ("dryad", "Dryad API", "planned"),
        ("dataverse", "Dataverse API (QDR, DANS, DataverseNO)", "planned"),
        ("ukds", "UK Data Service (scraper)", "planned"),
        ("qualidata", "QualidataNet (scraper)", "planned"),
        ("qualiservice", "Qualiservice (scraper)", "planned"),
    ]
    console.print("[bold]Available sources:[/bold]\n")
    for name, desc, state in sources:
        console.print(f"  {name:<15} {desc:<45} [{state}]")


if __name__ == "__main__":
    cli()
