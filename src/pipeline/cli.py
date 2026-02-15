"""CLI entry point for the pipeline."""

import logging
from datetime import datetime
from pathlib import Path

import click
import httpx
from rich.console import Console
from rich.table import Table

from pipeline.config import EXPORTS_DIR, QDA_EXTENSIONS, ensure_dirs
from pipeline.connectors import CONNECTORS
from pipeline.db.connection import get_session, init_db
from pipeline.db.export import export_to_csv
from pipeline.db.models import File
from pipeline.storage.file_manager import compute_sha256, get_storage_path
from pipeline.utils.license import is_open_license
from pipeline.utils.logging import setup_logging

console = Console()
logger = logging.getLogger("pipeline")


@click.group()
def cli() -> None:
    """Seeding QDArchive — data acquisition pipeline."""
    ensure_dirs()
    init_db()
    setup_logging()


def _get_connector(source: str):
    """Look up a connector by source name, or exit with an error."""
    connector = CONNECTORS.get(source)
    if connector is None:
        available = ", ".join(CONNECTORS.keys())
        console.print(f"[red]Unknown source '{source}'. Available: {available}[/red]")
        raise SystemExit(1)
    return connector


def _save_metadata_only(session, source, result, metadata, finfo, fname, file_ext, is_qda):
    """Save a metadata-only DB record for a file we couldn't download (e.g. 403)."""
    existing = (
        session.query(File)
        .filter_by(source_name=source, download_url=finfo["download_url"], file_name=fname)
        .first()
    )
    if existing:
        return  # already cataloged

    file_record = File(
        source_name=source,
        source_url=result.source_url,
        download_url=finfo["download_url"],
        file_name=fname,
        file_type=file_ext,
        file_size_bytes=finfo.get("size"),
        local_path=None,
        license_type=metadata.license_type,
        license_url=metadata.license_url,
        title=metadata.title,
        description=metadata.description,
        authors=metadata.authors,
        date_published=metadata.date_published,
        tags="; ".join(metadata.tags) if metadata.tags else None,
        is_qda_file=is_qda,
        notes="access restricted (403)",
    )
    session.add(file_record)
    session.commit()


@cli.command()
@click.argument("source")
@click.option("--query", "-q", default="qualitative", help="Search query string.")
@click.option("--file-type", "-t", default=None, help="Filter by file type extension.")
def search(source: str, query: str, file_type: str | None) -> None:
    """Search a data source for qualitative data."""
    connector = _get_connector(source)

    console.print(f"[bold]Searching {source}[/bold] for '{query}'...")
    try:
        results = connector.search(query, file_type)
    except Exception as e:
        console.print(f"[red]Search failed: {e}[/red]")
        raise SystemExit(1) from e

    if not results:
        console.print("[yellow]No results found.[/yellow]")
        return

    table = Table(title=f"Search results from {source} ({len(results)} datasets)")
    table.add_column("#", style="dim", width=4)
    table.add_column("Title", max_width=60)
    table.add_column("Authors", max_width=30)
    table.add_column("Published", width=12)

    for i, r in enumerate(results, 1):
        table.add_row(
            str(i),
            r.title[:60],
            r.authors[:30] if r.authors else "",
            r.date_published[:10] if r.date_published else "",
        )

    console.print(table)


@cli.command()
@click.argument("source")
@click.option("--limit", "-n", default=None, type=int, help="Max datasets to scrape.")
@click.option("--query", "-q", default="qualitative", help="Search query string.")
def scrape(source: str, limit: int | None, query: str) -> None:
    """Scrape and download data from a source."""
    connector = _get_connector(source)

    console.print(f"[bold]Scraping {source}[/bold] query='{query}' limit={limit}")

    # Step 1: Search for datasets
    try:
        results = connector.search(query)
    except Exception as e:
        console.print(f"[red]Search failed: {e}[/red]")
        raise SystemExit(1) from e

    if limit:
        results = results[:limit]

    console.print(f"Found {len(results)} datasets to process.")

    session = get_session()
    downloaded_count = 0
    skipped_count = 0
    restricted_count = 0

    try:
        for i, result in enumerate(results, 1):
            console.print(f"\n[bold][{i}/{len(results)}][/bold] {result.title[:70]}")

            # Step 2: Get full metadata
            try:
                metadata = connector.get_metadata(result.source_url)
            except Exception as e:
                console.print(f"  [red]Metadata fetch failed: {e}[/red]")
                continue

            # Step 3: Check license
            if not is_open_license(metadata.license_type):
                console.print(
                    f"  [yellow]Skipping — license not open: "
                    f"'{metadata.license_type or 'none'}'[/yellow]"
                )
                skipped_count += 1
                continue

            if not metadata.files:
                console.print("  [yellow]No files in this dataset.[/yellow]")
                continue

            # Step 4: Download each file
            for finfo in metadata.files:
                fname = finfo["name"]
                download_url = finfo["download_url"]
                file_ext = Path(fname).suffix.lower()

                # Determine if it's a QDA file
                is_qda = file_ext in QDA_EXTENSIONS

                # Build storage path
                if "persistentId=" in result.source_url:
                    record_id = result.source_url.split("persistentId=")[-1]
                else:
                    record_id = str(finfo["id"])
                # Sanitize record_id for use as directory name
                record_id = record_id.replace("/", "_").replace(":", "_")
                dest_dir = str(get_storage_path(source, record_id, "").parent)

                try:
                    local_path = connector.download(download_url, dest_dir)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 403:
                        # Restricted file — save metadata-only record
                        _save_metadata_only(
                            session, source, result, metadata, finfo,
                            fname, file_ext, is_qda,
                        )
                        restricted_count += 1
                        label = "[green]QDA[/green]" if is_qda else "[dim]file[/dim]"
                        console.print(
                            f"  {label} {fname} [yellow](restricted — metadata saved)[/yellow]"
                        )
                        continue
                    console.print(f"  [red]Download failed for {fname}: {e}[/red]")
                    continue
                except Exception as e:
                    console.print(f"  [red]Download failed for {fname}: {e}[/red]")
                    continue

                # Compute hash
                file_hash = compute_sha256(Path(local_path))

                # Check for duplicate by hash
                existing = session.query(File).filter_by(file_hash=file_hash).first()
                if existing:
                    console.print(f"  [dim]Duplicate (hash match): {fname}[/dim]")
                    Path(local_path).unlink(missing_ok=True)
                    continue

                # Save to DB
                file_record = File(
                    source_name=source,
                    source_url=result.source_url,
                    download_url=download_url,
                    file_name=fname,
                    file_type=file_ext,
                    file_hash=file_hash,
                    file_size_bytes=finfo.get("size"),
                    local_path=local_path,
                    license_type=metadata.license_type,
                    license_url=metadata.license_url,
                    title=metadata.title,
                    description=metadata.description,
                    authors=metadata.authors,
                    date_published=metadata.date_published,
                    tags="; ".join(metadata.tags) if metadata.tags else None,
                    is_qda_file=is_qda,
                    downloaded_at=datetime.utcnow(),
                )
                session.add(file_record)
                session.commit()
                downloaded_count += 1

                label = "[green]QDA[/green]" if is_qda else "[blue]file[/blue]"
                console.print(f"  {label} {fname} ({finfo.get('size', '?')} bytes)")

    finally:
        session.close()

    console.print(
        f"\n[bold]Done.[/bold] Downloaded: {downloaded_count}, "
        f"Restricted (metadata only): {restricted_count}, "
        f"Skipped (license): {skipped_count}"
    )


@cli.command("export")
@click.option("--format", "fmt", default="csv", type=click.Choice(["csv"]), help="Export format.")
@click.option("--output", "-o", default=None, help="Output file path.")
def export_cmd(fmt: str, output: str | None) -> None:
    """Export the metadata database."""
    if output is None:
        output = str(EXPORTS_DIR / f"metadata.{fmt}")

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

        # Per-source counts
        from sqlalchemy import func

        source_counts = (
            session.query(File.source_name, func.count(File.id))
            .group_by(File.source_name)
            .all()
        )

        console.print(f"[bold]Total records:[/bold]    {total}")
        console.print(f"[bold]QDA files:[/bold]        {qda}")
        console.print(f"[bold]Downloaded files:[/bold] {downloaded}")

        if source_counts:
            console.print("\n[bold]By source:[/bold]")
            for src, count in source_counts:
                console.print(f"  {src:<15} {count}")
    finally:
        session.close()


@cli.command("db")
@click.option("--source", "-s", default=None, help="Filter by source name.")
@click.option("--qda-only", is_flag=True, help="Show only QDA files.")
@click.option("--restricted-only", is_flag=True, help="Show only restricted files.")
@click.option("--limit", "-n", default=50, type=int, help="Max rows to display.")
def db_view(source: str | None, qda_only: bool, restricted_only: bool, limit: int) -> None:
    """Browse the metadata database."""
    session = get_session()
    try:
        query = session.query(File)
        if source:
            query = query.filter(File.source_name == source)
        if qda_only:
            query = query.filter(File.is_qda_file.is_(True))
        if restricted_only:
            query = query.filter(File.local_path.is_(None))

        total = query.count()
        records = query.order_by(File.id).limit(limit).all()

        if not records:
            console.print("[yellow]No records found.[/yellow]")
            return

        table = Table(title=f"Database records ({total} total, showing {len(records)})")
        table.add_column("ID", style="dim", width=5)
        table.add_column("File", max_width=40)
        table.add_column("Type", width=6)
        table.add_column("Source", width=8)
        table.add_column("QDA", width=4)
        table.add_column("Status", width=12)
        table.add_column("Size", width=10, justify="right")

        for r in records:
            if r.local_path:
                status = "[green]downloaded[/green]"
            elif r.notes and "restricted" in r.notes:
                status = "[yellow]restricted[/yellow]"
            else:
                status = "[dim]metadata[/dim]"

            size = _format_size(r.file_size_bytes) if r.file_size_bytes else ""
            qda_label = "[green]yes[/green]" if r.is_qda_file else ""

            table.add_row(
                str(r.id),
                r.file_name[:40],
                r.file_type or "",
                r.source_name,
                qda_label,
                status,
                size,
            )

        console.print(table)

        if total > limit:
            console.print(f"[dim]Showing {limit} of {total} — use --limit to see more[/dim]")
    finally:
        session.close()


def _format_size(size_bytes: int) -> str:
    """Format file size in human-readable form."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


@cli.command("list-sources")
def list_sources() -> None:
    """List available data source connectors."""
    console.print("[bold]Available sources:[/bold]\n")
    for name, connector in CONNECTORS.items():
        console.print(f"  {name:<15} {connector.name:<45} [green]ready[/green]")

    planned = [
        ("zenodo", "Zenodo API"),
        ("dryad", "Dryad API"),
        ("dans", "DANS Dataverse"),
        ("dataverseno", "DataverseNO"),
        ("ukds", "UK Data Service (scraper)"),
        ("qualidata", "QualidataNet (scraper)"),
        ("qualiservice", "Qualiservice (scraper)"),
    ]
    for name, desc in planned:
        if name not in CONNECTORS:
            console.print(f"  {name:<15} {desc:<45} [yellow]planned[/yellow]")


if __name__ == "__main__":
    cli()
