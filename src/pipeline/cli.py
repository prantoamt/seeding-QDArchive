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


def _save_metadata_only(
    session, source, result, metadata, finfo, fname, file_ext, is_qda, dir_name=None,
):
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
        local_directory=dir_name,
        license_type=metadata.license_type,
        license_url=metadata.license_url,
        title=metadata.title,
        description=metadata.description,
        authors=metadata.authors,
        date_published=metadata.date_published,
        tags="; ".join(metadata.tags) if metadata.tags else None,
        keywords="; ".join(metadata.keywords) if metadata.keywords else None,
        kind_of_data="; ".join(metadata.kind_of_data) if metadata.kind_of_data else None,
        language="; ".join(metadata.language) if metadata.language else None,
        software="; ".join(metadata.software) if metadata.software else None,
        geographic_coverage=(
            "; ".join(metadata.geographic_coverage) if metadata.geographic_coverage else None
        ),
        content_type=finfo.get("content_type"),
        friendly_type=finfo.get("friendly_type"),
        restricted=finfo.get("restricted", False),
        api_checksum=finfo.get("api_checksum"),
        uploader_name=metadata.uploader_name or None,
        uploader_email=metadata.uploader_email or None,
        is_qda_file=is_qda,
        notes="access restricted",
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


def _scrape_results(connector, source, results, session):
    """Process a list of search results: fetch metadata, check license, download files.

    Returns (downloaded_count, restricted_count, skipped_count).
    """
    downloaded_count = 0
    skipped_count = 0
    restricted_count = 0

    for i, result in enumerate(results, 1):
        console.print(f"\n[bold][{i}/{len(results)}][/bold] {result.title[:70]}")

        # Get full metadata
        try:
            metadata = connector.get_metadata(result.source_url)
        except Exception as e:
            console.print(f"  [red]Metadata fetch failed: {e}[/red]")
            continue

        # Check license
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

        # Download each file
        for finfo in metadata.files:
            fname = finfo["name"]
            download_url = finfo["download_url"]
            file_ext = Path(fname).suffix.lower()
            friendly = finfo.get("friendly_type", "")
            ctype = finfo.get("content_type", "")
            is_qda = (
                file_ext in QDA_EXTENSIONS
                or "refi-qda" in friendly.lower()
                or "refiqda" in ctype.lower()
            )

            # Build storage path
            if "persistentId=" in result.source_url:
                record_id = result.source_url.split("persistentId=")[-1]
            else:
                record_id = str(finfo["id"])
            record_id = record_id.replace("/", "_").replace(":", "_")
            storage_path = get_storage_path(source, record_id, fname, title=metadata.title)
            dest_dir = str(storage_path.parent)
            dir_name = storage_path.parent.name

            # Skip if already in DB (by download_url)
            already = (
                session.query(File)
                .filter_by(source_name=source, download_url=download_url)
                .first()
            )
            if already:
                console.print(f"  [dim]Already cataloged: {fname}[/dim]")
                continue

            # Skip download for known-restricted files
            if finfo.get("restricted", False):
                _save_metadata_only(
                    session, source, result, metadata, finfo,
                    fname, file_ext, is_qda, dir_name=dir_name,
                )
                restricted_count += 1
                label = "[green]QDA[/green]" if is_qda else "[dim]file[/dim]"
                console.print(
                    f"  {label} {fname} "
                    f"[yellow](restricted — metadata saved)[/yellow]"
                )
                continue

            try:
                local_path = connector.download(
                    download_url, dest_dir, filename=fname
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    _save_metadata_only(
                        session, source, result, metadata, finfo,
                        fname, file_ext, is_qda, dir_name=dir_name,
                    )
                    restricted_count += 1
                    label = "[green]QDA[/green]" if is_qda else "[dim]file[/dim]"
                    console.print(
                        f"  {label} {fname} "
                        f"[yellow](restricted — metadata saved)[/yellow]"
                    )
                    continue
                console.print(f"  [red]Download failed for {fname}: {e}[/red]")
                continue
            except Exception as e:
                console.print(f"  [red]Download failed for {fname}: {e}[/red]")
                continue

            file_hash = compute_sha256(Path(local_path))

            existing = session.query(File).filter_by(file_hash=file_hash).first()
            if existing:
                console.print(f"  [dim]Duplicate (hash match): {fname}[/dim]")
                Path(local_path).unlink(missing_ok=True)
                continue

            file_record = File(
                source_name=source,
                source_url=result.source_url,
                download_url=download_url,
                file_name=fname,
                file_type=file_ext,
                file_hash=file_hash,
                file_size_bytes=finfo.get("size"),
                local_path=local_path,
                local_directory=dir_name,
                license_type=metadata.license_type,
                license_url=metadata.license_url,
                title=metadata.title,
                description=metadata.description,
                authors=metadata.authors,
                date_published=metadata.date_published,
                tags="; ".join(metadata.tags) if metadata.tags else None,
                keywords="; ".join(metadata.keywords) if metadata.keywords else None,
                kind_of_data=(
                    "; ".join(metadata.kind_of_data) if metadata.kind_of_data else None
                ),
                language="; ".join(metadata.language) if metadata.language else None,
                software="; ".join(metadata.software) if metadata.software else None,
                geographic_coverage=(
                    "; ".join(metadata.geographic_coverage)
                    if metadata.geographic_coverage
                    else None
                ),
                content_type=finfo.get("content_type"),
                friendly_type=finfo.get("friendly_type"),
                restricted=finfo.get("restricted", False),
                api_checksum=finfo.get("api_checksum"),
                uploader_name=metadata.uploader_name or None,
                uploader_email=metadata.uploader_email or None,
                is_qda_file=is_qda,
                downloaded_at=datetime.utcnow(),
            )
            session.add(file_record)
            session.commit()
            downloaded_count += 1

            label = "[green]QDA[/green]" if is_qda else "[blue]file[/blue]"
            console.print(f"  {label} {fname} ({finfo.get('size', '?')} bytes)")

    return downloaded_count, restricted_count, skipped_count


@cli.command()
@click.argument("source")
@click.option("--limit", "-n", default=None, type=int, help="Max datasets per query.")
@click.option("--query", "-q", default=None, help="Search query string.")
@click.option(
    "--queries-file", "-f", default=None,
    type=click.Path(exists=True),
    help="Text file with one search query per line.",
)
def scrape(
    source: str, limit: int | None, query: str | None, queries_file: str | None
) -> None:
    """Scrape and download data from a source."""
    connector = _get_connector(source)

    # Build list of queries
    if queries_file:
        queries = [
            line.strip() for line in Path(queries_file).read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
    elif query:
        queries = [query]
    else:
        queries = ["qualitative"]

    session = get_session()
    total_downloaded = 0
    total_restricted = 0
    total_skipped = 0
    seen_urls: set[str] = set()

    try:
        for qi, q in enumerate(queries, 1):
            console.print(
                f"\n[bold]=== Query {qi}/{len(queries)}: '{q}' ===[/bold]"
            )

            try:
                results = connector.search(q)
            except Exception as e:
                console.print(f"[red]Search failed: {e}[/red]")
                continue

            # Deduplicate across queries by source_url
            results = [r for r in results if r.source_url not in seen_urls]
            seen_urls.update(r.source_url for r in results)

            if limit:
                results = results[:limit]

            console.print(f"Found {len(results)} new datasets to process.")

            if not results:
                continue

            dl, rest, skip = _scrape_results(connector, source, results, session)
            total_downloaded += dl
            total_restricted += rest
            total_skipped += skip

    finally:
        session.close()

    console.print(
        f"\n[bold]All done.[/bold] Queries: {len(queries)}, "
        f"Downloaded: {total_downloaded}, "
        f"Restricted (metadata only): {total_restricted}, "
        f"Skipped (license): {total_skipped}"
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
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
def reset(yes: bool) -> None:
    """Delete database, downloaded data, exports, and logs — full clean slate."""
    import shutil

    from pipeline.config import DATA_DIR, DB_PATH, EXPORTS_DIR, LOG_FILE

    if not yes:
        msg = "This will delete the database, all downloaded data, exports, and logs. Continue?"
        if not click.confirm(msg):
            console.print("[dim]Aborted.[/dim]")
            return

    removed = []

    if DB_PATH.exists():
        DB_PATH.unlink()
        removed.append(f"Database: {DB_PATH}")

    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
        removed.append(f"Data: {DATA_DIR}")

    if EXPORTS_DIR.exists():
        shutil.rmtree(EXPORTS_DIR)
        removed.append(f"Exports: {EXPORTS_DIR}")

    if LOG_FILE.exists():
        LOG_FILE.unlink()
        removed.append(f"Log: {LOG_FILE}")

    # Re-create directories and DB
    ensure_dirs()
    init_db()

    if removed:
        for r in removed:
            console.print(f"  Deleted {r}")
    else:
        console.print("  Nothing to clean.")

    console.print("[bold]Reset complete.[/bold]")


@cli.command()
def status() -> None:
    """Show pipeline status and record counts."""
    session = get_session()
    try:
        total = session.query(File).count()
        qda = session.query(File).filter(File.is_qda_file.is_(True)).count()
        downloaded = session.query(File).filter(File.local_path.isnot(None)).count()

        from sqlalchemy import case, func

        restricted = session.query(File).filter(File.restricted.is_(True)).count()

        console.print(f"[bold]Total records:[/bold]    {total}")
        console.print(f"[bold]QDA files:[/bold]        {qda}")
        console.print(f"[bold]Downloaded files:[/bold] {downloaded}")
        console.print(f"[bold]Restricted:[/bold]      {restricted}")

        # Reusable aggregation columns
        col_total = func.count(File.id).label("total")
        col_qda = func.sum(case((File.is_qda_file.is_(True), 1), else_=0)).label(
            "qda"
        )
        col_dl = func.sum(
            case((File.local_path.isnot(None), 1), else_=0)
        ).label("downloaded")
        col_restricted = func.sum(
            case((File.restricted.is_(True), 1), else_=0)
        ).label("restricted")

        def _print_breakdown(title: str, rows: list, name_width: int = 30) -> None:
            if not rows:
                return
            console.print(f"\n[bold]{title}[/bold]")
            header = f"  {'':>{name_width}}  {'Total':>7}  {'QDA':>5}  {'Down':>7}  {'Restr':>7}"
            console.print(f"[dim]{header}[/dim]")
            for name, t, q, d, r in rows:
                console.print(
                    f"  {name:>{name_width}}  {t:>7}  {q:>5}  {d:>7}  {r:>7}"
                )

        # Per-source breakdown
        source_rows = (
            session.query(
                File.source_name, col_total, col_qda, col_dl, col_restricted
            )
            .group_by(File.source_name)
            .order_by(col_total.desc())
            .all()
        )
        _print_breakdown("By source:", source_rows, name_width=20)

        # Language breakdown (top 10)
        lang_rows = (
            session.query(File.language, col_total, col_qda, col_dl, col_restricted)
            .filter(File.language.isnot(None))
            .group_by(File.language)
            .order_by(col_total.desc())
            .limit(10)
            .all()
        )
        _print_breakdown("By language:", lang_rows, name_width=35)

        # Software breakdown
        sw_rows = (
            session.query(File.software, col_total, col_qda, col_dl, col_restricted)
            .filter(File.software.isnot(None))
            .group_by(File.software)
            .order_by(col_total.desc())
            .all()
        )
        _print_breakdown("By software:", sw_rows, name_width=35)

        # File type breakdown
        ft_rows = (
            session.query(
                File.file_type, col_total, col_qda, col_dl, col_restricted
            )
            .filter(File.file_type.isnot(None))
            .group_by(File.file_type)
            .order_by(col_total.desc())
            .all()
        )
        _print_breakdown("By file type:", ft_rows, name_width=20)

        # License type breakdown
        lic_rows = (
            session.query(
                File.license_type, col_total, col_qda, col_dl, col_restricted
            )
            .filter(File.license_type.isnot(None))
            .group_by(File.license_type)
            .order_by(col_total.desc())
            .all()
        )
        _print_breakdown("By license:", lic_rows, name_width=35)
    finally:
        session.close()


@cli.command("db")
@click.option("--source", "-s", default=None, help="Filter by source name.")
@click.option("--qda-only", is_flag=True, help="Show only QDA files.")
@click.option("--restricted-only", is_flag=True, help="Show only restricted files.")
@click.option("--search", default=None, help="Search title, description, keywords, tags.")
@click.option("--language", default=None, help="Filter by language (substring match).")
@click.option("--software", default=None, help="Filter by software (substring match).")
@click.option("--file-type", "file_type", default=None, help="Filter by file type (e.g. .pdf).")
@click.option("--has-software", is_flag=True, help="Show only records with software info.")
@click.option("--has-keywords", is_flag=True, help="Show only records with keywords.")
@click.option("--limit", "-n", default=50, type=int, help="Max rows to display.")
def db_view(
    source: str | None,
    qda_only: bool,
    restricted_only: bool,
    search: str | None,
    language: str | None,
    software: str | None,
    file_type: str | None,
    has_software: bool,
    has_keywords: bool,
    limit: int,
) -> None:
    """Browse the metadata database."""
    session = get_session()
    try:
        from sqlalchemy import or_

        query = session.query(File)
        if source:
            query = query.filter(File.source_name == source)
        if qda_only:
            query = query.filter(File.is_qda_file.is_(True))
        if restricted_only:
            query = query.filter(File.restricted.is_(True))
        if search:
            pattern = f"%{search}%"
            query = query.filter(or_(
                File.title.ilike(pattern),
                File.description.ilike(pattern),
                File.keywords.ilike(pattern),
                File.tags.ilike(pattern),
            ))
        if language:
            query = query.filter(File.language.ilike(f"%{language}%"))
        if software:
            query = query.filter(File.software.ilike(f"%{software}%"))
        if file_type:
            ft = file_type if file_type.startswith(".") else f".{file_type}"
            query = query.filter(File.file_type == ft)
        if has_software:
            query = query.filter(File.software.isnot(None))
        if has_keywords:
            query = query.filter(File.keywords.isnot(None))

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


@cli.command("show")
@click.argument("ids", nargs=-1, required=True, type=int)
def db_show(ids: tuple[int, ...]) -> None:
    """Show full details for one or more records by ID."""
    session = get_session()
    try:
        for record_id in ids:
            r = session.query(File).filter_by(id=record_id).first()
            if not r:
                console.print(f"[red]Record {record_id} not found.[/red]")
                continue

            from rich.panel import Panel

            if r.local_path:
                status = "downloaded"
            elif r.notes and "restricted" in r.notes:
                status = "restricted"
            else:
                status = "metadata only"

            size = _format_size(r.file_size_bytes) if r.file_size_bytes else "unknown"

            lines = [
                f"[bold]File:[/bold]        {r.file_name}",
                f"[bold]Type:[/bold]        {r.file_type or 'unknown'}",
                f"[bold]Size:[/bold]        {size}",
                f"[bold]QDA file:[/bold]    {'yes' if r.is_qda_file else 'no'}",
                f"[bold]Status:[/bold]      {status}",
                f"[bold]Restricted:[/bold]  {'yes' if r.restricted else 'no'}",
                "",
                f"[bold]Title:[/bold]       {r.title or '—'}",
                f"[bold]Authors:[/bold]     {r.authors or '—'}",
                f"[bold]Uploader:[/bold]    {r.uploader_name or '—'}",
                f"[bold]Uploader email:[/bold] {r.uploader_email or '—'}",
                f"[bold]Published:[/bold]   {r.date_published or '—'}",
                f"[bold]Tags:[/bold]        {r.tags or '—'}",
                f"[bold]Keywords:[/bold]    {r.keywords or '—'}",
                f"[bold]Kind of data:[/bold] {r.kind_of_data or '—'}",
                f"[bold]Language:[/bold]    {r.language or '—'}",
                f"[bold]Software:[/bold]    {r.software or '—'}",
                f"[bold]Geography:[/bold]   {r.geographic_coverage or '—'}",
                "",
                f"[bold]Source:[/bold]      {r.source_name}",
                f"[bold]Source URL:[/bold]  {r.source_url or '—'}",
                f"[bold]Download URL:[/bold] {r.download_url or '—'}",
                f"[bold]License:[/bold]     {r.license_type or '—'}",
                f"[bold]License URL:[/bold] {r.license_url or '—'}",
                "",
                f"[bold]Content type:[/bold] {r.content_type or '—'}",
                f"[bold]Friendly type:[/bold] {r.friendly_type or '—'}",
                f"[bold]Local dir:[/bold]   {r.local_directory or '—'}",
                f"[bold]Local path:[/bold]  {r.local_path or '—'}",
                f"[bold]File hash:[/bold]   {r.file_hash or '—'}",
                f"[bold]API checksum:[/bold] {r.api_checksum or '—'}",
                f"[bold]Downloaded:[/bold]  {r.downloaded_at or '—'}",
                f"[bold]Created:[/bold]     {r.created_at}",
                f"[bold]Notes:[/bold]       {r.notes or '—'}",
            ]

            desc = r.description or ""
            if desc:
                # Truncate long descriptions
                if len(desc) > 300:
                    desc = desc[:300] + "..."
                lines.insert(7, f"[bold]Description:[/bold] {desc}")

            console.print(Panel(
                "\n".join(lines),
                title=f"Record #{r.id}",
                expand=False,
            ))

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
        ("ukds", "UK Data Service (scraper)"),
        ("qualidata", "QualidataNet (scraper)"),
        ("qualiservice", "Qualiservice (scraper)"),
    ]
    for name, desc in planned:
        if name not in CONNECTORS:
            console.print(f"  {name:<15} {desc:<45} [yellow]planned[/yellow]")


if __name__ == "__main__":
    cli()
