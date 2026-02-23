"""CLI entry point for the pipeline."""

import logging
from datetime import datetime
from pathlib import Path

import click
import httpx
from rich.console import Console
from rich.table import Table

from pipeline.config import (
    EXPORTS_DIR,
    PROJECT_ROOT,
    QDA_EXTENSIONS,
    QUALITATIVE_EXTENSIONS,
    QUALITATIVE_KEYWORDS,
    SKIP_KIND_OF_DATA,
    SOURCE_DIR_NAMES,
    ensure_dirs,
)
from pipeline.connectors import CONNECTORS
from pipeline.db.connection import get_session, init_db
from pipeline.db.export import export_to_csv
from pipeline.db.models import File
from pipeline.storage.file_manager import compute_sha256, get_storage_path
from pipeline.utils.license import is_open_license, normalize_license
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
    session, source, result, metadata, finfo, fname, file_ext, is_qda,
    dir_name=None, notes="access restricted",
):
    """Save a metadata-only DB record for a file we couldn't download."""
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
        license_type=normalize_license(metadata.license_type),
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
        depositor=metadata.depositor or None,
        producer="; ".join(metadata.producer) if metadata.producer else None,
        publication="; ".join(metadata.publication) if metadata.publication else None,
        date_of_collection=metadata.date_of_collection or None,
        time_period_covered=metadata.time_period_covered or None,
        uploader_name=metadata.uploader_name or None,
        uploader_email=metadata.uploader_email or None,
        is_qda_file=is_qda,
        notes=notes,
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

        # Skip non-data resource types (publications, presentations, etc.)
        # unless the record contains a QDA file
        if metadata.kind_of_data:
            kod_values = {v.strip().lower() for v in metadata.kind_of_data}
            if kod_values & SKIP_KIND_OF_DATA:
                # Check for QDA files before skipping
                has_qda_in_skip = any(
                    Path(f["name"]).suffix.lower() in QDA_EXTENSIONS
                    or "refi-qda" in f.get("friendly_type", "").lower()
                    or "refiqda" in f.get("content_type", "").lower()
                    for f in metadata.files
                )
                if not has_qda_in_skip:
                    kod_str = "; ".join(metadata.kind_of_data)
                    console.print(
                        f"  [dim]Skipping — resource type not data: "
                        f"'{kod_str}'[/dim]"
                    )
                    skipped_count += 1
                    continue

        # Always keep datasets that contain QDA files, regardless of description
        has_qda_file = any(
            Path(f["name"]).suffix.lower() in QDA_EXTENSIONS
            or "refi-qda" in f.get("friendly_type", "").lower()
            or "refiqda" in f.get("content_type", "").lower()
            for f in metadata.files
        )

        # Skip datasets whose description AND keywords lack qualitative signal
        if not has_qda_file:
            text_to_check = (metadata.description or "").lower()
            # Also check keywords/tags for qualitative relevance
            if metadata.keywords:
                text_to_check += " " + " ".join(
                    kw.lower() for kw in metadata.keywords
                )
            if not any(kw in text_to_check for kw in QUALITATIVE_KEYWORDS):
                console.print("  [dim]Skipping — description has no qualitative relevance[/dim]")
                skipped_count += 1
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

            # Only download QDA files and qualitative data formats;
            # save everything else as metadata-only
            if not is_qda and file_ext not in QUALITATIVE_EXTENSIONS:
                _save_metadata_only(
                    session, source, result, metadata, finfo,
                    fname, file_ext, is_qda, dir_name=None,
                    notes="irrelevant file type",
                )
                console.print(
                    f"  [dim]{fname} ({file_ext}) — metadata only "
                    f"(not qualitative)[/dim]"
                )
                continue

            # Build storage path
            if "persistentId=" in result.source_url:
                record_id = result.source_url.split("persistentId=")[-1]
            else:
                record_id = str(finfo["id"])
            record_id = record_id.replace("/", "_").replace(":", "_")
            dir_label = SOURCE_DIR_NAMES.get(source, source)
            storage_path = get_storage_path(dir_label, record_id, fname, title=metadata.title)
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
                    finfo_restricted = {**finfo, "restricted": True}
                    _save_metadata_only(
                        session, source, result, metadata, finfo_restricted,
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
                local_path=str(Path(local_path).relative_to(PROJECT_ROOT)),
                local_directory=dir_name,
                license_type=normalize_license(metadata.license_type),
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
                depositor=metadata.depositor or None,
                producer="; ".join(metadata.producer) if metadata.producer else None,
                publication="; ".join(metadata.publication) if metadata.publication else None,
                date_of_collection=metadata.date_of_collection or None,
                time_period_covered=metadata.time_period_covered or None,
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


def _load_queries(
    queries_file: str | None, query: str | None,
) -> list[str]:
    """Build a list of search queries from a file, a single string, or the default."""
    if queries_file:
        return [
            line.strip() for line in Path(queries_file).read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
    if query:
        return [query]
    return ["qualitative"]


def _scrape_source(
    connector, source: str, queries: list[str], limit: int | None,
) -> tuple[int, int, int]:
    """Run all queries against a single source. Returns (downloaded, restricted, skipped)."""
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

    return total_downloaded, total_restricted, total_skipped


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
    queries = _load_queries(queries_file, query)

    dl, rest, skip = _scrape_source(connector, source, queries, limit)

    console.print(
        f"\n[bold]All done.[/bold] Queries: {len(queries)}, "
        f"Downloaded: {dl}, "
        f"Restricted (metadata only): {rest}, "
        f"Skipped (license): {skip}"
    )


@cli.command("scrape-all")
@click.option(
    "--queries-file", "-f", default=None,
    type=click.Path(exists=True),
    help="Text file with one search query per line (default: queries.txt).",
)
@click.option("--limit", "-n", default=None, type=int, help="Max datasets per query per source.")
@click.option("--retries", "-r", default=1, type=int, help="Retries for fully-failed sources.")
def scrape_all(
    queries_file: str | None, limit: int | None, retries: int,
) -> None:
    """Scrape all sources sequentially with per-source error handling."""
    # Default to queries.txt in project root if it exists
    if queries_file is None:
        default_qf = PROJECT_ROOT / "queries.txt"
        if default_qf.exists():
            queries_file = str(default_qf)
    queries = _load_queries(queries_file, None)
    console.print(
        f"[bold]Scraping all {len(CONNECTORS)} sources "
        f"with {len(queries)} queries (limit={limit or 'none'}, retries={retries})[/bold]\n"
    )

    # Track per-source results: {source: {status, downloaded, restricted, skipped, error}}
    source_results: dict[str, dict] = {}
    failed_sources: list[str] = list()

    for source, connector in CONNECTORS.items():
        console.print(f"\n[bold cyan]>>> Source: {source}[/bold cyan]")
        try:
            dl, rest, skip = _scrape_source(connector, source, queries, limit)
            source_results[source] = {
                "status": "OK",
                "downloaded": dl,
                "restricted": rest,
                "skipped": skip,
                "error": None,
            }
        except Exception as e:
            logger.exception("Source %s failed", source)
            console.print(f"[red]Source {source} failed: {e}[/red]")
            source_results[source] = {
                "status": "FAILED",
                "downloaded": 0,
                "restricted": 0,
                "skipped": 0,
                "error": str(e),
            }
            failed_sources.append(source)

    # Retry failed sources
    for attempt in range(1, retries + 1):
        if not failed_sources:
            break
        console.print(
            f"\n[bold yellow]Retrying {len(failed_sources)} failed source(s) "
            f"(attempt {attempt}/{retries})[/bold yellow]"
        )
        still_failed: list[str] = []
        for source in failed_sources:
            connector = CONNECTORS[source]
            console.print(f"\n[bold cyan]>>> Retry: {source}[/bold cyan]")
            try:
                dl, rest, skip = _scrape_source(connector, source, queries, limit)
                source_results[source] = {
                    "status": "OK",
                    "downloaded": dl,
                    "restricted": rest,
                    "skipped": skip,
                    "error": None,
                }
            except Exception as e:
                logger.exception("Source %s retry %d failed", source, attempt)
                console.print(f"[red]Source {source} retry failed: {e}[/red]")
                source_results[source]["error"] = str(e)
                still_failed.append(source)
        failed_sources = still_failed

    _print_scrape_all_summary(source_results)


def _print_scrape_all_summary(source_results: dict[str, dict]) -> None:
    """Print a Rich summary table of scrape-all results."""
    table = Table(title="Scrape-all Summary")
    table.add_column("Source", style="bold", width=14)
    table.add_column("Status", width=8)
    table.add_column("Downloaded", justify="right", width=11)
    table.add_column("Restricted", justify="right", width=11)
    table.add_column("Skipped", justify="right", width=8)
    table.add_column("Error", max_width=40)

    total_dl = total_rest = total_skip = 0
    ok_count = fail_count = 0

    for source, info in source_results.items():
        status_style = "[green]OK[/green]" if info["status"] == "OK" else "[red]FAILED[/red]"
        table.add_row(
            source,
            status_style,
            str(info["downloaded"]),
            str(info["restricted"]),
            str(info["skipped"]),
            info["error"] or "",
        )
        total_dl += info["downloaded"]
        total_rest += info["restricted"]
        total_skip += info["skipped"]
        if info["status"] == "OK":
            ok_count += 1
        else:
            fail_count += 1

    console.print()
    console.print(table)
    console.print(
        f"\n[bold]Totals:[/bold] {ok_count} succeeded, {fail_count} failed | "
        f"Downloaded: {total_dl}, Restricted: {total_rest}, Skipped: {total_skip}"
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

    if DATA_DIR.is_symlink():
        # Symlink (e.g. NAS mount) — clear contents but keep the link
        for child in DATA_DIR.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        removed.append(f"Data (contents): {DATA_DIR}")
    elif DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
        removed.append(f"Data: {DATA_DIR}")

    if EXPORTS_DIR.is_symlink():
        for child in EXPORTS_DIR.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        removed.append(f"Exports (contents): {EXPORTS_DIR}")
    elif EXPORTS_DIR.exists():
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

        metadata_only = total - downloaded - restricted

        console.print(f"[bold]Total records:[/bold]    {total}")
        console.print(f"[bold]QDA files:[/bold]        {qda}")
        console.print()
        console.print(f"  [green]Downloaded:[/green]     {downloaded}")
        console.print(f"  [yellow]Restricted:[/yellow]     {restricted}  (metadata only)")
        console.print(f"  [dim]Other:[/dim]          {metadata_only}  (metadata only)")

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
                f"[bold]Depositor:[/bold]   {r.depositor or '—'}",
                f"[bold]Producer:[/bold]    {r.producer or '—'}",
                f"[bold]Publication:[/bold] {r.publication or '—'}",
                f"[bold]Collection:[/bold]  {r.date_of_collection or '—'}",
                f"[bold]Time period:[/bold] {r.time_period_covered or '—'}",
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


@cli.command()
def stats() -> None:
    """Comprehensive data analysis — reproduces all report figures."""
    from sqlalchemy import case, distinct, func

    session = get_session()
    try:
        # ── 1. Executive Summary ──────────────────────────────────────
        total = session.query(File).count()
        downloaded = session.query(File).filter(File.local_path.isnot(None)).count()
        qda_total = session.query(File).filter(File.is_qda_file.is_(True)).count()
        qda_downloaded = (
            session.query(File)
            .filter(File.is_qda_file.is_(True), File.local_path.isnot(None))
            .count()
        )
        restricted = session.query(File).filter(File.restricted.is_(True)).count()
        unique_datasets = session.query(
            func.count(distinct(File.source_url))
        ).scalar()
        total_size = (
            session.query(func.sum(File.file_size_bytes))
            .filter(File.local_path.isnot(None))
            .scalar()
        ) or 0
        # Duplicate count: files sharing a hash with at least one other file
        dup_hashes = (
            session.query(File.file_hash)
            .filter(File.file_hash.isnot(None))
            .group_by(File.file_hash)
            .having(func.count(File.id) > 1)
            .count()
        )
        # Count QDA formats
        qda_formats = (
            session.query(func.count(distinct(File.file_type)))
            .filter(File.is_qda_file.is_(True))
            .scalar()
        ) or 0

        size_gb = total_size / (1024 ** 3)

        console.print("\n[bold cyan]═══ Comprehensive Data Analysis ═══[/bold cyan]\n")

        summary = Table(title="Executive Summary", show_header=False, pad_edge=False)
        summary.add_column("Metric", style="bold", width=30)
        summary.add_column("Value", justify="right", width=20)
        summary.add_row("Total metadata records", f"{total:,}")
        summary.add_row("Files downloaded", f"{downloaded:,} ({size_gb:.2f} GB)")
        summary.add_row(
            "QDA files found",
            f"{qda_total} (across {qda_formats} formats)",
        )
        summary.add_row("QDA files downloaded", str(qda_downloaded))
        summary.add_row(
            "QDA files restricted",
            str(qda_total - qda_downloaded),
        )
        summary.add_row("Restricted (metadata only)", f"{restricted:,}")
        summary.add_row("Unique datasets", f"{unique_datasets:,}")
        summary.add_row("Duplicate files (by SHA-256)", str(dup_hashes))
        console.print(summary)

        # ── 2. Per-Source Breakdown ───────────────────────────────────
        col_total = func.count(File.id)
        col_dl = func.sum(case((File.local_path.isnot(None), 1), else_=0))
        col_qda = func.sum(case((File.is_qda_file.is_(True), 1), else_=0))
        col_restricted = func.sum(case((File.restricted.is_(True), 1), else_=0))
        col_size = func.sum(
            case((File.local_path.isnot(None), File.file_size_bytes), else_=0)
        )
        col_datasets = func.count(distinct(File.source_url))

        source_rows = (
            session.query(
                File.source_name,
                col_total.label("total"),
                col_dl.label("downloaded"),
                col_qda.label("qda"),
                col_restricted.label("restricted"),
                col_size.label("size"),
                col_datasets.label("datasets"),
            )
            .group_by(File.source_name)
            .order_by(col_total.desc())
            .all()
        )

        console.print()
        src_table = Table(title="Per-Source Breakdown")
        src_table.add_column("Source", style="bold", width=16)
        src_table.add_column("Total", justify="right", width=8)
        src_table.add_column("Downloaded", justify="right", width=11)
        src_table.add_column("QDA", justify="right", width=5)
        src_table.add_column("Restricted", justify="right", width=11)
        src_table.add_column("Size (GB)", justify="right", width=10)
        src_table.add_column("Datasets", justify="right", width=9)

        for row in source_rows:
            s_gb = (row.size or 0) / (1024 ** 3)
            size_str = f"{s_gb:.2f}" if s_gb >= 0.01 else "<0.01"
            src_table.add_row(
                row.source_name,
                str(row.total),
                str(row.downloaded),
                str(row.qda),
                str(row.restricted),
                size_str,
                str(row.datasets),
            )

        console.print(src_table)

        # ── 3. QDA Files by Format and Source ─────────────────────────
        qda_by_format = (
            session.query(File.file_type, func.count(File.id))
            .filter(File.is_qda_file.is_(True))
            .group_by(File.file_type)
            .order_by(func.count(File.id).desc())
            .all()
        )
        if qda_by_format:
            console.print()
            qda_table = Table(title="QDA Files by Format")
            qda_table.add_column("Format", style="bold", width=12)
            qda_table.add_column("Count", justify="right", width=8)
            for fmt, cnt in qda_by_format:
                qda_table.add_row(fmt or "unknown", str(cnt))
            console.print(qda_table)

        qda_by_source = (
            session.query(File.source_name, func.count(File.id))
            .filter(File.is_qda_file.is_(True))
            .group_by(File.source_name)
            .order_by(func.count(File.id).desc())
            .all()
        )
        if qda_by_source:
            console.print()
            qda_src_table = Table(title="QDA Files by Source")
            qda_src_table.add_column("Source", style="bold", width=16)
            qda_src_table.add_column("Count", justify="right", width=8)
            for src, cnt in qda_by_source:
                qda_src_table.add_row(src, str(cnt))
            console.print(qda_src_table)

        # ── 4. File Type Distribution (downloaded files, top 15) ──────
        ft_rows = (
            session.query(File.file_type, func.count(File.id))
            .filter(File.local_path.isnot(None))
            .group_by(File.file_type)
            .order_by(func.count(File.id).desc())
            .limit(15)
            .all()
        )
        if ft_rows:
            console.print()
            ft_table = Table(title="File Type Distribution (downloaded, top 15)")
            ft_table.add_column("Extension", style="bold", width=12)
            ft_table.add_column("Count", justify="right", width=8)
            ft_table.add_column("% of downloads", justify="right", width=15)
            for ext, cnt in ft_rows:
                pct = cnt / downloaded * 100 if downloaded else 0
                ft_table.add_row(ext or "none", str(cnt), f"{pct:.1f}%")
            console.print(ft_table)

        # ── 5. Qualitative Relevance ──────────────────────────────────
        dl_files = (
            session.query(File.title, File.description, File.keywords, File.kind_of_data)
            .filter(File.local_path.isnot(None))
            .all()
        )

        def _is_qualitative(title, description, keywords, kind_of_data):
            text = " ".join(
                (part or "").lower()
                for part in (title, description, keywords, kind_of_data)
            )
            return any(kw in text for kw in QUALITATIVE_KEYWORDS)

        qual_count = sum(1 for f in dl_files if _is_qualitative(*f))
        qual_pct = qual_count / len(dl_files) * 100 if dl_files else 0

        console.print(
            f"\n[bold]Qualitative Relevance (downloaded files):[/bold] "
            f"{qual_count:,}/{len(dl_files):,} ({qual_pct:.1f}%)"
        )

        # Per-source qualitative relevance
        dl_by_source = (
            session.query(
                File.source_name, File.title, File.description,
                File.keywords, File.kind_of_data,
            )
            .filter(File.local_path.isnot(None))
            .all()
        )
        source_qual: dict[str, list[int]] = {}  # {source: [total, qual]}
        for row in dl_by_source:
            src = row.source_name
            if src not in source_qual:
                source_qual[src] = [0, 0]
            source_qual[src][0] += 1
            if _is_qualitative(row.title, row.description, row.keywords, row.kind_of_data):
                source_qual[src][1] += 1

        qual_table = Table(title="Qualitative Relevance by Source")
        qual_table.add_column("Source", style="bold", width=16)
        qual_table.add_column("Downloaded", justify="right", width=11)
        qual_table.add_column("Qualitative", justify="right", width=12)
        qual_table.add_column("Rate", justify="right", width=8)
        for src in sorted(source_qual, key=lambda s: source_qual[s][0], reverse=True):
            t, q = source_qual[src]
            rate = q / t * 100 if t else 0
            qual_table.add_row(src, str(t), str(q), f"{rate:.1f}%")
        console.print()
        console.print(qual_table)

        # ── 6. Metadata Completeness ──────────────────────────────────
        metadata_fields = [
            ("description", File.description),
            ("license", File.license_type),
            ("keywords", File.keywords),
            ("language", File.language),
            ("kind_of_data", File.kind_of_data),
            ("geographic_coverage", File.geographic_coverage),
            ("software", File.software),
        ]

        console.print()
        mc_table = Table(title="Metadata Completeness (all records)")
        mc_table.add_column("Field", style="bold", width=22)
        mc_table.add_column("Records with data", justify="right", width=18)
        mc_table.add_column("Coverage", justify="right", width=10)
        for label, col in metadata_fields:
            filled = session.query(File).filter(col.isnot(None), col != "").count()
            pct = filled / total * 100 if total else 0
            mc_table.add_row(label, f"{filled:,}", f"{pct:.1f}%")
        console.print(mc_table)

        # ── 7. License Distribution (downloaded, top 10) ──────────────
        lic_rows = (
            session.query(File.license_type, func.count(File.id))
            .filter(File.local_path.isnot(None), File.license_type.isnot(None))
            .group_by(File.license_type)
            .order_by(func.count(File.id).desc())
            .limit(10)
            .all()
        )
        if lic_rows:
            console.print()
            lic_table = Table(title="License Distribution (downloaded, top 10)")
            lic_table.add_column("License", style="bold", width=40)
            lic_table.add_column("Count", justify="right", width=8)
            lic_table.add_column("% of downloads", justify="right", width=15)
            for lic, cnt in lic_rows:
                pct = cnt / downloaded * 100 if downloaded else 0
                lic_table.add_row(lic or "none", str(cnt), f"{pct:.1f}%")
            console.print(lic_table)

        # ── 8. Language Distribution (downloaded, top 10) ─────────────
        lang_rows = (
            session.query(File.language, func.count(File.id))
            .filter(File.local_path.isnot(None), File.language.isnot(None))
            .group_by(File.language)
            .order_by(func.count(File.id).desc())
            .limit(10)
            .all()
        )
        if lang_rows:
            console.print()
            lang_table = Table(title="Language Distribution (downloaded, top 10)")
            lang_table.add_column("Language", style="bold", width=40)
            lang_table.add_column("Count", justify="right", width=8)
            lang_table.add_column("% of downloads", justify="right", width=15)
            for lang, cnt in lang_rows:
                pct = cnt / downloaded * 100 if downloaded else 0
                lang_table.add_row(lang, str(cnt), f"{pct:.1f}%")
            console.print(lang_table)

        console.print()
    finally:
        session.close()


@cli.command("list-sources")
def list_sources() -> None:
    """List available data source connectors."""
    console.print("[bold]Available sources:[/bold]\n")
    for name, connector in CONNECTORS.items():
        console.print(f"  {name:<15} {connector.name:<45} [green]ready[/green]")

    skipped = [
        ("qualiservice", "Qualiservice — formal contract required"),
    ]
    for name, desc in skipped:
        if name not in CONNECTORS:
            console.print(f"  {name:<15} {desc:<45} [dim]skipped[/dim]")


if __name__ == "__main__":
    cli()
