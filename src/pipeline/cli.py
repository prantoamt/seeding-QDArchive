"""CLI entry point for the pipeline."""

import logging
import os
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
    QUALITATIVE_KEYWORDS,
    REPOSITORY_IDS,
    REPOSITORY_URLS,
    SOURCE_DIR_NAMES,
    ensure_dirs,
    normalize_language,
)
from pipeline.connectors import CONNECTORS
from pipeline.db.connection import get_session, init_db
from pipeline.db.export import export_to_csv
from pipeline.db.models import (
    DownloadMethod,
    Keyword,
    PersonRole,
    PersonRoleType,
    Project,
    ProjectFile,
)
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


def _fsync_file(path: Path) -> None:
    """Force-flush a file to disk to prevent SMB write-buffer losses."""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _get_or_create_project(session, source, metadata, query_string):
    """Find an existing project by project_url or create a new one.

    Returns (project, is_new).
    """
    existing = session.query(Project).filter_by(project_url=metadata.source_url).first()
    if existing:
        return existing, False

    repo_id = REPOSITORY_IDS.get(source, 999)
    repo_url = REPOSITORY_URLS.get(source, "")
    dir_name = SOURCE_DIR_NAMES.get(source, source)

    # Language: take first language and convert to ISO 639-1
    lang = None
    if metadata.language:
        lang = normalize_language(metadata.language[0])

    # Determine if any file is restricted
    any_restricted = any(f.get("restricted", False) for f in metadata.files)

    project = Project(
        query_string=query_string,
        repository_id=repo_id,
        repository_url=repo_url,
        project_url=metadata.source_url,
        version=metadata.version or None,
        title=metadata.title,
        description=metadata.description or None,
        language=lang,
        doi=metadata.doi or None,
        upload_date=metadata.date_published or None,
        download_repository_folder=dir_name,
        download_project_folder=metadata.project_id_on_source,
        download_method=DownloadMethod.API_CALL,
        license_type=normalize_license(metadata.license_type),
        license_url=metadata.license_url or None,
        tags="; ".join(metadata.tags) if metadata.tags else None,
        kind_of_data="; ".join(metadata.kind_of_data) if metadata.kind_of_data else None,
        software="; ".join(metadata.software) if metadata.software else None,
        geographic_coverage=(
            "; ".join(metadata.geographic_coverage)
            if metadata.geographic_coverage else None
        ),
        restricted=any_restricted,
        depositor=metadata.depositor or None,
        producer="; ".join(metadata.producer) if metadata.producer else None,
        publication="; ".join(metadata.publication) if metadata.publication else None,
        date_of_collection=metadata.date_of_collection or None,
        time_period_covered=metadata.time_period_covered or None,
    )

    # Keywords
    for kw in metadata.keywords:
        if kw:
            project.keywords.append(Keyword(keyword=kw))

    # Persons
    for person in metadata.persons:
        name = person.get("name", "")
        if not name:
            continue
        role_str = person.get("role", "UNKNOWN").upper()
        try:
            role = PersonRoleType(role_str)
        except ValueError:
            role = PersonRoleType.UNKNOWN
        project.persons.append(PersonRole(name=name, role=role))

    session.add(project)
    session.flush()  # get project.id
    return project, True


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
        authors = "; ".join(p["name"] for p in r.persons[:3]) if r.persons else ""
        table.add_row(
            str(i),
            r.title[:60],
            authors[:30],
            r.date_published[:10] if r.date_published else "",
        )

    console.print(table)


def _scrape_results(connector, source, results, session, query_string):
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

        # Check qualitative relevance from description + keywords
        text_to_check = (metadata.description or "").lower()
        if metadata.keywords:
            text_to_check += " " + " ".join(kw.lower() for kw in metadata.keywords)
        if not any(kw in text_to_check for kw in QUALITATIVE_KEYWORDS):
            console.print("  [dim]Skipping — description has no qualitative relevance[/dim]")
            skipped_count += 1
            continue

        # Check if project already exists (dedup by project_url)
        project, is_new = _get_or_create_project(
            session, source, metadata, query_string
        )
        if not is_new:
            console.print("  [dim]Project already cataloged[/dim]")
            continue

        dir_name = SOURCE_DIR_NAMES.get(source, source)
        project_folder = metadata.project_id_on_source

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
            storage_path = get_storage_path(
                dir_name, project_folder, fname,
                version_folder=metadata.version or None,
            )
            dest_dir = str(storage_path.parent)

            # Skip download for known-restricted files
            if finfo.get("restricted", False):
                file_record = ProjectFile(
                    project_id=project.id,
                    file_name=fname,
                    file_type=file_ext,
                    file_size_bytes=finfo.get("size"),
                    content_type=finfo.get("content_type"),
                    friendly_type=finfo.get("friendly_type"),
                    api_checksum=finfo.get("api_checksum"),
                    is_qda_file=is_qda,
                    download_url=download_url,
                )
                session.add(file_record)
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
                    file_record = ProjectFile(
                        project_id=project.id,
                        file_name=fname,
                        file_type=file_ext,
                        file_size_bytes=finfo.get("size"),
                        content_type=finfo.get("content_type"),
                        friendly_type=finfo.get("friendly_type"),
                        api_checksum=finfo.get("api_checksum"),
                        is_qda_file=is_qda,
                        download_url=download_url,
                    )
                    session.add(file_record)
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

            # Force-flush to disk
            _fsync_file(Path(local_path))

            file_hash = compute_sha256(Path(local_path))

            # Check for duplicate by hash across all files
            existing_dup = (
                session.query(ProjectFile)
                .filter_by(file_hash=file_hash)
                .first()
            )
            if existing_dup:
                console.print(f"  [dim]Duplicate (hash match): {fname}[/dim]")
                Path(local_path).unlink(missing_ok=True)
                continue

            file_record = ProjectFile(
                project_id=project.id,
                file_name=fname,
                file_type=file_ext,
                file_hash=file_hash,
                file_size_bytes=finfo.get("size"),
                content_type=finfo.get("content_type"),
                friendly_type=finfo.get("friendly_type"),
                api_checksum=finfo.get("api_checksum"),
                is_qda_file=is_qda,
                download_url=download_url,
                local_path=str(Path(local_path).relative_to(PROJECT_ROOT)),
                downloaded_at=datetime.utcnow(),
            )
            session.add(file_record)
            downloaded_count += 1

            label = "[green]QDA[/green]" if is_qda else "[blue]file[/blue]"
            console.print(f"  {label} {fname} ({finfo.get('size', '?')} bytes)")

        # Set download_date after all files for this project
        project.download_date = datetime.utcnow()
        session.commit()

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

            dl, rest, skip = _scrape_results(connector, source, results, session, q)
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
    if queries_file is None:
        default_qf = PROJECT_ROOT / "queries.txt"
        if default_qf.exists():
            queries_file = str(default_qf)
    queries = _load_queries(queries_file, None)
    console.print(
        f"[bold]Scraping all {len(CONNECTORS)} sources "
        f"with {len(queries)} queries (limit={limit or 'none'}, retries={retries})[/bold]\n"
    )

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
        from sqlalchemy import case, distinct, func

        total_projects = session.query(Project).count()
        total_files = session.query(ProjectFile).count()
        qda_files = session.query(ProjectFile).filter(ProjectFile.is_qda_file.is_(True)).count()
        downloaded_files = session.query(ProjectFile).filter(
            ProjectFile.local_path.isnot(None)
        ).count()
        restricted_projects = session.query(Project).filter(
            Project.restricted.is_(True)
        ).count()

        console.print(f"[bold]Total projects:[/bold]    {total_projects}")
        console.print(f"[bold]Total files:[/bold]       {total_files}")
        console.print(f"[bold]QDA files:[/bold]         {qda_files}")
        console.print()
        console.print(f"  [green]Downloaded:[/green]      {downloaded_files}")
        console.print(f"  [yellow]Restricted:[/yellow]      {restricted_projects} projects")

        # Per-source breakdown
        col_projects = func.count(distinct(Project.id))
        col_files = func.count(ProjectFile.id)
        col_qda = func.sum(case((ProjectFile.is_qda_file.is_(True), 1), else_=0))
        col_dl = func.sum(case((ProjectFile.local_path.isnot(None), 1), else_=0))

        source_rows = (
            session.query(
                Project.download_repository_folder,
                col_projects.label("projects"),
                col_files.label("files"),
                col_qda.label("qda"),
                col_dl.label("downloaded"),
            )
            .outerjoin(ProjectFile, Project.id == ProjectFile.project_id)
            .group_by(Project.download_repository_folder)
            .order_by(col_files.desc())
            .all()
        )

        if source_rows:
            console.print("\n[bold]By source:[/bold]")
            header = f"  {'Source':>20}  {'Projects':>9}  {'Files':>7}  {'QDA':>5}  {'Down':>7}"
            console.print(f"[dim]{header}[/dim]")
            for name, proj, files, qda, dl in source_rows:
                console.print(
                    f"  {name:>20}  {proj:>9}  {files:>7}  {qda or 0:>5}  {dl or 0:>7}"
                )

        # Language breakdown
        lang_rows = (
            session.query(Project.language, func.count(Project.id))
            .filter(Project.language.isnot(None))
            .group_by(Project.language)
            .order_by(func.count(Project.id).desc())
            .limit(10)
            .all()
        )
        if lang_rows:
            console.print("\n[bold]By language (top 10):[/bold]")
            for lang, cnt in lang_rows:
                console.print(f"  {lang:>20}  {cnt:>5} projects")

        # File type breakdown
        ft_rows = (
            session.query(ProjectFile.file_type, func.count(ProjectFile.id))
            .filter(ProjectFile.local_path.isnot(None))
            .group_by(ProjectFile.file_type)
            .order_by(func.count(ProjectFile.id).desc())
            .limit(10)
            .all()
        )
        if ft_rows:
            console.print("\n[bold]By file type (downloaded, top 10):[/bold]")
            for ext, cnt in ft_rows:
                console.print(f"  {ext or 'none':>20}  {cnt:>5}")

    finally:
        session.close()


@cli.command("db")
@click.option("--source", "-s", default=None, help="Filter by source (repository folder).")
@click.option("--qda-only", is_flag=True, help="Show only projects with QDA files.")
@click.option("--restricted-only", is_flag=True, help="Show only restricted projects.")
@click.option("--search", default=None, help="Search title, description, keywords.")
@click.option("--language", default=None, help="Filter by language code (substring match).")
@click.option("--limit", "-n", default=50, type=int, help="Max rows to display.")
def db_view(
    source: str | None,
    qda_only: bool,
    restricted_only: bool,
    search: str | None,
    language: str | None,
    limit: int,
) -> None:
    """Browse the project database."""
    session = get_session()
    try:
        from sqlalchemy import or_

        query = session.query(Project)
        if source:
            query = query.filter(Project.download_repository_folder == source)
        if restricted_only:
            query = query.filter(Project.restricted.is_(True))
        if search:
            pattern = f"%{search}%"
            query = query.filter(or_(
                Project.title.ilike(pattern),
                Project.description.ilike(pattern),
            ))
        if language:
            query = query.filter(Project.language.ilike(f"%{language}%"))
        if qda_only:
            query = query.join(ProjectFile).filter(ProjectFile.is_qda_file.is_(True))

        total = query.count()
        records = query.order_by(Project.id).limit(limit).all()

        if not records:
            console.print("[yellow]No projects found.[/yellow]")
            return

        table = Table(title=f"Projects ({total} total, showing {len(records)})")
        table.add_column("ID", style="dim", width=5)
        table.add_column("Title", max_width=50)
        table.add_column("Source", width=14)
        table.add_column("Files", width=6, justify="right")
        table.add_column("Lang", width=5)
        table.add_column("Status", width=12)

        for p in records:
            file_count = len(p.files) if p.files else 0
            if p.restricted:
                status_str = "[yellow]restricted[/yellow]"
            elif any(f.local_path for f in p.files):
                status_str = "[green]downloaded[/green]"
            else:
                status_str = "[dim]metadata[/dim]"

            table.add_row(
                str(p.id),
                (p.title[:50] if p.title else "—"),
                p.download_repository_folder,
                str(file_count),
                p.language or "",
                status_str,
            )

        console.print(table)

        if total > limit:
            console.print(f"[dim]Showing {limit} of {total} — use --limit to see more[/dim]")
    finally:
        session.close()


@cli.command("show")
@click.argument("ids", nargs=-1, required=True, type=int)
def db_show(ids: tuple[int, ...]) -> None:
    """Show full details for one or more projects by ID."""
    session = get_session()
    try:
        for project_id in ids:
            p = session.query(Project).filter_by(id=project_id).first()
            if not p:
                console.print(f"[red]Project {project_id} not found.[/red]")
                continue

            from rich.panel import Panel

            persons_str = "; ".join(
                f"{pr.name} ({pr.role.value})" for pr in p.persons
            )
            keywords_str = "; ".join(kw.keyword for kw in p.keywords)

            desc = p.description or ""
            if len(desc) > 300:
                desc = desc[:300] + "..."

            lines = [
                f"[bold]Title:[/bold]       {p.title or '—'}",
                f"[bold]Description:[/bold] {desc or '—'}",
                f"[bold]Persons:[/bold]     {persons_str or '—'}",
                f"[bold]Keywords:[/bold]    {keywords_str or '—'}",
                f"[bold]Language:[/bold]    {p.language or '—'}",
                f"[bold]DOI:[/bold]         {p.doi or '—'}",
                f"[bold]Version:[/bold]     {p.version or '—'}",
                f"[bold]Published:[/bold]   {p.upload_date or '—'}",
                f"[bold]License:[/bold]     {p.license_type or '—'}",
                f"[bold]Tags:[/bold]        {p.tags or '—'}",
                "",
                f"[bold]Repository:[/bold]  {p.repository_url} (ID: {p.repository_id})",
                f"[bold]Project URL:[/bold] {p.project_url}",
                f"[bold]Method:[/bold]      "
                f"{p.download_method.value if p.download_method else '—'}",
                f"[bold]Folder:[/bold]      "
                f"{p.download_repository_folder}/{p.download_project_folder}",
                f"[bold]Downloaded:[/bold]  {p.download_date or '—'}",
                f"[bold]Restricted:[/bold]  {'yes' if p.restricted else 'no'}",
                f"[bold]Query:[/bold]       {p.query_string or '—'}",
            ]

            extras = [
                ("Kind of data", p.kind_of_data),
                ("Software", p.software),
                ("Geography", p.geographic_coverage),
                ("Depositor", p.depositor),
                ("Producer", p.producer),
                ("Publication", p.publication),
                ("Collection", p.date_of_collection),
                ("Time period", p.time_period_covered),
                ("Notes", p.notes),
            ]
            has_extras = any(v for _, v in extras)
            if has_extras:
                lines.append("")
                for label, val in extras:
                    if val:
                        lines.append(f"[bold]{label}:[/bold] {val}")

            if p.files:
                lines.append(f"\n[bold]Files ({len(p.files)}):[/bold]")
                for f in p.files:
                    status = ""
                    if f.local_path:
                        status = "[green]downloaded[/green]"
                    elif f.is_qda_file:
                        status = "[yellow]QDA (not downloaded)[/yellow]"
                    size = _format_size(f.file_size_bytes) if f.file_size_bytes else ""
                    qda_tag = " [green][QDA][/green]" if f.is_qda_file else ""
                    lines.append(f"  {f.file_name}{qda_tag} {size} {status}")

            console.print(Panel(
                "\n".join(lines),
                title=f"Project #{p.id}",
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
        total_projects = session.query(Project).count()
        total_files = session.query(ProjectFile).count()
        downloaded_files = session.query(ProjectFile).filter(
            ProjectFile.local_path.isnot(None)
        ).count()
        qda_total = session.query(ProjectFile).filter(
            ProjectFile.is_qda_file.is_(True)
        ).count()
        qda_downloaded = (
            session.query(ProjectFile)
            .filter(ProjectFile.is_qda_file.is_(True), ProjectFile.local_path.isnot(None))
            .count()
        )
        restricted_projects = session.query(Project).filter(
            Project.restricted.is_(True)
        ).count()
        total_size = (
            session.query(func.sum(ProjectFile.file_size_bytes))
            .filter(ProjectFile.local_path.isnot(None))
            .scalar()
        ) or 0
        dup_hashes = (
            session.query(ProjectFile.file_hash)
            .filter(ProjectFile.file_hash.isnot(None))
            .group_by(ProjectFile.file_hash)
            .having(func.count(ProjectFile.id) > 1)
            .count()
        )
        qda_formats = (
            session.query(func.count(distinct(ProjectFile.file_type)))
            .filter(ProjectFile.is_qda_file.is_(True))
            .scalar()
        ) or 0

        size_gb = total_size / (1024 ** 3)

        console.print("\n[bold cyan]═══ Comprehensive Data Analysis ═══[/bold cyan]\n")

        summary = Table(title="Executive Summary", show_header=False, pad_edge=False)
        summary.add_column("Metric", style="bold", width=30)
        summary.add_column("Value", justify="right", width=20)
        summary.add_row("Total projects", f"{total_projects:,}")
        summary.add_row("Total file records", f"{total_files:,}")
        summary.add_row("Files downloaded", f"{downloaded_files:,} ({size_gb:.2f} GB)")
        summary.add_row("QDA files found", f"{qda_total} (across {qda_formats} formats)")
        summary.add_row("QDA files downloaded", str(qda_downloaded))
        summary.add_row("Restricted projects", f"{restricted_projects:,}")
        summary.add_row("Duplicate files (by SHA-256)", str(dup_hashes))
        console.print(summary)

        # Per-source breakdown
        col_projects = func.count(distinct(Project.id))
        col_files = func.count(ProjectFile.id)
        col_dl = func.sum(case((ProjectFile.local_path.isnot(None), 1), else_=0))
        col_qda = func.sum(case((ProjectFile.is_qda_file.is_(True), 1), else_=0))
        col_size = func.sum(
            case((ProjectFile.local_path.isnot(None), ProjectFile.file_size_bytes), else_=0)
        )

        source_rows = (
            session.query(
                Project.download_repository_folder,
                col_projects.label("projects"),
                col_files.label("files"),
                col_dl.label("downloaded"),
                col_qda.label("qda"),
                col_size.label("size"),
            )
            .outerjoin(ProjectFile, Project.id == ProjectFile.project_id)
            .group_by(Project.download_repository_folder)
            .order_by(col_files.desc())
            .all()
        )

        console.print()
        src_table = Table(title="Per-Source Breakdown")
        src_table.add_column("Source", style="bold", width=16)
        src_table.add_column("Projects", justify="right", width=9)
        src_table.add_column("Files", justify="right", width=7)
        src_table.add_column("Downloaded", justify="right", width=11)
        src_table.add_column("QDA", justify="right", width=5)
        src_table.add_column("Size (GB)", justify="right", width=10)

        for row in source_rows:
            s_gb = (row.size or 0) / (1024 ** 3)
            size_str = f"{s_gb:.2f}" if s_gb >= 0.01 else "<0.01"
            src_table.add_row(
                row.download_repository_folder,
                str(row.projects),
                str(row.files),
                str(row.downloaded),
                str(row.qda),
                size_str,
            )
        console.print(src_table)

        # QDA files by format
        qda_by_format = (
            session.query(ProjectFile.file_type, func.count(ProjectFile.id))
            .filter(ProjectFile.is_qda_file.is_(True))
            .group_by(ProjectFile.file_type)
            .order_by(func.count(ProjectFile.id).desc())
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

        # File type distribution
        ft_rows = (
            session.query(ProjectFile.file_type, func.count(ProjectFile.id))
            .filter(ProjectFile.local_path.isnot(None))
            .group_by(ProjectFile.file_type)
            .order_by(func.count(ProjectFile.id).desc())
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
                pct = cnt / downloaded_files * 100 if downloaded_files else 0
                ft_table.add_row(ext or "none", str(cnt), f"{pct:.1f}%")
            console.print(ft_table)

        # License distribution
        lic_rows = (
            session.query(Project.license_type, func.count(Project.id))
            .filter(Project.license_type.isnot(None))
            .group_by(Project.license_type)
            .order_by(func.count(Project.id).desc())
            .limit(10)
            .all()
        )
        if lic_rows:
            console.print()
            lic_table = Table(title="License Distribution (top 10)")
            lic_table.add_column("License", style="bold", width=40)
            lic_table.add_column("Projects", justify="right", width=9)
            for lic, cnt in lic_rows:
                lic_table.add_row(lic or "none", str(cnt))
            console.print(lic_table)

        # Language distribution
        lang_rows = (
            session.query(Project.language, func.count(Project.id))
            .filter(Project.language.isnot(None))
            .group_by(Project.language)
            .order_by(func.count(Project.id).desc())
            .limit(10)
            .all()
        )
        if lang_rows:
            console.print()
            lang_table = Table(title="Language Distribution (top 10)")
            lang_table.add_column("Language", style="bold", width=20)
            lang_table.add_column("Projects", justify="right", width=9)
            for lang, cnt in lang_rows:
                lang_table.add_row(lang, str(cnt))
            console.print(lang_table)

        console.print()
    finally:
        session.close()


@cli.command("list-sources")
def list_sources() -> None:
    """List available data source connectors."""
    console.print("[bold]Available sources:[/bold]\n")
    for name, connector in CONNECTORS.items():
        repo_id = REPOSITORY_IDS.get(name, "—")
        console.print(f"  {name:<15} {connector.name:<45} ID={repo_id}  [green]ready[/green]")

    skipped = [
        ("qualiservice", "Qualiservice — formal contract required"),
    ]
    for name, desc in skipped:
        if name not in CONNECTORS:
            console.print(f"  {name:<15} {desc:<45}       [dim]skipped[/dim]")


if __name__ == "__main__":
    cli()
