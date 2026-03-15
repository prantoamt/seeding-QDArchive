"""Export database contents to CSV (flat join format)."""

import csv
from pathlib import Path

from sqlalchemy.orm import joinedload

from pipeline.db.connection import get_session
from pipeline.db.models import Project


def export_to_csv(output_path: Path) -> int:
    """Export all projects to a flat CSV (one row per file, project fields repeated).

    Keywords and persons are comma-joined into single columns.
    Returns the number of rows exported.
    """
    session = get_session()
    try:
        projects = (
            session.query(Project)
            .options(
                joinedload(Project.files),
                joinedload(Project.keywords),
                joinedload(Project.persons),
            )
            .all()
        )
        if not projects:
            return 0

        headers = [
            # Project fields (course schema)
            "project_id", "query_string", "repository_id", "repository_url",
            "project_url", "version", "title", "description", "language",
            "doi", "upload_date", "download_date",
            "download_repository_folder", "download_project_folder",
            "download_version_folder", "download_method",
            # Project extra fields
            "license_type", "license_url", "tags", "kind_of_data",
            "software", "geographic_coverage", "restricted",
            "depositor", "producer", "publication",
            "date_of_collection", "time_period_covered", "notes",
            # Joined fields
            "keywords", "persons",
            # File fields (course schema)
            "file_id", "file_name", "file_type",
            # File extra fields
            "file_hash", "file_size_bytes", "content_type", "friendly_type",
            "api_checksum", "is_qda_file", "download_url", "local_path",
            "downloaded_at",
        ]

        row_count = 0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for project in projects:
                keywords_str = "; ".join(kw.keyword for kw in project.keywords)
                persons_str = "; ".join(
                    f"{p.name} ({p.role.value})" for p in project.persons
                )

                project_row = [
                    project.id, project.query_string, project.repository_id,
                    project.repository_url, project.project_url,
                    project.version, project.title, project.description,
                    project.language, project.doi, project.upload_date,
                    project.download_date,
                    project.download_repository_folder,
                    project.download_project_folder,
                    project.download_version_folder,
                    project.download_method.value if project.download_method else "",
                    project.license_type, project.license_url,
                    project.tags, project.kind_of_data,
                    project.software, project.geographic_coverage,
                    project.restricted, project.depositor,
                    project.producer, project.publication,
                    project.date_of_collection, project.time_period_covered,
                    project.notes,
                    keywords_str, persons_str,
                ]

                if project.files:
                    for pf in project.files:
                        file_row = [
                            pf.id, pf.file_name, pf.file_type,
                            pf.file_hash, pf.file_size_bytes,
                            pf.content_type, pf.friendly_type,
                            pf.api_checksum, pf.is_qda_file,
                            pf.download_url, pf.local_path,
                            pf.downloaded_at,
                        ]
                        writer.writerow(project_row + file_row)
                        row_count += 1
                else:
                    # Project with no files (metadata-only)
                    writer.writerow(project_row + [""] * 11)
                    row_count += 1

        return row_count
    finally:
        session.close()
