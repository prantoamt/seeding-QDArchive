"""Export database contents to CSV."""

import csv
from pathlib import Path

from sqlalchemy import inspect

from pipeline.db.connection import get_session
from pipeline.db.models import File


def export_to_csv(output_path: Path) -> int:
    """Export all file records to CSV. Returns the number of rows exported."""
    session = get_session()
    try:
        records = session.query(File).all()
        if not records:
            return 0

        columns = [c.key for c in inspect(File).mapper.column_attrs]

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for record in records:
                writer.writerow([getattr(record, col) for col in columns])

        return len(records)
    finally:
        session.close()
