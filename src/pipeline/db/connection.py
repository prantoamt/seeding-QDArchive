"""Database session management."""

import logging

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from pipeline.config import DB_URL
from pipeline.db.models import Base, File

logger = logging.getLogger("pipeline")

engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

# New nullable columns added after initial schema.  Maps column name to SQL type.
_MIGRATION_COLUMNS: dict[str, str] = {
    "keywords": "TEXT",
    "kind_of_data": "TEXT",
    "language": "VARCHAR(100)",
    "content_type": "VARCHAR(200)",
    "friendly_type": "VARCHAR(200)",
    "software": "TEXT",
    "geographic_coverage": "TEXT",
    "restricted": "BOOLEAN",
    "api_checksum": "VARCHAR(150)",
}


def _migrate_add_columns() -> None:
    """Add any missing columns to the files table (lightweight migration)."""
    insp = inspect(engine)
    if not insp.has_table(File.__tablename__):
        return  # table doesn't exist yet; create_all will handle it

    existing = {col["name"] for col in insp.get_columns(File.__tablename__)}
    with engine.begin() as conn:
        for col_name, col_type in _MIGRATION_COLUMNS.items():
            if col_name not in existing:
                conn.execute(
                    text(f"ALTER TABLE {File.__tablename__} ADD COLUMN {col_name} {col_type}")
                )
                logger.info("Migration: added column '%s' to files table", col_name)


def init_db() -> None:
    """Create all tables if they don't exist, then apply migrations."""
    Base.metadata.create_all(engine)
    _migrate_add_columns()


def get_session() -> Session:
    """Return a new database session."""
    return SessionLocal()
