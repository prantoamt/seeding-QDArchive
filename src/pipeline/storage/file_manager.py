"""File download, organization, and hashing utilities."""

import hashlib
import re
import unicodedata
from pathlib import Path

from pipeline.config import DATA_DIR


def slugify(text: str, max_length: int = 60) -> str:
    """Convert text to a filesystem-friendly slug.

    NFKD normalize → ASCII → lowercase → non-alphanumeric to hyphens
    → collapse runs → strip → truncate on word boundary.
    """
    # Normalize unicode and drop non-ASCII
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    # Replace non-alphanumeric characters with hyphens
    text = re.sub(r"[^a-z0-9]+", "-", text)
    # Collapse multiple hyphens and strip leading/trailing
    text = re.sub(r"-{2,}", "-", text).strip("-")
    # Truncate on word boundary
    if len(text) > max_length:
        text = text[:max_length].rsplit("-", 1)[0]
    return text


def get_storage_path(
    source_name: str, record_id: str, filename: str, title: str | None = None
) -> Path:
    """Return the local storage path: data/{source_name}/{slug-record_id}/{filename}.

    When *title* is given, the directory is ``{slugify(title)}-{record_id}``.
    Falls back to plain ``{record_id}`` when title is None or empty.
    """
    slug = slugify(title) if title else ""
    dir_name = f"{slug}-{record_id}" if slug else record_id
    path = DATA_DIR / source_name / dir_name
    path.mkdir(parents=True, exist_ok=True)
    return path / filename


def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
