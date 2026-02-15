"""File download, organization, and hashing utilities."""

import hashlib
from pathlib import Path

from pipeline.config import DATA_DIR


def get_storage_path(source_name: str, record_id: str, filename: str) -> Path:
    """Return the local storage path: data/{source_name}/{record_id}/{filename}."""
    path = DATA_DIR / source_name / record_id
    path.mkdir(parents=True, exist_ok=True)
    return path / filename


def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
