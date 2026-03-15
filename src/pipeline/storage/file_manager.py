"""File download, organization, and hashing utilities."""

import hashlib
from pathlib import Path

from pipeline.config import DATA_DIR


def get_storage_path(
    repository_folder: str, project_folder: str, filename: str,
    version_folder: str | None = None,
) -> Path:
    """Return the local storage path: data/{repository_folder}/{project_folder}/{filename}.

    If version_folder is given:
        data/{repository_folder}/{project_folder}/{version_folder}/{filename}

    If the target path already exists on disk, a numeric suffix is appended
    to avoid overwriting (e.g., ``results_table_2.txt``).
    """
    path = DATA_DIR / repository_folder / project_folder
    if version_folder:
        path = path / version_folder
    path.mkdir(parents=True, exist_ok=True)

    target = path / filename
    if target.exists():
        stem = Path(filename).stem
        suffix = Path(filename).suffix
        counter = 2
        while target.exists():
            target = path / f"{stem}_{counter}{suffix}"
            counter += 1
    return target


def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
