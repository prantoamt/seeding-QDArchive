"""Project-wide configuration: paths, DB location, constants."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
EXPORTS_DIR = PROJECT_ROOT / "exports"
DB_PATH = PROJECT_ROOT / "pipeline.db"
LOG_FILE = PROJECT_ROOT / "pipeline.log"

DB_URL = f"sqlite:///{DB_PATH}"

# Known QDA file extensions (from QDA File Extensions Formats overview)
QDA_EXTENSIONS = {
    ".qdpx",    # REFI-QDA / QDAcity
    ".qde",     # REFI-QDA exchange
    ".mx",      # MAXQDA
    ".mx18",    # MAXQDA 2018
    ".mx20",    # MAXQDA 2020
    ".mx22",    # MAXQDA 2022
    ".mx24",    # MAXQDA 2024
    ".nvp",     # NVivo (older)
    ".nvpx",    # NVivo
    ".atlproj", # ATLAS.ti
    ".ddx",     # Dedoose
    ".qda",     # QDA Miner
}

QUALITATIVE_EXTENSIONS = {".txt", ".pdf", ".rtf", ".docx"}

# Human-readable directory names for each source (used in data/ folder)
SOURCE_DIR_NAMES: dict[str, str] = {
    "qdr": "qdr",
    "dans": "dans",
    "dataverseno": "dataverse-no",
    "zenodo": "zenodo",
    "dryad": "dryad",
    "ukds": "uk-data-service",
    "qualidatanet": "qualidata-net",
}


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
