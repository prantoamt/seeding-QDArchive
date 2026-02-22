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

# kind_of_data values that are NOT qualitative research data.
# Records with these types are saved as metadata-only (no download).
# Source: Zenodo resource_type.type values; Dataverse kindOfData is free-text.
SKIP_KIND_OF_DATA = {
    "publication",
    "presentation",
    "poster",
    "lesson",
    "software",
    "workflow",
    "image",
    "video",
    "event",
    "model",
}

# Keywords that signal qualitative relevance in a dataset description.
# Checked case-insensitively; includes common non-English equivalents.
QUALITATIVE_KEYWORDS = {
    # English
    "qualitative",
    "interview",
    "focus group",
    "ethnograph",          # ethnography, ethnographic
    "grounded theory",
    "thematic analysis",
    "narrative analysis",
    "case study",
    "participant observation",
    "semi-structured",
    "in-depth interview",
    "transcript",
    "field note",
    "fieldwork",
    "life histor",         # life history, life histories
    "oral histor",         # oral history, oral histories
    "phenomenolog",        # phenomenology, phenomenological
    "discourse analysis",
    "content analysis",
    "coding scheme",
    "coded data",
    "open-ended",
    # QDA software names (strong signal)
    "nvivo",
    "atlas.ti",
    "maxqda",
    "dedoose",
    "qdacity",
    "qda miner",
    "refi-qda",
    "caqdas",
    # Dutch (DANS)
    "kwalitatief",
    "interview",           # same in Dutch
    "focusgroep",
    "etnograf",
    # Norwegian (DataverseNO)
    "kvalitativ",
    "intervju",
    "fokusgruppe",
    # German (QualidataNet)
    "qualitativ",
    "leitfadeninterview",
    "gruppendiskussion",
    "biografieforschung",
    "inhaltsanalyse",
    "transkript",
    # Spanish
    "cualitativ",           # cualitativa, cualitativo
    "entrevista",
    "grupo focal",
    "análisis temático",
    # French
    "qualitatif",           # qualitatif, qualitative (French)
    "entretien",
    "groupe de discussion",
    # Portuguese
    "qualitativ",           # already matches Portuguese "qualitativa"
    "pesquisa qualitativa",
    "entrevista qualitativa",
    "grupo focal",          # same in Portuguese/Spanish
    "análise temática",
}

# Human-readable directory names for each source (used in data/ folder)
SOURCE_DIR_NAMES: dict[str, str] = {
    "qdr": "qdr",
    "dans": "dans",
    "dataverseno": "dataverse-no",
    "harvard": "harvard",
    "zenodo": "zenodo",
    "ukds": "uk-data-service",
}


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
