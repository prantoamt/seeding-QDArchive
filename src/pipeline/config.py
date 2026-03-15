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
    # REFI-QDA
    ".qdpx",      # REFI-QDA project / QDAcity
    ".qde",       # REFI-QDA exchange
    ".qdc",       # REFI-QDA codebook
    # MAXQDA — current & legacy
    ".mx",        # MAXQDA (generic)
    ".mqda",      # MAXQDA project
    ".mx24",      # MAXQDA 2024
    ".mx24bac",   # MAXQDA 2024 backup
    ".mc24",      # MAXQDA 2024 codebook
    ".mex24",     # MAXQDA 2024 exchange
    ".mx22",      # MAXQDA 2022
    ".mex22",     # MAXQDA 2022 exchange
    ".mx20",      # MAXQDA 2020
    ".mx18",      # MAXQDA 2018
    ".mx12",      # MAXQDA 12
    ".mx11",      # MAXQDA 11
    ".mx5",       # MAXQDA 10/5
    ".mx4",       # MAXQDA 4
    ".mx3",       # MAXQDA 3
    ".mx2",       # MAXQDA 2
    ".m2k",       # MAXQDA 2000
    ".mqbac",     # MAXQDA backup
    ".mqtc",      # MAXQDA teamwork
    ".mqex",      # MAXQDA exchange
    ".mqmtr",     # MAXQDA memo transfer
    ".loa",       # MAXQDA auxiliary
    ".sea",       # MAXQDA auxiliary
    ".mtr",       # MAXQDA auxiliary
    ".mod",       # MAXQDA auxiliary
    # NVivo
    ".nvp",       # NVivo (older)
    ".nvpx",      # NVivo
    # ATLAS.ti
    ".atlproj",   # ATLAS.ti (current)
    ".atlasproj", # ATLAS.ti (variant)
    ".hpr7",      # ATLAS.ti 7 hermeneutic unit
    # Dedoose
    ".ddx",       # Dedoose
    # QDA Miner
    ".qda",       # QDA Miner
    ".qpd",       # QDA Miner project
    # Transana
    ".ppj",       # Transana project
    ".pprj",      # Transana project
    ".qlt",       # Transana
    # f4analyse
    ".f4p",       # f4analyse project
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

# Repository IDs — course-defined (1–20), our additional sources (101+)
REPOSITORY_IDS: dict[str, int] = {
    # Course-defined
    "zenodo": 1,
    "dryad": 2,
    "ukds": 3,
    "qdr": 4,
    "dans": 5,
    "dataverseno": 6,
    "harvard": 10,
    # Our additional sources
    "sodha": 101,
    "acss": 102,
    "kuleuven": 103,
    "uclouvain": 104,
    "repod": 105,
    "heidata": 106,
    "bonndata": 107,
    "dataverselv": 108,
    "crossda": 109,
    "darus": 110,
    "rsu": 111,
    "nycu": 113,
    "pucp": 114,
}

# Repository base URLs — maps source key to top-level repository URL
REPOSITORY_URLS: dict[str, str] = {
    "zenodo": "https://zenodo.org",
    "dryad": "https://datadryad.org",
    "ukds": "https://reshare.ukdataservice.ac.uk",
    "qdr": "https://data.qdr.syr.edu",
    "dans": "https://ssh.datastations.nl",
    "dataverseno": "https://dataverse.no",
    "harvard": "https://dataverse.harvard.edu",
    "sodha": "https://www.sodha.be",
    "acss": "https://dataverse.theacss.org",
    "kuleuven": "https://rdr.kuleuven.be",
    "uclouvain": "https://dataverse.uclouvain.be",
    "repod": "https://repod.icm.edu.pl",
    "heidata": "https://heidata.uni-heidelberg.de",
    "bonndata": "https://bonndata.uni-bonn.de",
    "dataverselv": "https://dv.dataverse.lv",
    "crossda": "https://data.crossda.hr",
    "darus": "https://darus.uni-stuttgart.de",
    "rsu": "https://dataverse.rsu.lv",
    "nycu": "https://dataverse.lib.nycu.edu.tw",
    "pucp": "https://datos.pucp.edu.pe",
}

# ISO 639-1 language code mapping (free-text → 2-letter code)
LANGUAGE_MAP: dict[str, str] = {
    "english": "en",
    "german": "de",
    "french": "fr",
    "dutch": "nl",
    "norwegian": "no",
    "spanish": "es",
    "portuguese": "pt",
    "italian": "it",
    "swedish": "sv",
    "danish": "da",
    "finnish": "fi",
    "polish": "pl",
    "czech": "cs",
    "croatian": "hr",
    "hungarian": "hu",
    "romanian": "ro",
    "turkish": "tr",
    "arabic": "ar",
    "chinese": "zh",
    "japanese": "ja",
    "korean": "ko",
    "russian": "ru",
    "greek": "el",
    "latvian": "lv",
    "lithuanian": "lt",
    "estonian": "et",
    "slovenian": "sl",
    "slovak": "sk",
    "bulgarian": "bg",
    "serbian": "sr",
    "bosnian": "bs",
    "catalan": "ca",
    "basque": "eu",
    "galician": "gl",
    "welsh": "cy",
    "irish": "ga",
    "afrikaans": "af",
    "hindi": "hi",
    "urdu": "ur",
    "thai": "th",
    "vietnamese": "vi",
    "indonesian": "id",
    "malay": "ms",
    "swahili": "sw",
    "hebrew": "he",
    "persian": "fa",
    "ukrainian": "uk",
    "tamil": "ta",
    "bengali": "bn",
    "tagalog": "tl",
}


def normalize_language(raw: str) -> str | None:
    """Convert a free-text language string to ISO 639-1.

    Returns the 2-letter code if recognized, or the original string lowered
    if it already looks like a code (2-3 chars). Returns None for empty input.
    """
    if not raw or not raw.strip():
        return None
    cleaned = raw.strip().lower()
    # Already a code (e.g., "en", "eng", "en-US")
    if len(cleaned) <= 3:
        return cleaned[:2]
    # BCP 47 with region (e.g., "en-US")
    if "-" in cleaned and len(cleaned) <= 6:
        return cleaned.split("-")[0]
    # Look up in mapping
    return LANGUAGE_MAP.get(cleaned, cleaned)


# Human-readable directory names for each source (used in data/ folder)
SOURCE_DIR_NAMES: dict[str, str] = {
    "qdr": "qdr",
    "dans": "dans",
    "dataverseno": "dataverse-no",
    "harvard": "harvard",
    "zenodo": "zenodo",
    "ukds": "uk-data-service",
    "sodha": "sodha",
    "acss": "acss",
    "kuleuven": "kuleuven",
    "uclouvain": "uclouvain",
    "repod": "repod",
    "heidata": "heidata",
    "bonndata": "bonndata",
    "dataverselv": "dataverse-lv",
    "crossda": "crossda",
    "darus": "darus",
    "rsu": "rsu",
    "nycu": "nycu",
    "pucp": "pucp",
    "dryad": "dryad",
}


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
