# Implementation Plan

Detailed technical implementation plan for the Seeding QDArchive project (Parts 1 & 2).

**Tech stack:** Python, PDM, SQLite, CLI interface, per-source connectors (API-first, scraping as fallback)

---

## Data Source Reference

Access method and API details for each professor-provided repository.

### API-based sources (priority — build these first)

| Source         | API Type  | Base URL                           | Auth for Search | Auth for Download       |
| -------------- | --------- | ---------------------------------- | --------------- | ----------------------- |
| Zenodo         | REST      | `https://zenodo.org/api/`          | No              | No (public files)       |
| Dryad          | REST v2   | `https://datadryad.org/api/v2/`    | No              | No (public files)       |
| QDR (Syracuse) | Dataverse | `https://data.qdr.syr.edu/api/`    | No              | Depends on access level |
| DANS           | Dataverse | Dataverse API (migrated from EASY) | No              | No (open data)          |
| DataverseNO    | Dataverse | `https://dataverse.no/api/`        | No              | No (open data)          |

> **Note on "Dataverse" API type:** Dataverse is open-source repository software (developed by Harvard IQSS) that institutions install to host their own data repositories. All Dataverse installations share the same API endpoints and response schema — same JSON structure, same field names (`global_id`, `latestVersion`, `metadataBlocks`, etc.). This means a single connector class works for QDR, DANS, and DataverseNO by just swapping the base URL. Minor differences exist (custom metadata blocks, access restrictions) but the core citation schema is always the same. Zenodo and Dryad, by contrast, are custom platforms with their own unique REST APIs.

**Zenodo**

- Docs: https://developers.zenodo.org/
- Key endpoint: `GET /api/records?q=...&type=dataset`
- Rich metadata: title, authors, license, DOI, dates, description

**Dryad**

- Docs: https://datadryad.org/api/v2/docs
- Key endpoint: `GET /api/v2/search?q=...`
- Also has R package `rdryad` for programmatic access

**Dataverse (shared API for QDR, DANS, DataverseNO)**

- Docs: https://guides.dataverse.org/en/latest/api/index.html
- Key endpoint: `GET /api/search?q=...&type=dataset`
- One connector, three sources — just swap the base URL
- QDR is highest priority (dedicated qualitative data repository)
- Also supports OAI-PMH harvesting

### Scraping-required sources

| Source          | Access Method         | Data Available?                       | Notes                                                               |
| --------------- | --------------------- | ------------------------------------- | ------------------------------------------------------------------- |
| UK Data Service | Scrape QualiBank      | Metadata yes, files need registration | No API for qualitative data; SDMX API exists but only for macrodata |
| QualidataNet    | Scrape search portal  | Metadata only                         | Federated portal — actual files hosted at partner institutions      |
| Qualiservice    | Scrape QSearch portal | Metadata only                         | Data download requires formal contract                              |

### Not pursued

| Source  | Reason                                                                                |
| ------- | ------------------------------------------------------------------------------------- |
| QualiBi | No public-facing data portal found; contributes only to QualidataNet federated search |

---

## QDA File Format Reference

### QDPX/REFI Format (primary target)

A `.qdpx` file is a ZIP archive containing:

```
my_analysis.qdpx (renamed .zip)
├── project.qde          # XML — codes, annotations, structure
├── sources/             # Original data files
│   ├── interview_01.txt
│   ├── interview_02.docx
│   └── article.pdf
└── media/               # Optional: audio/video
```

The `project.qde` XML contains:
- **Codes** — labels/categories (e.g., "Patient Experience", "Trust in Healthcare")
- **Coded segments** — mappings from text ranges to codes
- **Code hierarchy** — tree structure of codes
- **Memos/notes** — researcher annotations
- **Case structures** — groupings of sources

### Proprietary QDA Formats

| Tool | Extensions |
|------|-----------|
| NVivo | `.nvp`, `.nvpx` |
| MaxQDA | `.mx18`, `.mx22`, etc. |
| ATLAS.ti | `.atlproj`, `.hpr7` |
| Dedoose | Cloud-based, exports to `.qdpx` |

These are binary/proprietary — we download and store them but can't easily parse their contents.

### Processing Strategy

- **QDPX files**: Unzip → parse `project.qde` XML → extract codes, source list, coding density as metadata
- **Proprietary formats**: Download, store, record metadata from repository — don't attempt to parse
- **All QDA files**: Flag `is_qda_file = true` in metadata DB

### Realistic Expectations & Fallback

QDA files will be **rare** in open repositories — most researchers don't publish analysis files openly. Expected: fewer than 10 QDA files across all sources.

**Fallback strategy:**
1. Always search for QDA file extensions first in every source
2. Then broaden to qualitative data files (interview transcripts, research articles)
3. Document the scarcity as a "technical challenge with data" in the report — this is a valid finding
4. The qualitative data files we collect are still valuable for QDArchive

### Search Query List

Two-pass strategy: QDA-specific queries first (primary goal), then qualitative data queries (secondary). Deduplication across queries is handled automatically by the pipeline. These queries are stored in `queries.txt` for use with `pdm run pipeline scrape <source> -f queries.txt`.

**QDA-specific queries (first pass) — target QDA analysis files:**

| Query | Rationale | Reference |
|---|---|---|
| `qdpx` | REFI-QDA open interchange format — the standard QDA exchange format | [qdasoftware.org](https://www.qdasoftware.org/) (cited in project description slide 11) |
| `REFI-QDA` | Standard name for the QDA interchange format | [qdasoftware.org](https://www.qdasoftware.org/) (cited in project description slide 11) |
| `qualitative data analysis` | General QDA term — catches datasets mentioning analysis work | Project description slide 3: "QDA files = Structured data that capture the meaning in qualitative data" |
| `NVivo` | QDA software provider (proprietary project files: `.nvp`, `.nvpx`) | QDA File Extensions Formats - Overview.csv |
| `ATLAS.ti` | QDA software provider (proprietary project files: `.atlproj`) | QDA File Extensions Formats - Overview.csv |
| `MaxQDA` | QDA software provider (proprietary project files: `.mx18`–`.mx24`) | QDA File Extensions Formats - Overview.csv |
| `Dedoose` | QDA software provider (cloud-based, exports to `.qdpx`) | QDA File Extensions Formats - Overview.csv |
| `QDAcity` | QDA software provider (uses `.qdpx` format) | QDA File Extensions Formats - Overview.csv |
| `QDA Miner` | QDA software provider (proprietary project files: `.qda`) | QDA File Extensions Formats - Overview.csv |
| `CAQDAS` | Umbrella term: Computer-Assisted Qualitative Data Analysis Software | [Wikipedia](https://en.wikipedia.org/wiki/Computer-assisted_qualitative_data_analysis_software) (listed in QDA File Extensions CSV) |
| `coded transcript` | QDA output — transcripts with applied codes/annotations | Standard QDA terminology |

**Qualitative data queries (second pass) — target raw research data:**

| Query | Rationale | Reference |
|---|---|---|
| `interview transcript` | Core qualitative data type | Project description slide 3: "interview transcripts, research articles, audio/video" |
| `qualitative research` | Broad catch-all for qualitative studies | Project description slide 12 |
| `qualitative data` | Broad catch-all for qualitative datasets | Project description slide 3 |
| `focus group` | Common qualitative research method producing group discussion data | Standard qualitative methodology |
| `ethnography` | Qualitative research design producing observational/interview data | Standard qualitative methodology |
| `grounded theory` | Qualitative methodology — datasets often include coded data | Standard qualitative methodology |
| `semi-structured interview` | Most common qualitative interview format | Standard qualitative methodology |
| `thematic analysis` | Common qualitative analysis approach — datasets contain coded data | Standard qualitative methodology |
| `narrative analysis` | Qualitative analysis approach focusing on personal stories | Standard qualitative methodology |
| `case study research` | Qualitative research design producing in-depth study data | Standard qualitative methodology |
| `participant observation` | Qualitative data collection method producing field notes | Standard qualitative methodology |

---

## Project Structure

```
seeding-QDArchive/
├── pyproject.toml              # PDM project config, dependencies, CLI entry points
├── pdm.lock
├── src/
│   └── pipeline/
│       ├── __init__.py
│       ├── cli.py              # CLI entry point (click/typer)
│       ├── config.py           # Global config (paths, DB location, logging)
│       ├── db/
│       │   ├── __init__.py
│       │   ├── models.py       # SQLite schema (SQLAlchemy or raw)
│       │   ├── connection.py   # DB connection management
│       │   └── export.py       # CSV export logic
│       ├── connectors/
│       │   ├── __init__.py
│       │   ├── base.py         # Abstract base connector interface
│       │   ├── zenodo.py       # Zenodo REST API connector
│       │   ├── dryad.py        # Dryad REST API v2 connector
│       │   ├── dataverse.py    # Shared Dataverse connector (QDR, DANS, DataverseNO)
│       │   ├── ukdata.py       # UK Data Service scraper
│       │   ├── qualidatanet.py # QualidataNet scraper
│       │   └── qualiservice.py # Qualiservice metadata scraper
│       ├── storage/
│       │   ├── __init__.py
│       │   └── file_manager.py # Download, organize, and link files to metadata
│       └── utils/
│           ├── __init__.py
│           ├── logging.py      # Logging setup
│           └── license.py      # License detection and validation
├── data/                       # Downloaded files (gitignored)
│   └── {source}/{file_id}/     # Organized by source and ID
├── exports/                    # CSV exports
├── tests/
│   ├── __init__.py
│   ├── test_connectors/
│   ├── test_db/
│   └── test_storage/
├── master-plan.md
├── implementation-plan.md
├── open-questions.md
└── .gitignore
```

---

## Part 1: Data Acquisition

### Phase 1.1 — Project Scaffolding

1. **Initialize PDM project**
   - `pdm init` with Python 3.11+
   - Add core dependencies: `httpx` (HTTP client), `click` or `typer` (CLI), `sqlalchemy` (ORM), `beautifulsoup4` (scraping), `rich` (logging/progress)
   - Add dev dependencies: `pytest`, `ruff` (linting)
   - Configure CLI entry point in `pyproject.toml`

2. **Set up SQLite database schema**
   - Initial metadata fields:
     - `id` (auto, primary key)
     - `source_name` (e.g., "Zenodo", "Dryad")
     - `source_url` (link to the dataset/record page)
     - `download_url` (direct file download link)
     - `file_name`
     - `file_type` (qdpx, pdf, txt, docx, rtf, etc.)
     - `file_hash` (SHA-256 for deduplication)
     - `file_size_bytes`
     - `local_path` (relative path in data/ directory)
     - `license_type` (e.g., "CC BY 4.0")
     - `license_url`
     - `title`
     - `description`
     - `authors`
     - `date_published`
     - `tags` (comma-separated or JSON array)
     - `is_qda_file` (boolean)
     - `downloaded_at` (timestamp)
     - `notes`
   - Schema should be easy to extend (SQLAlchemy model or migration-friendly)

3. **CSV export utility**
   - Export full metadata table to CSV
   - Filterable by source, file type, license, etc.

4. **Logging setup**
   - Structured logging to console and file
   - Log every download attempt, success, failure, and skip

### Phase 1.2 — Base Connector Interface

Design an abstract base class that all connectors implement:

```python
class BaseConnector:
    name: str           # e.g., "zenodo"
    base_url: str

    def search(self, query: str, file_types: list[str]) -> Iterator[DatasetResult]:
        """Search the repository for qualitative data / QDA files."""
        ...

    def get_metadata(self, record_id: str) -> FileMetadata:
        """Fetch full metadata for a specific record."""
        ...

    def download(self, record_id: str, target_dir: Path) -> Path:
        """Download file(s) and return local path."""
        ...
```

This ensures every new source follows the same pattern. The CLI orchestrates connectors by name.

### Phase 1.3 — API Connectors (Priority: QDA files first)

Build in this order (most relevant source first, then by volume):

#### 1. Dataverse connector (`dataverse.py`) — covers 3 sources, QDR first

- **Shared API** for QDR (`data.qdr.syr.edu`), DANS, DataverseNO (`dataverse.no`)
- **API:** `GET /api/search?q=...&type=dataset`
- **Config:** Pass base URL per instance — one connector, three sources
- **Metadata:** Title, authors, description, dates, file info all via API
- **No auth required** for open datasets
- **Build and run against QDR first** — it's the only dedicated qualitative data repository, highest chance of QDA files
- Then run same connector against DANS and DataverseNO

#### 2. Zenodo connector (`zenodo.py`)

- **API:** `https://zenodo.org/api/records`
- **Search strategy:** Query for QDA file types (qdpx, REFI), qualitative research keywords
- **Endpoints:** `GET /api/records?q=...&type=dataset` → iterate results → download files
- **Metadata:** Rich — title, authors, license, DOI, dates, description all available via API
- **No auth required** for public records
- Largest volume source — broadest search after QDR

#### 3. Dryad connector (`dryad.py`)

- **API:** `https://datadryad.org/api/v2/search?q=...`
- **Metadata:** Title, authors, abstract, license, keywords
- **No auth required** for search and download

### Phase 1.4 — Web Scrapers (Secondary priority)

#### 4. UK Data Service / QualiBank scraper (`ukdata.py`)

- **No API for qualitative data** — scrape QualiBank web interface
- **Note:** Downloads may require registration — collect metadata even if files can't be downloaded
- Use `beautifulsoup4` + `httpx`

#### 5. QualidataNet scraper (`qualidatanet.py`)

- **Federated search portal** — scrape search results for metadata
- Actual files are hosted at partner institutions — follow links to originals

#### 6. Qualiservice scraper (`qualiservice.py`)

- **Metadata only** — actual data requires a contract
- Scrape publicly available metadata from QSearch portal

#### Skip: QualiBi

- No public-facing data portal found — not worth implementing unless discovered later

### Phase 1.5 — File Storage & Organization

- Download files to `data/{source_name}/{title-slug}-{record_id}/{filename}`
- Compute SHA-256 hash on download for deduplication
- Support resumable/incremental downloads — skip re-downloading if file hash already exists in DB
- Track `local_path` in metadata DB linking each record to its file

### Phase 1.6 — CLI Commands

```
pdm run pipeline search <source> [--query Q] [--file-type TYPE]   # Search a source
pdm run pipeline scrape <source|all> [--limit N]                  # Run acquisition
pdm run pipeline export [--format csv] [--output FILE]            # Export metadata
pdm run pipeline status                                           # Show collection stats
pdm run pipeline list-sources                                     # List available connectors
```

### Phase 1.7 — Operate & Collect

- Run all connectors with QDA-focused queries first
- Then broaden to general qualitative data queries
- Review and fix metadata gaps
- Generate Part 1 report: sources used, file counts by type, metadata completeness, licensing issues

### Phase 1.8 — Part 1 Deliverables

- `git tag part-1-release`
- CSV export of full metadata database
- `data/` folder with all downloaded files
- Technical challenges report (data challenges, not programming)

---

## Part 2: Data Classification

> Classifier approach (LLM vs rule-based vs ML) to be decided when Part 2 starts.

### Phase 2.1 — Database Merging

- Import other students' CSV databases into the SQLite DB
- Add CLI command: `pdm run pipeline import <csv_file> [--source STUDENT_NAME]`
- Deduplication:
  - Primary: match by `file_hash` (SHA-256)
  - Secondary: match by `source_url` + `file_name`
  - On conflict: keep the record with more complete metadata

### Phase 2.2 — ISIC Rev. 5 Taxonomy Setup

- Download or embed ISIC Rev. 5 hierarchy (sections → divisions)
- Store as a reference table in the database
- Each file gets: `isic_section`, `isic_division`, `classification_confidence`

### Phase 2.3 — Classifier Development

- Classifier reads file content (text extraction from PDF/DOCX/txt) + metadata
- Outputs: ISIC section, ISIC division, and suggested tags
- Approach TBD (LLM-based, rule-based, or trained ML)
- Add CLI command: `pdm run pipeline classify <record_id|all> [--dry-run]`

### Phase 2.4 — Tagging System

- Tags stored in metadata DB (JSON array or separate tags table)
- Tags describe: research method, topic area, data type
- Generated alongside ISIC classification

### Phase 2.5 — Statistical Reporting

- Add CLI command: `pdm run pipeline stats`
- Report includes:
  - Total files collected
  - Distribution by ISIC section and division
  - Distribution by file type
  - Tag frequency
  - Metadata completeness rates

### Phase 2.6 — Part 2 Deliverables

- `git tag part-2-release`
- Updated CSV export with classification columns
- Statistical reports
- Technical challenges report (data challenges)

---

## Development Workflow

```bash
pdm install                    # Install dependencies
pdm run pipeline --help        # See available commands
pdm run pytest                 # Run tests
pdm run ruff check src/        # Lint
pdm run ruff format src/       # Format
```

---

## Implementation Order & Timeline

Today is Feb 14. Part 1 due **March 15** (4 weeks). Part 2 due **April 12** (4 weeks after Part 1).

### Part 1: Data Acquisition (Feb 15 → March 15)

| Week | Dates | Steps | What |
|------|-------|-------|------|
| **Week 1** | Feb 15–21 | 1–3 | Project scaffolding, base connector, Dataverse/QDR connector |
| **Week 2** | Feb 22–28 | 4–7 | Evaluate QDR results, run DANS + DataverseNO, Zenodo connector, Dryad connector |
| **Week 3** | Mar 1–7 | 8–12 | File storage, CSV export, all scrapers (UK Data Service, QualidataNet, Qualiservice) |
| **Week 4** | Mar 8–14 | 13–14 | Run full acquisition across all sources, review data, write report, tag `part-1-release` |

**Coordination meetings:** Feb 20, Feb 26, Mar 6, Mar 13 (Fridays/Thursdays at 4pm CET)

### Part 2: Data Classification (Mar 16 → April 12)

| Week | Dates | Steps | What |
|------|-------|-------|------|
| **Week 5** | Mar 16–22 | 15 | Database merge + dedup tooling, import other students' databases |
| **Week 6** | Mar 23–29 | 16 | ISIC Rev. 5 taxonomy integration |
| **Week 7** | Mar 30–Apr 5 | 17–18 | Classifier development, tagging + stats reporting |
| **Week 8** | Apr 6–11 | 19 | Final run, review results, write report, tag `part-2-release` |

**Coordination meetings:** Mar 23, Mar 30, Apr 6 (Mondays at 4pm CET)

### Implementation Steps Reference

| Step | Done | What                                                | Est. Complexity | Notes |
| ---- | ---- | --------------------------------------------------- | --------------- | ----- |
| 1    | [x]  | Project scaffolding (PDM, DB, CLI skeleton)         | Low             | |
| 2    | [x]  | Base connector interface                            | Low             | |
| 3    | [x]  | Dataverse connector → run against QDR first         | Medium          | QDR is the highest-signal source (dedicated qualitative data repo); one connector reused for 3 Dataverse instances |
| 4    | [ ]  | Evaluate QDR results, validate pipeline works       | —               | |
| 5    | [ ]  | Run Dataverse connector against DANS + DataverseNO  | Low (reuse)     | Just add new entries with different base URLs |
| 6    | [ ]  | Zenodo API connector                                | Medium          | |
| 7    | [ ]  | Dryad API connector                                 | Medium          | |
| 8    | [ ]  | File storage manager                                | Low             | |
| 9    | [ ]  | CSV export                                          | Low             | |
| 10   | [ ]  | UK Data Service scraper                             | High            | |
| 11   | [ ]  | QualidataNet scraper                                | High            | |
| 12   | [ ]  | Qualiservice metadata scraper                       | Medium          | |
| 13   | [ ]  | Run full acquisition across all sources, review data| —               | |
| 14   | [ ]  | Part 1 report + tag release                         | Low             | |
| 15   | [ ]  | Database merge + dedup tooling                      | Medium          | |
| 16   | [ ]  | ISIC taxonomy integration                           | Medium          | |
| 17   | [ ]  | Classifier development                              | High            | |
| 18   | [ ]  | Tagging + stats reporting                           | Medium          | |
| 19   | [ ]  | Part 2 report + tag release                         | Low             | |
