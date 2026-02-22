<div align="center">

# Seeding QDArchive

**Acquire, catalog, and classify open qualitative research data for the QDArchive.**

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![PDM](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm-project.org)
[![Code style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000?logo=ruff&logoColor=white)](https://docs.astral.sh/ruff/)

---

_Academic project (10 ECTS) — FAU Erlangen-Nurnberg_

</div>

## Overview

This pipeline searches open qualitative data repositories, downloads files and metadata, stores everything in a local SQLite database, and exports results for further analysis. It targets **QDA project files** (REFI/qdpx, MAXQDA, NVivo, ATLAS.ti, Dedoose, QDAcity, QDA Miner) and **general qualitative data** (interview transcripts, research articles).

The project has two parts:

|       | Phase               | Deliverable                      | Deadline |
| ----- | ------------------- | -------------------------------- | -------- |
| **1** | Data Acquisition    | Metadata DB + downloaded files   | March 15 |
| **2** | Data Classification | Merged DB with ISIC Rev. 5 codes | April 12 |

## Data Sources

| Source                                                 | Type    | API URL                                    | Coverage                                          |
| ------------------------------------------------------ | ------- | ------------------------------------------ | ------------------------------------------------- |
| [Zenodo](https://zenodo.org)                           | API     | `https://zenodo.org/api/`                  | Open research repository                          |
| [QDR](https://data.qdr.syr.edu)                        | API     | `https://data.qdr.syr.edu/api/`            | Qualitative Data Repository (Syracuse)            |
| [DANS](https://ssh.datastations.nl)                    | API     | `https://ssh.datastations.nl/api/`         | Dutch national research data archive              |
| [DataverseNO](https://dataverse.no)                    | API     | `https://dataverse.no/api/`                | Norwegian research data archive                   |
| [UK Data Service](https://reshare.ukdataservice.ac.uk) | API     | `https://reshare.ukdataservice.ac.uk/cgi/` | UK national data archive (ReShare EPrints)        |
| [QualidataNet](https://www.qualidatanet.com)           | API     | `https://www.qualidatanet.com/es/`         | Federated qualitative data portal (metadata-only) |
| [Qualiservice](https://qualiservice.org)               | Scraper | `https://www.qualiservice.org/`            | German qualitative data service                   |

## Quick Start

### Prerequisites

- Python 3.10+
- [PDM](https://pdm-project.org) package manager

### Installation

```bash
git clone git@github.com:prantoamt/seeding-QDArchive.git && cd seeding-QDArchive
pdm install
```

### Usage

```bash
pdm run pipeline search qdr -q "qualitative"         # Search QDR
pdm run pipeline scrape qdr -f queries.txt           # Scrape with query file
pdm run pipeline db --qda-only                       # Browse QDA files
pdm run pipeline show 6 49 50                        # Inspect records
pdm run pipeline status                              # Collection progress
pdm run pipeline export                              # Export to CSV
```

> **Full CLI documentation:** See [manual.md](manual.md) for all commands, options, and examples.

## Project Structure

```
src/pipeline/
├── cli.py                  # Click CLI entry point
├── config.py               # Paths, constants, QDA extensions
├── connectors/
│   ├── base.py             # Abstract BaseConnector interface
│   └── dataverse.py        # Dataverse API connector (QDR, DANS, DataverseNO)
├── db/
│   ├── models.py           # SQLAlchemy models (files table)
│   ├── connection.py       # DB engine and session management
│   └── export.py           # CSV export
├── storage/
│   └── file_manager.py     # File organization + SHA-256 hashing
└── utils/
    ├── logging.py           # Rich console + file logging
    └── license.py           # Open license validation
```

**Data layout on disk:**

```
data/{source_name}/{title-slug}-{record_id}/{filename}
```

## Development

```bash
pdm run pytest               # Run tests
pdm run ruff check src/      # Lint
pdm run ruff format src/     # Format
```

## Tech Stack

| Tool                                                             | Role                             |
| ---------------------------------------------------------------- | -------------------------------- |
| [httpx](https://www.python-httpx.org/)                           | HTTP client for API calls        |
| [Click](https://click.palletsprojects.com/)                      | CLI framework                    |
| [SQLAlchemy](https://www.sqlalchemy.org/)                        | ORM for SQLite metadata DB       |
| [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/) | HTML scraping                    |
| [Rich](https://rich.readthedocs.io/)                             | Terminal formatting and progress |
| [Ruff](https://docs.astral.sh/ruff/)                             | Linting and formatting           |
| [pytest](https://docs.pytest.org/)                               | Testing                          |
