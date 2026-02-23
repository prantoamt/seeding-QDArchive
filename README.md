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

See [`datasources.csv`](datasources.csv) for the full list of evaluated repositories (status, API details, licensing, and skip reasons). For a detailed walkthrough of the scraping algorithm — search, filter cascade, download, deduplication, and storage — see [`algorithm.md`](algorithm.md).

## Documentation

| Document | Description |
|----------|-------------|
| [`algorithm.md`](algorithm.md) | Scraping algorithm — search, filter cascade, download, deduplication |
| [`database.md`](database.md) | Database schema, field availability by source, query filters |
| [`datasources.csv`](datasources.csv) | Source of truth for all evaluated repositories |
| [`report-draft-part-1.md`](report-draft-part-1.md) | Technical challenges report — Part 1: Data Acquisition |

## Setup

### Prerequisites

- Python 3.10+
- [PDM](https://pdm-project.org) package manager

### Install

```bash
git clone git@github.com:prantoamt/seeding-QDArchive.git
cd seeding-QDArchive
pdm install
```

Verify the installation:

```bash
pdm run pipeline --help
```

## Usage

### Scrape all sources at once

This runs every configured source using the queries in `queries.txt`:

```bash
pdm run pipeline scrape-all
```

### Scrape a single source

Available sources: `qdr`, `dans`, `dataverseno`, `harvard`, `sodha`, `zenodo`, `ukds`

```bash
# Scrape QDR with all queries from queries.txt
pdm run pipeline scrape qdr -f queries.txt

# Scrape Zenodo with a single query, limit 5 datasets
pdm run pipeline scrape zenodo -q "qualitative interview" -n 5

# Scrape DANS with default query ("qualitative")
pdm run pipeline scrape dans
```

### Search without downloading

Preview what a source returns before scraping:

```bash
pdm run pipeline search qdr -q "interview transcript"
```

### Browse and inspect results

```bash
# Browse all records
pdm run pipeline db

# Filter by source
pdm run pipeline db -s qdr

# Show only QDA files
pdm run pipeline db --qda-only

# Search across title, description, keywords
pdm run pipeline db --search "interview"

# Filter by language, software, file type
pdm run pipeline db --language english --has-software
pdm run pipeline db --file-type pdf -n 100

# Show full details for specific records
pdm run pipeline show 6 49 50
```

See [`database.md`](database.md) for the full schema, field availability by source, and all query filters.

### Check progress

```bash
pdm run pipeline status
```

### Export

```bash
# Export to default location (exports/metadata.csv)
pdm run pipeline export

# Export to a custom path
pdm run pipeline export -o results/metadata.csv
```

### List available sources

```bash
pdm run pipeline list-sources
```

### Reset everything

Deletes the database, all downloaded data, exports, and logs:

```bash
pdm run pipeline reset
```

## Data layout

Downloaded files are stored as:

```
data/{source_name}/{title-slug}-{record_id}/{filename}
```

## Development

```bash
pdm run pytest               # Run tests
pdm run ruff check src/      # Lint
pdm run ruff format src/     # Format
```
