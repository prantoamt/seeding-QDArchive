# CLI Manual

Full reference for the `pipeline` command-line tool.

```
pdm run pipeline <command> [options]
```

---

## Commands Overview

| Command | Description |
|---|---|
| [`search`](#pipeline-search) | Search a data source for qualitative data |
| [`scrape`](#pipeline-scrape) | Download data from a source |
| [`db`](#pipeline-db) | Browse the metadata database |
| [`show`](#pipeline-show) | Inspect full record details |
| [`status`](#pipeline-status) | Collection progress |
| [`list-sources`](#pipeline-list-sources) | Available connectors |
| [`export`](#pipeline-export) | Export metadata to CSV |

---

## `pipeline search`

Search a repository for qualitative data and display results in a table.

```
pipeline search <source> [options]
```

### Options

| Option | Short | Default | Description |
|---|---|---|---|
| `--query` | `-q` | `"qualitative"` | Search query string |
| `--file-type` | `-t` | | Filter by file type extension |

### Examples

```bash
# Search QDR for qualitative interview data
pdm run pipeline search qdr -q "interview transcript"

# Search for QDA-specific files
pdm run pipeline search qdr -q "REFI-QDA"

# Search with file type filter
pdm run pipeline search qdr -q "qualitative" -t qdpx
```

---

## `pipeline scrape`

Search, fetch metadata, check licenses, and download files from a data source.

**What it does for each dataset:**
1. Searches the source with the given query
2. Fetches full metadata (title, authors, license, file list)
3. Checks if the license is open — skips if not
4. Downloads each file to `data/{source}/{title-slug}-{record_id}/{filename}`
5. Computes SHA-256 hash for deduplication
6. Saves the record to the SQLite database
7. Files returning 403 (restricted) are saved as metadata-only records

```
pipeline scrape <source> [options]
```

### Options

| Option | Short | Default | Description |
|---|---|---|---|
| `--query` | `-q` | `"qualitative"` | Single search query |
| `--queries-file` | `-f` | | Text file with one query per line |
| `--limit` | `-n` | all | Max datasets to process per query |

> **Note:** `--query` and `--queries-file` are mutually exclusive. If neither is provided, defaults to `"qualitative"`. Lines starting with `#` in the queries file are ignored.

### Examples

```bash
# Single query, limit to 5 datasets
pdm run pipeline scrape qdr -q "qualitative" -n 5

# Run all queries from a file
pdm run pipeline scrape qdr -f queries.txt

# Run all queries, max 20 datasets per query
pdm run pipeline scrape qdr -f queries.txt -n 20
```

### Queries File Format

Create a text file with one search query per line. Comments (`#`) and blank lines are ignored.

```
# queries.txt

# QDA-specific queries (first pass)
qdpx
REFI-QDA
qualitative data analysis
NVivo
ATLAS.ti
MaxQDA
coded transcript

# Qualitative data queries (second pass)
interview transcript
qualitative research
qualitative data
focus group
ethnography
grounded theory
semi-structured interview
thematic analysis
```

### Output Explained

```
[1/5] Interview Dataset Title
  file transcript_01.pdf (125217 bytes)          # Downloaded successfully
  QDA analysis.qdpx (restricted — metadata saved) # 403, metadata saved to DB
  Duplicate (hash match): codebook.pdf            # Already in DB, skipped

Done. Queries: 1, Downloaded: 12, Restricted (metadata only): 8, Skipped (license): 2
```

---

## `pipeline db`

Browse the metadata database in a summary table.

```
pipeline db [options]
```

For the full database schema, available query filters, and field availability by source, see [`database.md`](database.md).

### Options

| Option | Short | Default | Description |
|---|---|---|---|
| `--source` | `-s` | all | Filter by source name |
| `--qda-only` | | `false` | Show only QDA files |
| `--restricted-only` | | `false` | Show only restricted files (uses `restricted` column) |
| `--search` | | | Case-insensitive substring search across title, description, keywords, tags |
| `--language` | | | Filter by language (case-insensitive substring match) |
| `--software` | | | Filter by software (case-insensitive substring match) |
| `--file-type` | | | Filter by file type extension (e.g. `.pdf`). Auto-prepends `.` if missing |
| `--has-software` | | `false` | Show only records that have software info |
| `--has-keywords` | | `false` | Show only records that have keywords |
| `--limit` | `-n` | `50` | Max rows to display |

### Examples

```bash
pdm run pipeline db
pdm run pipeline db -s qdr --qda-only
pdm run pipeline db --search "interview"
pdm run pipeline db --language english --has-software
pdm run pipeline db --restricted-only --file-type pdf -n 100
```

---

## `pipeline show`

Display all metadata fields for one or more records. Shows a formatted panel with every column from the database schema (see [`database.md`](database.md) for column descriptions).

```
pipeline show <id> [id ...]
```

### Examples

```bash
pdm run pipeline show 6
pdm run pipeline show 6 49 50
pdm run pipeline show 1 2 3 4 5
```

---

## `pipeline status`

Show a summary of the collection progress: total records, QDA file count, downloaded file count, restricted count, and breakdowns by source, language, software, file type, and license. Each breakdown row includes columns for **Total**, **QDA**, **Downloaded**, and **Restricted**.

```bash
pdm run pipeline status
```

### Example Output

```
Total records:    25411
QDA files:        8
Downloaded files: 11319
Restricted:      14092

By source:
                          Total    QDA     Down    Restr
                   qdr    25411      8    11319    14092

By language:
                                         Total    QDA     Down    Restr
                              English    16154      7    10937     5217
  English; French; Spanish, Castilian      940      0        2      938
                      English; French      676      0       25      651

By software:
                                         Total    QDA     Down    Restr
                                NVivo      124      2       34       90
  Taguette; GitHub; GitLab; Git; Zoom       73      2       27       46
            NVivo (QSR International)       71      1       10       61

By file type:
                          Total    QDA     Down    Restr
                  .pdf    21050      0    10983    10067
                  .txt     2192      0      176     2016
                  .tab      605      0       59      546

By license:
                                         Total    QDA     Down    Restr
                      Standard Access    14879      8      789    14090
                            CC BY 4.0    10523      0    10523        0
                              CC0 1.0        7      0        5        2
```

---

## `pipeline list-sources`

List all configured (ready) and planned data source connectors.

```bash
pdm run pipeline list-sources
```

### Example Output

```
Available sources:

  qdr             qdr                                           ready
  zenodo          Zenodo API                                    planned
  dryad           Dryad API                                     planned
  dans            DANS Dataverse                                planned
  dataverseno     DataverseNO                                   planned
```

---

## `pipeline export`

Export the full metadata database to a CSV file.

```
pipeline export [options]
```

### Options

| Option | Short | Default | Description |
|---|---|---|---|
| `--format` | | `csv` | Export format |
| `--output` | `-o` | `exports/metadata.csv` | Output file path |

All database columns are included in the export. See [`database.md`](database.md) for the full column list.

### Examples

```bash
# Export to default location
pdm run pipeline export

# Export to custom path
pdm run pipeline export -o results/metadata.csv
```
