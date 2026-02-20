# Database Documentation

The pipeline stores all file metadata in a single SQLite table (`files`). This document describes the schema, data sources per field, and available query filters.

Database file: `pipeline.db` (gitignored)

---

## Schema

Single table: **`files`**

### Primary Key

| Column | Type | Required | Description |
|---|---|---|---|
| `id` | INTEGER | auto | Auto-incrementing record ID |

### Source Information

| Column | Type | Required | Description |
|---|---|---|---|
| `source_name` | VARCHAR(100) | yes | Repository connector name (e.g. `qdr`, `dans`, `dataverseno`) |
| `source_url` | TEXT | yes | URL to the dataset page on the source repository |
| `download_url` | TEXT | yes | Direct download URL for the file |

### File Information

| Column | Type | Required | Description |
|---|---|---|---|
| `file_name` | VARCHAR(500) | yes | Original filename from the repository |
| `file_type` | VARCHAR(50) | no | File extension (e.g. `.pdf`, `.qdpx`, `.tab`) |
| `file_hash` | VARCHAR(64) | no | SHA-256 hash of the downloaded file (NULL if not downloaded) |
| `file_size_bytes` | INTEGER | no | File size in bytes |
| `local_path` | TEXT | no | Full local filesystem path (NULL for restricted/metadata-only records) |
| `local_directory` | TEXT | no | Directory name where the file is stored (e.g. `study-title-doi_10.5064_XYZ`) |

### License

| Column | Type | Required | Description |
|---|---|---|---|
| `license_type` | VARCHAR(100) | no | License name (e.g. `CC BY 4.0`, `CC0 1.0`, `Standard Access`) |
| `license_url` | TEXT | no | URL to the license text |

### Dataset Metadata

| Column | Type | Required | Description |
|---|---|---|---|
| `title` | TEXT | no | Dataset/study title |
| `description` | TEXT | no | Dataset description (HTML stripped) |
| `authors` | TEXT | no | Semicolon-separated author names |
| `date_published` | VARCHAR(50) | no | Publication/release date from the repository |
| `tags` | TEXT | no | Semicolon-separated subject tags |

### Extended Metadata (from API)

| Column | Type | Required | Description |
|---|---|---|---|
| `keywords` | TEXT | no | Semicolon-separated keywords from the repository API |
| `kind_of_data` | TEXT | no | Semicolon-separated data types (e.g. `interview transcripts; coded qualitative data`) |
| `language` | VARCHAR(100) | no | Semicolon-separated language(s) of the data |
| `content_type` | VARCHAR(200) | no | MIME type from the API (e.g. `application/x-zip-refiqda`) |
| `friendly_type` | VARCHAR(200) | no | Human-readable file type (e.g. `REFI-QDA-Project`, `Adobe PDF`) |
| `software` | TEXT | no | QDA software used (e.g. `NVivo 12`, `ATLAS.ti`) |
| `geographic_coverage` | TEXT | no | Semicolon-separated countries/regions |
| `restricted` | BOOLEAN | no | Whether the file is access-restricted on the source |
| `api_checksum` | VARCHAR(150) | no | Checksum from the API in `TYPE:VALUE` format (e.g. `SHA-512:abc...`) |

### Provenance

| Column | Type | Required | Description |
|---|---|---|---|
| `depositor` | TEXT | no | Person who deposited the data |
| `producer` | TEXT | no | Semicolon-separated producing organizations |
| `publication` | TEXT | no | Semicolon-separated related publication citations |
| `date_of_collection` | TEXT | no | Data collection period (e.g. `2022-01-01 to 2022-12-31`) |
| `time_period_covered` | TEXT | no | Time period the data covers (e.g. `2020-01-01 to 2022-06-30`) |

### Uploader / Contact

| Column | Type | Required | Description |
|---|---|---|---|
| `uploader_name` | TEXT | no | Contact person name from the repository |
| `uploader_email` | VARCHAR(200) | no | Contact person email |

### Classification

| Column | Type | Required | Description |
|---|---|---|---|
| `is_qda_file` | BOOLEAN | yes | Whether the file is a QDA project file (detected by extension + content type) |

### Timestamps

| Column | Type | Required | Description |
|---|---|---|---|
| `downloaded_at` | DATETIME | no | When the file was successfully downloaded (NULL if not downloaded) |
| `created_at` | DATETIME | yes | When the database record was created |

### Notes

| Column | Type | Required | Description |
|---|---|---|---|
| `notes` | TEXT | no | System notes (e.g. `access restricted`) |

---

## Field Availability by Source

Not all sources provide the same metadata. Fields left blank indicate the source does not supply that data.

| Field | QDR | DANS | DataverseNO |
|---|---|---|---|
| `keywords` | yes | yes | yes |
| `kind_of_data` | yes | no | sometimes |
| `language` | yes | yes | sometimes |
| `software` | yes | no | no |
| `geographic_coverage` | yes | no | no |
| `depositor` | no | no | yes |
| `producer` | no | no | yes |
| `publication` | no | no | yes |
| `date_of_collection` | no | no | sometimes |
| `time_period_covered` | no | no | sometimes |
| `uploader_name` | yes | yes | yes |
| `uploader_email` | yes | rarely | rarely |

---

## Query Filters

The `pipeline db` command supports the following filters. All filters can be combined.

| Filter | Type | Description |
|---|---|---|
| `--source` / `-s` | exact match | Filter by `source_name` (e.g. `qdr`, `dans`) |
| `--qda-only` | boolean | Only records where `is_qda_file = true` |
| `--restricted-only` | boolean | Only records where `restricted = true` |
| `--search` | substring | Case-insensitive search across `title`, `description`, `keywords`, `tags` |
| `--language` | substring | Case-insensitive match on `language` |
| `--software` | substring | Case-insensitive match on `software` |
| `--file-type` | exact match | Match on `file_type` (auto-prepends `.` if missing) |
| `--has-software` | boolean | Only records where `software IS NOT NULL` |
| `--has-keywords` | boolean | Only records where `keywords IS NOT NULL` |
| `--limit` / `-n` | integer | Max rows to display (default: 50) |

### Examples

```bash
# Browse first 50 records
pdm run pipeline db

# Show only QDA files from QDR
pdm run pipeline db -s qdr --qda-only

# Search for interview-related records
pdm run pipeline db --search "interview"

# English records that have software metadata
pdm run pipeline db --language english --has-software

# Restricted PDF files, up to 100
pdm run pipeline db --restricted-only --file-type pdf -n 100
```

---

## Deduplication Strategy

- **Primary:** `file_hash` (SHA-256) — identical file content is detected regardless of filename
- **Secondary:** `source_url` + `file_name` — prevents re-downloading the same file from the same dataset

---

## Schema Migration

The schema evolves without requiring a database reset. On startup, `init_db()` inspects existing columns and runs `ALTER TABLE ADD COLUMN` for any missing ones. New columns are always nullable so existing data is preserved.

---

## Export

`pipeline export` writes all columns to CSV. Column discovery is automatic via SQLAlchemy introspection, so new columns appear in exports without code changes.
