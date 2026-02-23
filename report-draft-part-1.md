# Technical Challenges Report — Part 1: Data Acquisition

**Project:** Seeding QDArchive (SQ26, 10 ECTS)
**Author:** [Your Name]
**Date:** February 2026
**Supervisor:** Prof. Dirk Riehle, FAU Erlangen-Nürnberg

---

## Executive Summary

The goal of Part 1 was to find, download, and catalog as much open qualitative research data as possible from the web — with a priority on QDA (Qualitative Data Analysis) project files. An automated pipeline was built to search twenty-one data repositories across fourteen countries, download files with open licenses, and store rich metadata in a SQLite database.

**Key results:**

| Metric | Value | Description |
|---|---|---|
| Total metadata records | 50,057 | All file records in the database (downloaded + metadata-only) |
| Files downloaded | 11,335 (17.21 GB) | Files successfully saved to disk with open licenses |
| QDA files found | 83 (across 9 formats) | Structured analysis files (qdpx, nvp, mx20, atlproj, etc.) |
| QDA files downloaded | 73 | QDA files accessible and saved; 10 were restricted |
| Restricted (metadata only) | 7,517 | Files requiring access approval; metadata preserved |
| Unique datasets | 3,086 | Distinct repository records (each may contain multiple files) |
| Data sources implemented | 21 | Repositories with automated connectors |
| Data sources evaluated | 40 | Total repositories investigated for qualitative data |
| Qualitative relevance rate | 95.8% | Files whose metadata contains qualitative keywords; the remaining 4.2% are mostly non-English datasets or primary sources without keyword matches, not necessarily irrelevant |
| Duplicate files (by SHA-256) | 0 | No identical files across sources or queries |

All figures in this report can be reproduced by running `pdm run pipeline stats`.

---

## Data Sources Overview

Twenty-one repositories were implemented with automated connectors across fourteen countries. A further 19 sources were evaluated and documented but skipped due to access restrictions, anti-bot protections, or lack of downloadable data (see `datasources.csv` for the full list).

| Source | API Type | Country | Total Records | Downloaded | QDA | Size (GB) | Datasets |
|---|---|---|---|---|---|---|---|
| Harvard Dataverse | Dataverse API | USA | 16,288 | 3,684 | 8 | 4.17 | 447 |
| DANS | Dataverse API | Netherlands | 10,833 | 2,614 | 19 | 6.81 | 375 |
| QDR (Syracuse) | Dataverse API | USA | 4,724 | 552 | 8 | 0.19 | 133 |
| KU Leuven | Dataverse API | Belgium | 3,224 | 244 | 0 | 0.07 | 43 |
| DataverseNO | Dataverse API | Norway | 3,153 | 1,099 | 1 | 2.97 | 111 |
| Dryad | Custom REST API v2 | USA | 3,137 | 0 | 0 | <0.01 | 430 |
| Zenodo (CERN) | Custom REST API | International | 2,639 | 1,665 | 42 | 2.24 | 692 |
| UK Data Service | EPrints JSON API | UK | 2,605 | 872 | 4 | 0.24 | 705 |
| DaRUS (Stuttgart) | Dataverse API | Germany | 1,838 | 141 | 0 | 0.02 | 25 |
| HeiDATA (Heidelberg) | Dataverse API | Germany | 368 | 166 | 0 | 0.09 | 27 |
| PUCP (Peru) | Dataverse API | Peru | 269 | 4 | 0 | <0.01 | 6 |
| RSU (Latvia) | Dataverse API | Latvia | 236 | 26 | 0 | <0.01 | 23 |
| SODHA | Dataverse API | Belgium | 141 | 141 | 0 | 0.14 | 1 |
| bonndata (Bonn) | Dataverse API | Germany | 139 | 23 | 1 | 0.01 | 14 |
| NYCU (Taiwan) | Dataverse API | Taiwan | 128 | 19 | 0 | 0.08 | 29 |
| UVA (Virginia) | Dataverse API | USA | 116 | 4 | 0 | <0.01 | 7 |
| CROSSDA (Croatia) | Dataverse API | Croatia | 89 | 2 | 0 | <0.01 | 2 |
| ACSS | Dataverse API | MENA | 82 | 50 | 0 | 0.18 | 8 |
| DataverseLV (Latvia) | Dataverse API | Latvia | 48 | 29 | 0 | 0.01 | 8 |

Eighteen of the twenty-one sources are Dataverse-based, requiring zero new connector code — only a base URL and source name. The top 7 sources (Harvard, DANS, Zenodo, DataverseNO, UKDS, QDR, KU Leuven) account for 94.7% of all downloads. The remaining 14 smaller sources collectively contributed 605 additional files (~560 MB). Dryad yielded 3,137 metadata records across 430 datasets but zero downloaded files: most datasets lacked qualitative relevance, and the few qualitative files (e.g., `.txt` transcripts) returned HTTP 401 Unauthorized — file downloads require authentication despite the API being publicly accessible for search and metadata. Rate limiting (HTTP 429) also caused some metadata fetches to fail.

---

## Technical Challenges with Data

### 1. Restricted File Access

A significant portion of datasets contain files marked as **restricted** at the file level. The dataset metadata is publicly available, but individual files require access approval from the data owner. This is a per-file flag — within the same dataset, some files may be open while others are restricted.

Of 50,057 total records, 7,517 (15.0%) are restricted. QDR has 4,084 restricted files out of 4,724 total (86.5%). The UK Data Service shows a similar pattern: 897 restricted out of 2,605 records (34.4%). Many qualitative data files — especially raw interview transcripts and QDA project files — are restricted precisely because they contain sensitive participant data. The most valuable files for our purposes are often the least accessible.

**Mitigation:** The pipeline detects the `restricted` flag from the API response *before* attempting a download, avoiding wasted HTTP 403 errors. Restricted files are saved as metadata-only records to preserve the information for Part 2 classification.

### 2. Non-Standard License Identifiers

Repositories do not consistently use standard license identifiers (CC BY 4.0, CC0, etc.). QDR labels most datasets as **"Standard Access"** rather than a recognized Creative Commons license. According to QDR's terms, documentation is under CC BY-SA 4.0 and data files are freely accessible to registered users — but this information is not machine-readable from the API response. The UK Data Service uses EPrints-specific identifiers like `cc_by` and `cc_by_sa` rather than SPDX codes.

A strict filter accepting only CC/CC0 identifiers would reject 4,687 QDR datasets (effectively all of QDR) and all UK Data Service records. Conversely, being too lenient risks including datasets that are not truly open.

**Mitigation:** QDR's "Standard Access" is treated as open based on their published terms. The UK Data Service connector includes a license mapping table that translates EPrints identifiers to standard SPDX codes.

### 3. License Information in Unexpected API Fields

The Dataverse API provides license information in two different places: a structured `license` block (with `name` and `uri` fields) and a free-text `termsOfAccess` field. Some datasets populate one but not the other. QDR datasets frequently have no `license` block at all and instead put access terms in `termsOfAccess`.

**Mitigation:** The connector checks both fields and falls back to `termsOfAccess` when the `license` block is empty.

### 4. HTML Embedded in Metadata Text Fields

Dataset descriptions returned by the Dataverse API frequently contain raw HTML markup (`<p>`, `<br>`, `<em>`, etc.) rather than plain text, because the Dataverse web UI supports rich-text editing for descriptions.

Storing HTML-laden descriptions pollutes the database and makes text search unreliable — a search for "interview" might miss `<em>interview</em>`.

**Mitigation:** All description text is stripped of HTML tags and whitespace-normalized before storage.

### 5. Scarcity of QDA Project Files

Despite targeting twenty-one repositories including one dedicated to qualitative data (QDR), actual QDA project files are extremely rare. Only 83 out of 50,057 records (0.17%) are QDA files. The vast majority of shared data consists of PDFs, Word documents, and plain text — the *raw data* rather than the structured analysis files. Researchers rarely share their analysis project files, even when they share the underlying data.

**Mitigation:** QDA detection uses file extensions *plus* Dataverse-specific fields (`friendly_type` = "REFI-QDA-Project", `content_type` = "application/x-zip-refiqda") to catch QDA files with non-standard extensions. Search queries specifically target QDA software names (NVivo, ATLAS.ti, MaxQDA, Dedoose, QDAcity, QDA Miner, CAQDAS). Despite the rarity, 73 QDA files were successfully downloaded across 6 different QDA tools and 9 file formats (qdpx, nvp, nvpx, mx, mx20, mx22, mx24, atlproj, qdc).

### 6. Inconsistent and Missing Metadata Across Repositories

Metadata completeness varies dramatically across sources. Core fields are near-universal, but extended fields critical for classification are often absent:

| Field | Coverage |
|---|---|
| Description | 100.0% |
| License | 100.0% |
| Keywords | 84.8% |
| Language | 46.1% |
| Kind of data | 34.2% |
| Geographic coverage | 14.1% |
| Software used | 5.4% |

Different repositories also use different metadata schemas. Dataverse installations share a common API structure, but the UK Data Service (EPrints) uses entirely different field names (`data_kind` instead of `kindOfData`, `country` instead of `geographicCoverage`).

**Mitigation:** All extended metadata fields are captured when available but treated as optional. The connector layer normalizes field names into a common schema. Part 2 classification will need content-based analysis rather than relying solely on metadata.

### 7. Non-Standard Data Types in API Responses

The UK Data Service EPrints JSON API occasionally returns unexpected data types in metadata fields. The `keywords` field, which is normally a list of strings, sometimes contains integers.

This caused the pipeline to crash mid-scrape with `AttributeError: 'int' object has no attribute 'lower'` when attempting to filter keywords for qualitative relevance. The entire UK Data Service scrape aborted and had to retry, losing progress.

**Mitigation:** All keyword values are coerced to strings upon ingestion. A broader lesson: never trust API responses to contain the advertised data types.

### 8. Malformed Dates in Repository Metadata

The UK Data Service returns dates in non-standard formats. Instead of ISO 8601 `YYYY-MM-DD`, some records have dates like `0005-01-2019` (DD-MM-YYYY with zero-padded day) or `2019-01-0005` (YYYY-MM-DD with an oversized four-digit day).

**Mitigation:** A date normalization function detects and corrects both malformed patterns by examining which numeric parts are plausible years vs. days.

### 9. Filtering Qualitative Data from General Repositories

General-purpose repositories (Harvard, Zenodo, DataverseNO) contain predominantly quantitative data. Search queries for terms like "qualitative research" return many datasets where the word "qualitative" appears in the description but the actual data is quantitative (e.g., survey statistics, gene expression data, climate measurements).

**Mitigation:** A two-stage filtering approach: (1) search queries are designed to prioritize qualitative methods and QDA tools; (2) a qualitative relevance filter checks each dataset's description and keywords against a multilingual keyword set (114 terms across 7 languages) before downloading. Datasets with QDA files bypass this filter entirely. The result is a 95.8% qualitative relevance rate in downloaded data across all 21 sources, based on keyword matching against metadata (title, description, keywords, kind of data) in 7 languages. The remaining 4.2% without a keyword match are not necessarily irrelevant — they are predominantly non-English datasets whose metadata uses terms outside the keyword list, or primary source documents (surveys, policy documents, legal databases) that are legitimate qualitative raw material but lack explicit qualitative terminology.

### 10. Duplicate Datasets Across Search Queries

When running batch scrapes with 35 search queries across multiple languages (English, Dutch, Norwegian, German, Spanish, French, Portuguese), the same datasets frequently appear in results for different queries. A dataset about "qualitative interview transcripts" would match queries for "qualitative research", "interview transcript", and "transcript".

**Mitigation:** Two-level deduplication: (1) during scraping, dataset URLs seen in earlier queries are skipped within each source; (2) after download, SHA-256 file hashes identify exact duplicates. The current database has zero duplicate files by hash across all 11,335 downloaded files from 21 sources, confirming the deduplication works effectively.

### 11. Anti-Bot Protections Blocking Repository Access

Several repositories employ anti-bot measures or authentication requirements that block automated file access:
- **Dryad** — the search and metadata API is publicly accessible, but file downloads return HTTP 401 Unauthorized, requiring authentication that is not documented in the public API. The API also enforces aggressive rate limiting (HTTP 429) on metadata fetches. All 3,137 records are metadata-only.
- **ADA Australia** uses Anubis, a proof-of-work anti-bot system, that blocks the Dataverse API entirely despite having a dedicated Qualitative sub-archive
- **StoryCorps** returns HTTP 403 on any programmatic access

**Mitigation:** These sources were documented. Dryad metadata was preserved for Part 2 classification despite the download restriction. Manual download could be attempted for high-value datasets, but this does not scale.

### 12. EPrints Document ID Mismatch (UK Data Service)

The UK Data Service EPrints API includes a `placement` field in document metadata that appears to be a file sequence number. However, the actual download URL uses the internal `document_id` from the URI field, not the `placement` value.

Using the wrong ID in download URLs results in 404 errors or downloading the wrong file entirely.

**Mitigation:** Download URLs are constructed from the document URI (`/id/document/{doc_id}`) rather than the placement field.

### 13. Network Reliability and Download Failures

Multiple repositories occasionally drop connections during file downloads. QDR and Harvard Dataverse showed frequent connection resets (`[Errno 54] Connection reset by peer`), particularly for larger files. Long-running batch scrapes with thousands of files would fail partway through.

**Mitigation:** Downloads retry up to 3 times with exponential backoff (2s, 4s, 8s delays). The pipeline continues to the next file on permanent failures rather than aborting the entire run. A source-level retry mechanism also retries entire failed sources.
