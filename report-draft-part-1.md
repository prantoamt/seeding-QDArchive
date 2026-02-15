# Technical Challenges Report — Part 1: Data Acquisition

## 1. Restricted File Access

A significant portion of datasets on QDR (Syracuse) contain files marked as **restricted** at the file level. The dataset metadata is publicly available, but individual files require access approval from the data owner. This is a per-file flag — within the same dataset, some files may be open while others are restricted.

**Impact:** Many qualitative data files (especially raw interview transcripts and QDA project files) are restricted, precisely because they contain sensitive participant data. This means the most valuable files for our purposes are often the least accessible. We catalog these as metadata-only records to preserve the information even when the file itself cannot be downloaded.

**Mitigation:** The pipeline detects the `restricted` flag from the API response *before* attempting a download, avoiding wasted HTTP 403 errors. Files that slip through (e.g., where the flag is missing but the server still returns 403) are caught and saved as metadata-only records with a note.

## 2. Non-Standard License Identifiers

Repositories do not consistently use standard license identifiers (CC BY 4.0, CC0, etc.). QDR, for example, labels most datasets as **"Standard Access"** rather than a recognized Creative Commons license. According to QDR's terms, documentation is under CC BY-SA 4.0 and data files are freely accessible to registered users — but this information is not machine-readable from the API response.

**Impact:** Automated license filtering must account for repository-specific labels. A strict filter that only accepts CC/CC0 identifiers would reject the majority of QDR datasets, even though they are effectively open. Conversely, being too lenient risks including datasets that are not truly open.

**Mitigation:** We treat "Standard Access" as open based on QDR's published terms, but this is a judgment call that may need professor confirmation. The license check is implemented as a configurable allowlist.

## 3. License Information in Unexpected API Fields

The Dataverse API provides license information in two different places: a structured `license` block (with `name` and `uri` fields) and a free-text `termsOfAccess` field. Some datasets populate one but not the other. QDR datasets frequently have no `license` block at all and instead put the access terms in `termsOfAccess`.

**Impact:** Without checking both fields, many datasets would appear to have no license and be incorrectly skipped.

**Mitigation:** The connector falls back to `termsOfAccess` when the `license` block is empty, using whichever field provides the access terms.

## 4. HTML Embedded in Metadata Text Fields

Dataset descriptions returned by the Dataverse API frequently contain raw HTML markup (`<p>`, `<br>`, `<em>`, etc.) rather than plain text. This is because the Dataverse web UI supports rich-text editing for descriptions.

**Impact:** Storing HTML-laden descriptions pollutes the database and makes text search unreliable (a search for "interview" might miss `<em>interview</em>`). It also makes CSV exports harder to read.

**Mitigation:** All description text is stripped of HTML tags and whitespace-normalized before storage.

## 5. Scarcity of QDA Project Files

Despite targeting a repository specifically focused on qualitative data (QDR), actual QDA project files (`.qdpx`, `.nvp`, `.mx`, etc.) are extremely rare. The vast majority of files are PDFs, Word documents, or plain text — the *raw data* rather than the structured analysis files that are the primary target.

**Impact:** Of the datasets scraped, only a small fraction contain QDA files. This is a fundamental scarcity problem: researchers rarely share their analysis project files, even when they share the underlying data.

**Mitigation:** QDA detection uses not only file extensions but also the Dataverse `friendly_type` (e.g., "REFI-QDA-Project") and `content_type` (e.g., "application/x-zip-refiqda") fields, catching QDA files that might have non-standard extensions. Additionally, search queries specifically target QDA software names (NVivo, ATLAS.ti, MaxQDA, Dedoose, QDAcity, QDA Miner, CAQDAS) to surface datasets that are more likely to include analysis files.

## 6. Inconsistent and Missing Metadata

Metadata completeness varies dramatically across datasets. Key fields like `language`, `software`, `keywords`, and `kind_of_data` are often absent. Some datasets have rich metadata with keywords, geographic coverage, and software used, while others have only a title and author.

**Impact:** Filtering and classification in Part 2 will be harder for records with sparse metadata. Language detection, for example, cannot rely solely on the `language` field — many English-language datasets simply don't populate it.

**Mitigation:** All extended metadata fields are captured when available but treated as optional. The `--has-keywords` and `--has-software` filters in the CLI help identify which records have rich metadata versus sparse records that may need content-based classification.

## 7. Network Reliability and Download Failures

The QDR API occasionally drops connections during file downloads, particularly for larger files. A single failed download would previously halt the entire scraping session.

**Impact:** Long-running batch scrapes (with dozens of queries and hundreds of files) would fail partway through, requiring manual restarts.

**Mitigation:** Downloads retry up to 3 times with exponential backoff (2s, 4s, 8s delays) on connection errors. The pipeline continues to the next file on permanent failures rather than aborting the entire run.

## 8. Duplicate Datasets Across Search Queries

When running batch scrapes with multiple search queries (e.g., "interview transcript", "qualitative research", "focus group"), the same datasets frequently appear in results for different queries. Without deduplication, the same files would be downloaded multiple times.

**Impact:** Wasted bandwidth, storage, and time. Also inflates record counts if not caught.

**Mitigation:** Two levels of deduplication: (1) during scraping, dataset URLs seen in earlier queries are skipped; (2) after download, SHA-256 hashes are compared against existing records, and duplicates are deleted from disk.
