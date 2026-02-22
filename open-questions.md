# Open Questions

Questions to clarify with the professor or during coordination meetings.

---

## Part 1: Data Acquisition

### Licensing
- Are all Creative Commons variants acceptable (including CC BY-NC, CC BY-ND), or only fully open ones (CC BY, CC0)?
- Some repositories use custom access labels instead of standard license identifiers. Should we treat these as open? For example, QDR (Syracuse) labels datasets as **"Standard Access"** — their terms state documentation is CC BY-SA 4.0, and data files are freely accessible to registered users. We are currently treating "Standard Access" as open, but this is an assumption worth confirming.
  - **Example dataset:** "Interviews regarding data curation for qualitative data reuse and big qualitative data" — https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6UH4S0H
    - License shown in API: `"Standard Access"`
    - Actual terms: documentation under CC BY-SA 4.0, data files accessible without restrictions for registered QDR users
    - No standard CC/CC0 license identifier is provided — should we include or skip these?

### QualidataNet — Metadata Only, No File Downloads

QualidataNet (`qualidatanet.com`) is a federated metadata portal operated by KonsortSWD / RDC Qualiservice (University of Bremen). It aggregates ~143 records from 6 German research data centers via an open Elasticsearch API. However, **none of the partner institutions allow unrestricted file downloads** — all require registration, formal applications, or usage agreements:

| Data Center | Records | DOI Pattern | Access Requirement |
|---|---|---|---|
| FDZ Qualiservice (PANGAEA) | 58 | `doi.pangaea.de/10.1594/PANGAEA.*` | Login + signup required |
| FDZ Bildung | 47 | `doi.org/10.7477/*` | Registration + formal application |
| FDZ eLabour | 17 | `doi.org/10.60613/*` | Usage agreement/contract required |
| FDZ DZHW | 12 | `doi.org/10.21249/DZHW:*` | Scientific Use File application |
| QualiBi | 5 | `doi.org/10.25716/GUDE.*` | All rights reserved |
| FDZ-BO | 4 | `doi.org/10.7478/*` | JS-rendered portal, no direct access |

The `accessRestricted: false` field in QualidataNet's Elasticsearch API is misleading — it indicates metadata accessibility, not file downloadability.

**Current approach:** Save all 143 records as metadata-only (title, authors, description, keywords, license, DOI, data center). No file downloads attempted.

**Question for professor:** Should we attempt to register at individual data centers (e.g., FDZ Qualiservice via PANGAEA) to access files, or is metadata-only sufficient for QualidataNet?

---

## Part 2: Data Classification

### Database Merging
- How will other students' databases be shared for merging? Is there a common format or schema?
- **Proposal:** If all students agree on a common CSV column structure for Part 1, merging and deduplication in Part 2 becomes much simpler. Suggested minimum columns:
  ```
  source_name, source_url, download_url, file_name, file_type, file_hash,
  file_size_bytes, license_type, title, description, authors, date_published,
  tags, is_qda_file
  ```
  - `file_hash` (SHA-256) is critical — it's the primary dedup key
  - `source_url` + `file_name` serves as secondary dedup key
  - Without a shared schema, each student's export will need custom parsing and field mapping before merging
- Should we raise this at the next coordination meeting so everyone aligns early?

### ISIC Rev. 5 Classification
- Is there a reference mapping or example of qualitative data classified under ISIC that we can use as ground truth?

---

## General

- Should the git repository be **public or private**? Also — what license should the code use (MIT, Apache 2.0, GPL, etc.)?
