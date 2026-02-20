# Data Sources

Comprehensive reference of all qualitative data repositories discovered and evaluated for the Seeding QDArchive project.

---

## Summary

| # | Source | Type | Connector | Status | Files Downloadable? |
|---|--------|------|-----------|--------|---------------------|
| 1 | [QDR (Syracuse)](#1-qdr-syracuse) | Dataverse API | `qdr` | Implemented | Yes (open + restricted) |
| 2 | [DANS](#2-dans) | Dataverse API | `dans` | Implemented | Yes |
| 3 | [DataverseNO](#3-dataverseno) | Dataverse API | `dataverseno` | Implemented | Yes |
| 4 | [Zenodo](#4-zenodo) | REST API | `zenodo` | Implemented | Yes |
| 5 | [Dryad](#5-dryad) | REST API v2 | `dryad` | Implemented (metadata-only) | No (AWS WAF blocks downloads) |
| 6 | [UK Data Service](#6-uk-data-service) | EPrints JSON API | `ukds` | Implemented | Yes (public files only) |
| 7 | [QualidataNet](#7-qualidatanet) | Elasticsearch API | `qualidatanet` | Implemented (metadata-only) | No (partner registration required) |
| 8 | [Qualiservice](#8-qualiservice) | Scraper (planned) | `qualiservice` | Planned | No (formal contract required) |
| 9 | [QualiBi](#9-qualibi) | — | — | Skipped | No public portal |

---

## Detailed Source Profiles

### 1. QDR (Syracuse)

- **Full name:** Qualitative Data Repository
- **URL:** https://data.qdr.syr.edu
- **API URL:** `https://data.qdr.syr.edu/api/`
- **API type:** Dataverse (Harvard IQSS open-source platform)
- **API docs:** https://guides.dataverse.org/en/latest/api/index.html
- **Auth for search:** None
- **Auth for download:** None for open files; restricted files return 403
- **Connector class:** `DataverseConnector("https://data.qdr.syr.edu", "qdr")`
- **Data directory:** `data/qdr/`

**Description:** The only dedicated qualitative data repository in our list. Hosted at Syracuse University. Highest priority source — most likely to contain QDA files and qualitative research data.

**Licensing:** Uses "Standard Access" label instead of standard CC identifiers. Documentation is CC BY-SA 4.0. Data files are freely accessible to registered users. We treat "Standard Access" as open (see open-questions.md).

**Key findings:**
- Contains QDPX files (confirmed: `Micatka_AtlastiCoding.qdpx`)
- Some files restricted (return 403) — saved as metadata-only
- Rich metadata via Dataverse citation block: title, authors, description, keywords, license, DOI, dates, geographic coverage, kind of data, software, depositor, producer

---

### 2. DANS

- **Full name:** Data Archiving and Networked Services (DANS-KNAW)
- **URL:** https://ssh.datastations.nl
- **API URL:** `https://ssh.datastations.nl/api/`
- **API type:** Dataverse (migrated from EASY archive)
- **API docs:** https://guides.dataverse.org/en/latest/api/index.html
- **Auth for search:** None
- **Auth for download:** None (open data)
- **Connector class:** `DataverseConnector("https://ssh.datastations.nl", "dans")`
- **Data directory:** `data/dans/`

**Description:** Dutch national research data archive. Part of the SSH (Social Sciences and Humanities) Datastation. Migrated from the legacy EASY archive to Dataverse.

**Key findings:**
- Same Dataverse API as QDR — reuses the same connector class
- Open access to files
- Contains qualitative research data and interview materials

---

### 3. DataverseNO

- **Full name:** DataverseNO (Norwegian research data archive)
- **URL:** https://dataverse.no
- **API URL:** `https://dataverse.no/api/`
- **API type:** Dataverse
- **API docs:** https://guides.dataverse.org/en/latest/api/index.html
- **Auth for search:** None
- **Auth for download:** None (open data)
- **Connector class:** `DataverseConnector("https://dataverse.no", "dataverseno")`
- **Data directory:** `data/dataverse-no/`

**Description:** Norwegian national research data archive running Dataverse.

**Key findings:**
- Same Dataverse API — reuses connector
- Smaller volume of qualitative data than QDR or DANS

---

### 4. Zenodo

- **Full name:** Zenodo (CERN Open Research Repository)
- **URL:** https://zenodo.org
- **API URL:** `https://zenodo.org/api/`
- **API type:** Custom REST API
- **API docs:** https://developers.zenodo.org/
- **Auth for search:** None
- **Auth for download:** None (public records)
- **Connector class:** `ZenodoConnector()`
- **Data directory:** `data/zenodo/`
- **Key endpoint:** `GET /api/records?q=...&type=dataset`

**Description:** CERN-hosted open research repository. Largest general-purpose open data repository. Accepts any research output. Broad coverage but low density of qualitative data.

**Key findings:**
- Very large catalog — most results are quantitative, not qualitative
- Rich metadata: title, authors, license (SPDX), DOI, dates, description, keywords
- Files freely downloadable for public records
- Pagination via `page` + `size` params, max 10,000 results

---

### 5. Dryad

- **Full name:** Dryad Digital Repository
- **URL:** https://datadryad.org
- **API URL:** `https://datadryad.org/api/v2/`
- **API type:** Custom REST API v2
- **API docs:** https://datadryad.org/api/v2/docs
- **Auth for search:** None
- **Auth for download:** None documented, but blocked by AWS WAF
- **Connector class:** `DryadConnector()`
- **Data directory:** `data/dryad/`
- **Key endpoint:** `GET /api/v2/search?q=...`
- **Rate limit:** 100 requests/hour for downloads (`ratelimit-limit: 100` header)

**Description:** Digital data repository focused on research data underlying scientific publications. All published data is CC0-licensed.

**Download issue — AWS WAF bot protection:**
- Search and metadata retrieval work fine via the API
- File download URLs (`/stash/downloads/file_stream/{id}`) are protected by AWS WAF
- Automated downloads via httpx receive HTTP 403 (no User-Agent) or 202 with JavaScript challenge (`x-amzn-waf-action: challenge`)
- Cannot be solved with httpx — would require a headless browser (Playwright)

**Current status:** Metadata-only. All files saved as restricted. A future enhancement (step 12b in implementation plan) may use Playwright to bypass the WAF.

**Key findings:**
- Files require a second API call: dataset → version ID → `GET /versions/{id}/files`
- DOI-based record lookup: DOI must be URL-encoded in API paths
- All data is CC0 and public (in theory), but downloads are blocked
- HAL-style pagination (`_links.next.href`, `per_page=100`)

---

### 6. UK Data Service

- **Full name:** UK Data Service — ReShare Repository
- **URL:** https://reshare.ukdataservice.ac.uk
- **API URL:** `https://reshare.ukdataservice.ac.uk/cgi/`
- **API type:** EPrints JSON export (not a scraper — structured JSON endpoint)
- **API docs:** No official API docs; endpoint discovered by inspecting the ReShare export feature
- **Auth for search:** None
- **Auth for download:** None for public files; restricted files have `security: "staffonly"` or `security: "validuser"`
- **Connector class:** `UKDataServiceConnector()`
- **Data directory:** `data/uk-data-service/`
- **Key endpoints:**
  - Search: `GET /cgi/search/simple/export_reshare_JSON.js?output=JSON&q=...`
  - Single record: `GET /cgi/export/eprint/{id}/JSON/reshare-eprint-{id}.js`
  - File download: `GET /id/document/{doc_id}` (303 redirects to actual file)

**Description:** UK national data archive. The ReShare repository is the self-deposit arm of the UK Data Service, built on EPrints software. Contains qualitative research data including interview transcripts, video, audio, and documentation.

**Technical details:**
- EPrints JSON export returns all search results in one response (no pagination needed)
- The `placement` field in the JSON does NOT match the URL path number — download URLs must use the document URI (`/id/document/{doc_id}`) which EPrints 303-redirects to the correct file path
- License mapping from EPrints identifiers (`cc_by`, `cc_by_sa`, `ukda_eul`, etc.) to standard CC format
- Auto-generated EPrints files filtered out: `lightbox.jpg`, `preview.jpg`, `medium.jpg`, `small.jpg`, `indexcodes.txt`
- Files with `ukda_eul` license or non-public security marked as restricted

**Key findings:**
- Originally planned as a web scraper, discovered to have JSON API
- Large files (video/audio can be 300+ MB each)
- Rich metadata: keywords, language, kind of data, geographic coverage, funders, collection dates, contact details

---

### 7. QualidataNet

- **Full name:** QualidataNet (KonsortSWD / RDC Qualiservice)
- **URL:** https://www.qualidatanet.com (NOT `qualidata.net` — that is a parked domain)
- **API URL:** `https://www.qualidatanet.com/es/qualidatanet/dataset/_search`
- **API type:** Elasticsearch (public, no auth)
- **Auth for search:** None
- **Auth for download:** N/A (metadata-only portal)
- **Connector class:** `QualidataNetConnector()`
- **Data directory:** `data/qualidata-net/` (unused — no files to download)
- **Total records:** ~143

**Description:** Federated search portal for qualitative research data, operated by KonsortSWD / RDC Qualiservice at the University of Bremen. Aggregates metadata from 6 German research data centers. Does NOT host any files — all data lives at partner institutions.

**Partner data centers:**

| Partner | Records | DOI Pattern | Access Requirement |
|---------|---------|-------------|-------------------|
| FDZ Qualiservice (via PANGAEA) | 58 | `doi.pangaea.de/10.1594/PANGAEA.*` | Login + signup required |
| FDZ Bildung | 47 | `doi.org/10.7477/*` | Registration + formal application |
| FDZ eLabour | 17 | `doi.org/10.60613/*` | Usage agreement/contract required |
| FDZ DZHW | 12 | `doi.org/10.21249/DZHW:*` | Scientific Use File application |
| QualiBi | 5 | `doi.org/10.25716/GUDE.*` | All rights reserved |
| FDZ-BO | 4 | `doi.org/10.7478/*` | JS-rendered portal, no direct access |

**Key findings:**
- The `accessRestricted: false` field in the API is misleading — it means metadata is accessible, not that files are downloadable
- No partner institution offers unrestricted file downloads
- German research data centers are legally required to control access to qualitative data (personal information in interview transcripts)
- Metadata is bilingual (German/English)
- Built on Contao CMS with Elasticsearch backend
- `metadatalink` field is an analyzed text field (not keyword), so ES `term` queries fail — connector uses cache from search results

**Current status:** Metadata-only. All 143 records saved with title, authors, description, keywords, license, DOI, and data center name.

---

### 8. Qualiservice

- **Full name:** Qualiservice — Research Data Center for Qualitative Social Science Data
- **URL:** https://www.qualiservice.org
- **API type:** Web scraper (planned)
- **Auth for search:** None (public search portal)
- **Auth for download:** Formal contract/usage agreement required
- **Connector class:** Not yet implemented
- **Status:** Planned (step 12 in implementation plan)

**Description:** German qualitative data service based at the University of Bremen. Operates the QSearch portal for browsing qualitative datasets. Data access requires a formal contract.

**Key findings:**
- Qualiservice's data is also indexed in QualidataNet (58 records via PANGAEA)
- Actual file access requires user signup and a formal "Qualiservice License" agreement
- Metadata scraping from the QSearch portal is feasible
- May overlap significantly with QualidataNet records — deduplication needed

---

### 9. QualiBi

- **Full name:** QualiBi — Qualitative Data in Educational Research
- **URL:** No dedicated public portal
- **Status:** Skipped
- **Reason:** No public-facing data portal found. QualiBi contributes 5 records to the QualidataNet federated search but has no independent search or download interface. Records are hosted on GUDE (Goethe University Frankfurt DSpace) with "All rights reserved" licensing.
