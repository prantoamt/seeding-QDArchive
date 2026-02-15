# Open Questions

Questions to clarify with the professor or during coordination meetings.

---

## Part 1: Data Acquisition

### Data Scope
- What counts as "qualitative research data" beyond interview transcripts and articles in scope of a QDA file? Are field notes, focus group transcripts, or ethnographic observations in scope?
- Should audio/video files be downloaded, or only text-based formats? If yes, are there file size limits?
- The slides mention a spreadsheet of known QDA file types/extensions — is there a more complete version beyond the providers listed in the CSV?

### Pipeline & Storage
- What database should be used for metadata storage (SQLite, PostgreSQL, etc.), or is CSV-only sufficient for Part 1?
- Is there a preferred directory structure for organizing downloaded files (e.g., by source, by file type, by license)?
- Should the pipeline support incremental/resumable downloads, or is a one-shot run acceptable?
- Are there any storage size constraints or hosting expectations for the downloaded data?

### Metadata
- What are the minimum required metadata fields vs. nice-to-have fields?
- How should we handle files where the license is ambiguous (e.g., stated on the repository page but not in the file itself)?
- Should metadata include the download URL for re-acquisition, or just the source repository link?

### Licensing
- Are all Creative Commons variants acceptable (including CC BY-NC, CC BY-ND), or only fully open ones (CC BY, CC0)?
- How should government open data licenses (e.g., Open Government Licence) be treated?

---

## Part 2: Data Classification

### Database Merging
- How will other students' databases be shared for merging? Is there a common format or schema?
- What is the deduplication strategy — exact file hash, fuzzy metadata matching, or both?
- If two students collected the same file with different metadata, which metadata record takes priority?

### ISIC Rev. 5 Classification
- Is the classifier expected to use an LLM, rule-based logic, or a trained ML model?
- Should classification be fully automated, or is a human-in-the-loop review acceptable?
- How should files that don't fit any ISIC division be handled (e.g., an "unclassified" category)?
- Is there a reference mapping or example of qualitative data classified under ISIC that we can use as ground truth?

### Tags
- Are tags free-form, or should they come from a controlled vocabulary?
- Should tags describe the research method (e.g., "interview", "grounded theory"), the topic, or both?

---

## General

- Is there a preferred programming language or tech stack, or is this left to each student?
- Will the professor provide access to QDArchive's API or data model for Part 3 compatibility?
- Should the reports (technical challenges) be a separate document or part of the README/repo?
- Should the git repository be **public or private**? The professor runs a Professorship for Open-Source Software, but no guidance was given. Also — what license should the code use (MIT, Apache 2.0, GPL, etc.)?
