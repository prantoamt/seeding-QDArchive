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

---

## Part 2: Data Classification

### Database Merging
- How will other students' databases be shared for merging? Is there a common format or schema?

### ISIC Rev. 5 Classification
- Is there a reference mapping or example of qualitative data classified under ISIC that we can use as ground truth?

---

## General

- Should the git repository be **public or private**? Also — what license should the code use (MIT, Apache 2.0, GPL, etc.)?
