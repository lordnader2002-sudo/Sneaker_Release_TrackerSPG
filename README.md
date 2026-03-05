# Sneaker Release Tracker

Automated sneaker release tracking that pulls release data, merges multiple free sources, scores each release, tracks changes, and generates clean Excel files for weekly and monthly review.

## How scoring works

### Hype
Hype is an estimate of how important or in-demand a release may be.

It is based on things like:
- **brand strength** (Nike / Air Jordan usually score higher)
- **collab keywords** (like Travis Scott, Union, Off-White, etc.)
- **popular models** (like Jordan 1/3/4, Dunks, Air Max 95, Kobe)
- **retail vs resale spread** when resale data exists
- **limited / exclusive wording**

Hype is labeled as:
- **LOW**
- **MED**
- **HIGH**

### Confidence
Confidence is an estimate of how reliable the release data is.

It is based on things like:
- whether the release appeared in **more than one source**
- whether a **retail price** was found
- whether an **image URL** was found
- whether there is a **secondary source**
- whether there is a usable **release/source URL**

Confidence is labeled as:
- **LOW**
- **MED**
- **HIGH**

### Priority
Priority is the practical “how much should we care?” label.

It is based on **Hype + Confidence**:
- **Must Watch**
- **Watch**
- **Low Priority**

---

## What this project does

This repo is built to replace manual sneaker release spreadsheets with an automated workflow.

It:

- pulls sneaker release data automatically
- uses a **primary source** and a **fallback source**
- merges and cleans duplicate records
- scores each release for:
  - **Hype**
  - **Confidence**
  - **Priority**
- tracks changes from previous runs
- archives snapshots
- generates polished Excel files
- runs automatically with **GitHub Actions**

---

## Data flow

The workflow runs in this order:

1. **Primary fetcher**
   - `fetch_releases_primary.js`
   - Uses **Sneaks-API**
   - Pulls broad sneaker/product data

2. **Fallback fetcher**
   - `fetch_release_fallback.py`
   - Uses **Playwright**
   - Scrapes public release pages if the primary source is weak or fails

3. **Merge + compare**
   - `merge_and_compare.py`
   - Merges primary and fallback records
   - Deduplicates entries
   - Calculates hype, confidence, priority, and tags
   - Detects changes from the previous run
   - Writes archive snapshots

4. **Workbook builder**
   - `build_tracker_workbook.py`
   - Generates:
     - `output/weekly_tracker.xlsx`
     - `output/monthly_tracker.xlsx`

5. **GitHub Actions**
   - `.github/workflows/update_trackers.yml`
   - Runs the pipeline automatically on a schedule or manually
   - Uploads files as workflow artifacts
   - Commits updated outputs back into the repo

---

## Output files

After a successful run, the repo generates:

### Data files
- `data/primary_releases.json`
- `data/fallback_releases.json`
- `data/final_releases.json`
- `data/changes.json`

### Excel files
- `output/weekly_tracker.xlsx`
- `output/monthly_tracker.xlsx`

### Archive
- timestamped snapshots in `archive/`

---

## Workbook tabs

The generated workbook includes:

- **Tracker**
  - main weekly action sheet

- **Monthly**
  - broader release window

- **Changes**
  - new, removed, or updated releases

- **Raw Data**
  - normalized merged records

- **High Hype**
  - high-priority filtered view

- **Summary**
  - quick counts and breakdowns

---

## Repo structure

```text
.github/workflows/update_trackers.yml
.gitignore
README.md
package.json
requirements.txt

fetch_releases_primary.js
fetch_release_fallback.py
merge_and_compare.py
build_tracker_workbook.py

setup_local.bat
run_local_full.bat

data/
output/
archive/
