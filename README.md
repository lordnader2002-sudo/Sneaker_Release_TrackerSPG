# Sneaker Release Trackers (Auto-Updated)

This repo automatically generates **two Excel trackers** for sneaker releases:

- **Weekly Tracker** (next ~14 days)
- **Monthly Tracker** (next ~30–35 days)

✅ **Updated automatically twice per day**  
**12:00 PM** and **12:00 AM** Eastern (every day)

---

## Download the trackers

### Option 1 (recommended): One-click download page
If this repo has GitHub Pages enabled, use the website link in the repo (usually in the “About” section).  
From there you can download:

- `Weekly Tracker (Excel)`
- `Monthly Tracker (Excel)`

### Option 2: Download from the repo files
Go to the `output/` folder in this repo and download:

- `weekly_tracker.xlsx`
- `monthly_tracker.xlsx`

### Option 3 (backup): Download from Actions artifacts
1. Click the **Actions** tab
2. Open the most recent workflow run
3. Download the **artifact** called `sneaker-trackers`

---

## What’s inside the Excel tracker?

Each row is a sneaker release. The workbook includes helpful tabs:

- **Tracker** – main view (easy filtering/sorting)
- **Monthly** – longer lookahead
- **Changes** – what changed since the last run
- **High Hype** – only the most important releases
- **Summary** – quick totals/breakdowns
- **Raw Data** – full merged dataset

---

## How “Hype” is determined

**Hype** is a simple score that estimates demand/importance.

It uses signals like:
- popular brands/models (Nike, Jordan, Dunk, etc.)
- collaboration keywords (Travis Scott, Off-White, etc.)
- limited/exclusive wording
- resale spread (when available)

Hype labels:
- **LOW**
- **MED**
- **HIGH**

---

## How “Confidence” is determined

**Confidence** estimates how reliable the row’s data is.

It looks at:
- how many sources agreed on the release (**Source Count**)
- whether retail price was found
- whether a release URL exists
- whether an image URL exists

Confidence labels:
- **LOW**
- **MED**
- **HIGH**

---

## What “Source Count” means

**Source Count** = how many different sources matched that release.

Higher = more verified.

---

## Notes on accuracy

This tracker uses free public sources. Sometimes release dates shift or listings change.

Best practice:
- Check the **Changes** tab first
- Treat **HIGH confidence** rows as “most reliable”
- Treat **LOW confidence** rows as “needs quick review”

---

## For maintainers (optional)

The automation runs through GitHub Actions and produces:
- `output/weekly_tracker.xlsx`
- `output/monthly_tracker.xlsx`

If it ever fails:
- open **Actions → latest run → logs**
- identify which step failed (fetch → merge → build)

---

## Contact / Requests

If you want new sources, new columns, or a different layout, open an issue in this repo.
