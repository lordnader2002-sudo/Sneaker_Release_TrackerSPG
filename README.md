# Sneaker Release Tracker

Auto-updated twice daily. Scrapes 6+ sources, deduplicates, scores, and publishes a live web dashboard plus downloadable Excel workbooks.

**Updated:** 12:00 PM and 12:00 AM Eastern, every day.

---

## Live Dashboard

The web dashboard is available at the GitHub Pages link in the repo's "About" section. It includes:

- Live search by name, brand, or tags
- Filter by Hype level, Confidence level, Priority, and tags
- Card and table view
- Sortable columns (date, hype, price)
- Recent changes panel

---

## Download the Trackers

**Option 1 — Repo files** (simplest): go to the `output/` folder and download `weekly_tracker.xlsx` or `monthly_tracker.xlsx`.

**Option 2 — Actions artifact**: click **Actions** → latest run → download `sneaker-trackers`.

---

## Scoring Explained

### Hype

Hype estimates how much demand a release is expected to generate. It starts as a point total and converts to a label.

**Brand tier**

| Brand | Points |
|---|---|
| Air Jordan, Nike, Yeezy, New Balance, Adidas | +12 |
| Vans, Converse, Puma, Reebok, Saucony, Hoka, Salomon, ASICS | +7 |
| Everything else | +3 |

**Name/model signals**

| Signal | Points |
|---|---|
| Collab keyword in name (Travis Scott, Off-White, Sacai, NOCTA, Palace, Kith, A Ma Maniere, 50+ others) | +20 |
| Limited/exclusive keyword (raffle, lottery, friends & family, sample…) | +10 |
| Hot model (Jordan 1/3/4/5/6/11/12, Dunk, Kobe 6/8, Air Max 95/97, Samba, NB 550/990/2002…) | +12 |
| Retro / OG / heritage keyword | +4 |
| Unusual retail price (≥ $250 or ≤ $110) | +3 |

**Resale market signal** (when eBay data is available)

| Resale vs. Retail | Points |
|---|---|
| 2.5× or more | +35 |
| 2.0–2.5× | +28 |
| 1.5–2.0× | +18 |
| 1.2–1.5× | +8 |
| Spread ≥ $200 | +15 |
| Spread ≥ $150 | +10 |
| Spread ≥ $75 | +6 |

**Thresholds:** ≥ 42 points = **HIGH** · ≥ 20 points = **MED** · below 20 = **LOW**

---

### Confidence

Confidence measures how reliable and well-sourced the data is — not how hyped the shoe is.

| Factor | Points |
|---|---|
| Has a primary source name | +25 |
| Has a secondary source | +15 |
| Retail price found | +15 |
| Image found | +10 |
| Release URL found | +10 |
| Confirmed by 3+ independent scrapers | +28 |
| Confirmed by 2 scrapers | +18 |

**Thresholds:** ≥ 60 points = **HIGH** · ≥ 32 points = **MED** · below 32 = **LOW**

A shoe can be HIGH confidence with a LOW hype score (solid data on a regular release) or LOW confidence with a HIGH hype score (exciting release, only one source has it yet).

---

### Priority / Watch

Priority is a simple 2×2 matrix of Hype × Confidence:

| Hype | Confidence | Priority |
|---|---|---|
| HIGH | HIGH | **Must Watch** |
| HIGH | MED | **Watch** |
| MED | HIGH | **Watch** |
| Anything else | Anything else | Low Priority |

"Must Watch" means the release is both highly anticipated AND well-verified across multiple sources. "Watch" means one of those conditions is met but not both. "Low Priority" covers everything else — worth scanning but doesn't need immediate attention.

---

### Market Value

`estimatedMarketValue` is populated by searching eBay completed (sold) listings for each shoe. It uses the median sale price of DS (deadstock/new) listings. If fewer than 3 sold results are found for the exact shoe, it searches by model only (e.g., "Nike Dunk Low") as a fallback. If still too few results, the field is left blank.

This gives a real-market proxy for resale potential even before a shoe has officially dropped, by looking at what similar colorways or past versions of the same model are actually trading for.

---

## What the Tracker Columns Mean

| Column | Description |
|---|---|
| Date | Release date |
| Retail | Retail price (from labeled sources or product pages) |
| Market Value | eBay median sold price for similar DS pairs |
| Hype | LOW / MED / HIGH — demand estimate |
| Hype Score | Raw numeric score behind the Hype label |
| Confidence | LOW / MED / HIGH — data reliability |
| Conf. Score | Raw numeric score behind the Confidence label |
| Priority | Must Watch / Watch / Low Priority |
| Brand | Inferred from shoe name if not provided by source |
| Style | Shoe name as scraped |
| Tags | Auto-applied: collab, hot-model, retro, running, basketball, exclusive, kids, etc. |
| Source Primary | Which scraper first found this release |
| Source Count | How many independent scrapers agreed on this release |
| Release URL | Link to a source page for the shoe |

---

## Data Sources

| Source | Type |
|---|---|
| GOAT (primary) | Playwright + API interception |
| Nike.com | JSON extraction from page scripts |
| Foot Locker | Playwright scraper |
| Hibbett | Playwright scraper |
| KicksOnFire | Playwright scraper |
| Sole Collector | Playwright scraper |
| SneakerNews | Playwright scraper |
| StockX (enrichment) | Authenticated sold-listing price scraper (primary market value source) |
| eBay (enrichment) | Sold-listing price scraper (fills StockX gaps) |

Releases seen on multiple sources get a higher Confidence score. Prices are only extracted when explicitly labeled on a page (to avoid placeholder values); the eBay enrichment step fills in market values separately.

---

## Notes on Accuracy

- Release dates occasionally shift; check the **Changes** tab for recent updates.
- Treat HIGH confidence rows as most reliable. LOW confidence rows are worth a quick manual check before acting on them.
- Retail prices may be missing for upcoming releases where retailers haven't published prices yet — this is expected.
- Market values reflect recent eBay activity for similar pairs, not a guaranteed future resale price.

---

## Troubleshooting

If the workflow fails: **Actions → latest run → logs** and look at which step failed (fetch → merge → build workbooks → enrich → deploy).

To manually trigger a refresh: **Actions → Update Sneaker Trackers → Run workflow**.
