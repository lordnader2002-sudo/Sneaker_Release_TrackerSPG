# file: fetch_release_hibbett.py

from __future__ import annotations

import argparse
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from fetch_release_multisource_common import (
    clean_title,
    extract_image_url,
    find_card_price,
    infer_brand,
    normalize_text,
    parse_date_flexible,
    purge_placeholder_images,
    render_html,
    window_filter,
)

SOURCE_URL = "https://www.hibbett.com/launch-calendar/"
SOURCE_NAME = "hibbett"

DATE_RE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|June|July|August|September|October|November|December)\b\.?\s+(\d{1,2})(?:,\s*(\d{4}))?\b",
    re.I,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch release calendar from Hibbett (Playwright).")
    p.add_argument("--days", type=int, default=35)
    p.add_argument("--timeout-ms", type=int, default=60000)
    p.add_argument("-o", "--output", type=Path, default=Path("data/fallback_hibbett.json"))
    return p.parse_args()


def extract_rows(soup: BeautifulSoup) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    default_year = date.today().year

    for a in soup.find_all("a", href=True):
        title = clean_title(normalize_text(a.get_text(" ", strip=True)))
        if len(title) < 8:
            continue

        container = a.parent
        blob = ""
        for _ in range(6):
            if container is None:
                break
            blob = normalize_text(container.get_text(" ", strip=True))
            if DATE_RE.search(blob):
                break
            container = container.parent

        m = DATE_RE.search(blob)
        if not m:
            continue

        month = m.group(1)
        day = m.group(2)
        year = m.group(3)
        date_text = f"{month} {day} {year}" if year else f"{month} {day}"

        d = parse_date_flexible(date_text, default_year=default_year)
        if not d:
            continue

        # Labeled-only price from the immediate parent — avoids placeholder $130 pollution
        retail = find_card_price(container)

        href = a["href"]
        if href.startswith("/"):
            href = "https://www.hibbett.com" + href

        rows.append(
            {
                "releaseDate": d.isoformat(),
                "shoeName": title,
                "brand": infer_brand(title),
                "retailPrice": retail,
                "estimatedMarketValue": None,
                "imageUrl": extract_image_url(container, base_url="https://www.hibbett.com"),
                "sourcePrimary": SOURCE_NAME,
                "sourceSecondary": SOURCE_URL,
                "sourceUrl": SOURCE_URL,
                "releaseUrl": href,
            }
        )

    return rows


def dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    purge_placeholder_images(rows)
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r.get("releaseDate", ""), str(r.get("shoeName", "")).lower())
        if not key[0] or not key[1]:
            continue
        if key not in best or (r.get("retailPrice") or 0) > (best[key].get("retailPrice") or 0):
            best[key] = r
    return sorted(best.values(), key=lambda x: (x["releaseDate"], x.get("brand", ""), x["shoeName"].lower()))


def main() -> None:
    args = parse_args()
    html = render_html(SOURCE_URL, timeout_ms=args.timeout_ms)
    soup = BeautifulSoup(html, "html.parser")

    rows = window_filter(dedupe(extract_rows(soup)), days=args.days)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"{SOURCE_NAME} saved: {len(rows)} -> {args.output}")


if __name__ == "__main__":
    main()
