# file: fetch_release_sneakernews.py
#
# Uses Playwright instead of requests (SneakerNews is behind Cloudflare).
# Extracts release date, shoe name, retail price, and product image.

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from fetch_release_multisource_common import (
    extract_image_url,
    extract_price_smart,
    infer_brand,
    normalize_text,
    parse_date_flexible,
    render_html,
    window_filter,
)

SOURCE_URL = "https://sneakernews.com/release-dates/"
SOURCE_NAME = "sneakernews"

# "March 05, 2026" or "Mar 5, 2026"
DATE_RE = re.compile(r"\b([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})\b")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch SneakerNews release dates (Playwright).")
    p.add_argument("--days",       type=int,  default=35)
    p.add_argument("--timeout-ms", type=int,  default=60000)
    p.add_argument("-o", "--output", type=Path, default=Path("data/fallback_sneakernews.json"))
    return p.parse_args()


def extract_rows(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []

    # SneakerNews wraps each release in an article/section with a date header
    # Walk every element that has a recognizable date text near a shoe title link
    for tag in soup.find_all(True):
        text = normalize_text(tag.get_text(" ", strip=True))
        if not text:
            continue

        m = DATE_RE.search(text)
        if not m:
            continue

        d = parse_date_flexible(m.group(0))
        if not d:
            continue

        # Find the product title link (h2 > a pattern common on SneakerNews)
        h2 = tag.find("h2")
        if not h2:
            continue

        a = h2.find("a", href=True)
        if not a:
            continue

        title = normalize_text(a.get_text(" ", strip=True))
        if not title or len(title) < 6:
            continue

        # Pull retail price from the surrounding block text
        parent = h2.parent or tag
        block_text = normalize_text(parent.get_text(" ", strip=True)) if parent else text
        retail = extract_price_smart(block_text)

        # Find the product image from the same block
        image_url = extract_image_url(parent, base_url="https://sneakernews.com")

        href = a["href"]
        if href.startswith("/"):
            href = "https://sneakernews.com" + href

        rows.append(
            {
                "releaseDate":          d.isoformat(),
                "shoeName":             title,
                "brand":                infer_brand(title),
                "retailPrice":          retail,
                "estimatedMarketValue": None,
                "imageUrl":             image_url,
                "sourcePrimary":        SOURCE_NAME,
                "sourceSecondary":      SOURCE_URL,
                "sourceUrl":            SOURCE_URL,
                "releaseUrl":           href,
            }
        )

    return rows


def dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r.get("releaseDate", ""), str(r.get("shoeName", "")).lower())
        if not key[0] or not key[1]:
            continue
        existing = best.get(key)
        if existing is None:
            best[key] = r
            continue
        # Keep the record with more data
        def score(x: dict[str, Any]) -> int:
            return int(bool(x.get("imageUrl"))) + int((x.get("retailPrice") or 0) > 0)
        if score(r) > score(existing):
            best[key] = r

    return sorted(
        best.values(),
        key=lambda x: (x["releaseDate"], x.get("brand", ""), x["shoeName"].lower()),
    )


def main() -> None:
    args = parse_args()

    html = render_html(SOURCE_URL, timeout_ms=args.timeout_ms)
    rows = dedupe(extract_rows(html))
    rows = window_filter(rows, days=args.days)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"{SOURCE_NAME} rows={len(rows)} output={args.output}")


if __name__ == "__main__":
    main()
