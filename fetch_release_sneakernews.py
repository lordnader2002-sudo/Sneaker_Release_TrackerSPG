# file: fetch_release_sneakernews.py

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from fetch_release_multisource_common import infer_brand, normalize_text, parse_date_flexible, window_filter

SOURCE_URL = "https://sneakernews.com/release-dates/"
SOURCE_NAME = "sneakernews"

DATE_RE = re.compile(r"\b([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})\b")  # March 05, 2026
RETAIL_RE = re.compile(r"Retail Price:\s*\$\s*([0-9]{2,4})", re.I)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch release calendar from SneakerNews (requests).")
    p.add_argument("--days", type=int, default=35)
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("-o", "--output", type=Path, default=Path("data/fallback_sneakernews.json"))
    return p.parse_args()


def fetch_html(url: str, timeout: int) -> str:
    r = requests.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    r.raise_for_status()
    return r.text


def extract_rows(soup: BeautifulSoup) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    # Find date blocks and then the next H2 with the sneaker title link
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

        h2 = tag.find_next("h2")
        if not h2:
            continue

        a = h2.find("a", href=True)
        if not a:
            continue

        title = normalize_text(a.get_text(" ", strip=True))
        if not title:
            continue

        block_text = normalize_text(h2.parent.get_text(" ", strip=True)) if h2.parent else ""
        m_price = RETAIL_RE.search(block_text)
        retail = int(m_price.group(1)) if m_price else 0

        href = a["href"]
        if href.startswith("/"):
            href = "https://sneakernews.com" + href

        rows.append(
            {
                "releaseDate": d.isoformat(),
                "shoeName": title,
                "brand": infer_brand(title),
                "retailPrice": retail,
                "estimatedMarketValue": None,
                "imageUrl": None,
                "sourcePrimary": SOURCE_NAME,
                "sourceSecondary": SOURCE_URL,
                "sourceUrl": SOURCE_URL,
                "releaseUrl": href,
            }
        )

    return rows


def dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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

    html = fetch_html(SOURCE_URL, timeout=args.timeout)
    soup = BeautifulSoup(html, "html.parser")

    rows = dedupe(extract_rows(soup))
    rows = window_filter(rows, days=args.days)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    print(f"{SOURCE_NAME} saved: {len(rows)} -> {args.output}")


if __name__ == "__main__":
    main()
