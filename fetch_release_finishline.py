# file: fetch_release_finishline.py
#
# Scrapes the Finish Line launch calendar (US retailer, USD prices).
# URL: https://www.finishline.com/store/launch-calendar
#
# Strategy:
#   1. Try extracting from embedded __NEXT_DATA__ / window.__STATE__ JSON blobs
#      (Finish Line is a Next.js app and often embeds the catalogue there).
#   2. Fall back to BeautifulSoup card-walking if no JSON blob found.
#
# Finish Line is US-only, so all prices are USD and releases are US market.

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
    extract_retail_price,
    find_card_price,
    infer_brand,
    infer_release_method,
    normalize_text,
    parse_date_flexible,
    purge_placeholder_images,
    render_html,
    window_filter,
)

SOURCE_URL  = "https://www.finishline.com/store/launch-calendar"
SOURCE_NAME = "finishline"
BASE_URL    = "https://www.finishline.com"

# Matches "Mar 15", "March 15", "2026-03-15", etc. that appear in card text
_DATE_RE = re.compile(
    r"\b(?:"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,\s*\d{4})?"
    r"|\d{4}-\d{2}-\d{2}"
    r")\b",
    re.I,
)

# Price-like: "$120" or "$120.00"
_PRICE_RE = re.compile(r"\$\s*([0-9]{2,4})(?:\.[0-9]{2})?")


def _extract_from_json_blob(html: str) -> list[dict[str, Any]]:
    """Try to pull release data from Next.js __NEXT_DATA__ or similar JSON blobs."""
    rows: list[dict[str, Any]] = []
    default_year = date.today().year

    # Common blob patterns
    patterns = [
        re.compile(r"<script[^>]*id=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>", re.S | re.I),
        re.compile(r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});", re.S),
        re.compile(r"window\.__STATE__\s*=\s*(\{.*?\});", re.S),
    ]
    blobs: list[str] = []
    for pat in patterns:
        m = pat.search(html)
        if m:
            blobs.append(m.group(1))

    def _walk(obj: Any, depth: int = 0) -> None:
        """Recursively walk JSON looking for launch/release objects."""
        if depth > 12 or not isinstance(obj, (dict, list)):
            return
        if isinstance(obj, list):
            for item in obj:
                _walk(item, depth + 1)
            return

        # Detect a release-like dict: must have a name/title and either a date or price
        name = obj.get("name") or obj.get("title") or obj.get("productName") or obj.get("displayName") or ""
        raw_date = (
            obj.get("launchDate") or obj.get("releaseDate") or obj.get("startDate")
            or obj.get("date") or obj.get("launchDateFormatted") or ""
        )
        raw_price = obj.get("retailPrice") or obj.get("price") or obj.get("originalPrice") or obj.get("msrp") or 0
        img = (
            obj.get("imageUrl") or obj.get("image") or obj.get("heroImage")
            or obj.get("thumbnail") or obj.get("primaryImageUrl") or ""
        )
        url = obj.get("url") or obj.get("productUrl") or obj.get("pdpUrl") or obj.get("href") or ""
        sku = obj.get("sku") or obj.get("styleId") or obj.get("productId") or ""

        if name and (raw_date or raw_price):
            d = parse_date_flexible(str(raw_date), default_year=default_year) if raw_date else None
            try:
                price = int(round(float(str(raw_price).replace("$", "").replace(",", "").strip()))) if raw_price else 0
            except (ValueError, TypeError):
                price = 0
            if price < 40 or price > 700:
                price = 0

            if img and isinstance(img, str) and img.startswith("/"):
                img = BASE_URL + img
            if url and isinstance(url, str) and url.startswith("/"):
                url = BASE_URL + url

            title = clean_title(normalize_text(str(name)))
            if title and d:
                rows.append({
                    "releaseDate": d.isoformat(),
                    "shoeName": title,
                    "brand": infer_brand(title),
                    "retailPrice": price,
                    "estimatedMarketValue": None,
                    "imageUrl": img if isinstance(img, str) else None,
                    "sourcePrimary": SOURCE_NAME,
                    "sourceSecondary": SOURCE_URL,
                    "sourceUrl": SOURCE_URL,
                    "releaseUrl": url if isinstance(url, str) else "",
                    "releaseMethod": infer_release_method(title),
                })

        for v in obj.values():
            _walk(v, depth + 1)

    for blob in blobs:
        try:
            data = json.loads(blob)
            _walk(data)
        except (json.JSONDecodeError, ValueError):
            pass

    return rows


def _extract_from_html(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """Walk anchor tags and card containers as a fallback."""
    rows: list[dict[str, Any]] = []
    default_year = date.today().year
    seen: set[tuple[str, str]] = set()

    # Finish Line cards are typically <article> or <div> containers
    # Try card-based extraction first
    card_selectors = [
        {"name": re.compile(r"product.?card|launch.?card|release.?card", re.I)},
        {"class": re.compile(r"product.?card|launch.?card|release.?card|tile", re.I)},
    ]
    cards: list[Any] = []
    for sel in card_selectors:
        found = soup.find_all(attrs=sel)
        if found:
            cards = found
            break

    # If no explicit card containers, fall back to anchor walking
    if not cards:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href:
                continue
            # Finish Line product URLs typically contain /product/ or /p/
            if not re.search(r"/product/|/p/|/launch/", href, re.I):
                continue

            raw = normalize_text(a.get_text(" ", strip=True))
            if not raw or len(raw) < 5:
                continue

            # Look for a date in the anchor text or parent container
            container = a
            for _ in range(5):
                text = normalize_text(container.get_text(" ", strip=True)) if container else ""
                if _DATE_RE.search(text):
                    break
                container = container.parent if container else None

            if container is None:
                continue

            ctx = normalize_text(container.get_text(" ", strip=True))
            date_m = _DATE_RE.search(ctx)
            if not date_m:
                continue

            d = parse_date_flexible(date_m.group(0), default_year=default_year)
            if not d:
                continue

            title = clean_title(raw)
            if not title:
                continue

            key = (d.isoformat(), title.lower())
            if key in seen:
                continue
            seen.add(key)

            retail = find_card_price(a.parent) if a.parent else extract_retail_price(ctx[:300])

            full_href = href
            if full_href.startswith("/"):
                full_href = BASE_URL + full_href

            rows.append({
                "releaseDate": d.isoformat(),
                "shoeName": title,
                "brand": infer_brand(title),
                "retailPrice": retail,
                "estimatedMarketValue": None,
                "imageUrl": extract_image_url(container, base_url=BASE_URL),
                "sourcePrimary": SOURCE_NAME,
                "sourceSecondary": SOURCE_URL,
                "sourceUrl": SOURCE_URL,
                "releaseUrl": full_href,
                "releaseMethod": infer_release_method(title),
            })
        return rows

    # Card-based extraction
    for card in cards:
        # Date
        date_tag = card.find(class_=re.compile(r"date|launch|release", re.I))
        ctx = normalize_text(card.get_text(" ", strip=True))
        raw_date = normalize_text(date_tag.get_text(" ", strip=True)) if date_tag else ""
        date_m = _DATE_RE.search(raw_date or ctx)
        if not date_m:
            continue
        d = parse_date_flexible(date_m.group(0), default_year=default_year)
        if not d:
            continue

        # Title
        name_tag = card.find(class_=re.compile(r"name|title|product", re.I))
        title_raw = normalize_text(name_tag.get_text(" ", strip=True)) if name_tag else ""
        if not title_raw:
            # Try heading tags
            for htag in ("h2", "h3", "h4", "p"):
                candidate = card.find(htag)
                if candidate:
                    title_raw = normalize_text(candidate.get_text(" ", strip=True))
                    break
        title = clean_title(title_raw)
        if not title:
            continue

        key = (d.isoformat(), title.lower())
        if key in seen:
            continue
        seen.add(key)

        retail = find_card_price(card)

        # URL
        a_tag = card.find("a", href=True)
        href = a_tag["href"] if a_tag else ""
        if href.startswith("/"):
            href = BASE_URL + href

        rows.append({
            "releaseDate": d.isoformat(),
            "shoeName": title,
            "brand": infer_brand(title),
            "retailPrice": retail,
            "estimatedMarketValue": None,
            "imageUrl": extract_image_url(card, base_url=BASE_URL),
            "sourcePrimary": SOURCE_NAME,
            "sourceSecondary": SOURCE_URL,
            "sourceUrl": SOURCE_URL,
            "releaseUrl": href,
            "releaseMethod": infer_release_method(title),
        })

    return rows


def extract_rows(html: str, soup: BeautifulSoup) -> list[dict[str, Any]]:
    # Try JSON blob first (richer data)
    rows = _extract_from_json_blob(html)
    if rows:
        return rows
    # Fallback to HTML parsing
    return _extract_from_html(soup)


def dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    purge_placeholder_images(rows)
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r.get("releaseDate", ""), str(r.get("shoeName", "")).lower())
        if not key[0] or not key[1]:
            continue
        prev = best.get(key)
        if prev is None or (r.get("retailPrice") or 0) > (prev.get("retailPrice") or 0):
            best[key] = r
    return sorted(best.values(), key=lambda x: (x["releaseDate"], x.get("brand", ""), x["shoeName"].lower()))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch Finish Line launch calendar (US, USD).")
    p.add_argument("--days", type=int, default=35)
    p.add_argument("--timeout-ms", type=int, default=60000)
    p.add_argument("-o", "--output", type=Path, default=Path("data/fallback_finishline.json"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    html = render_html(SOURCE_URL, timeout_ms=args.timeout_ms)
    soup = BeautifulSoup(html, "html.parser")

    rows = window_filter(dedupe(extract_rows(html, soup)), days=args.days)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"{SOURCE_NAME} saved: {len(rows)} -> {args.output}")


if __name__ == "__main__":
    main()
