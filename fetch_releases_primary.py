# file: fetch_releases_primary.py
#
# Primary sneaker release scraper for GOAT.
#
# Fast path  – httpx (no browser, ~2 s):
#   Fetches GOAT pages directly and extracts __NEXT_DATA__ JSON embedded in the HTML.
#   Works as long as GOAT serves server-rendered content (Next.js).
#
# Slow path  – Playwright + network interception (~45 s):
#   Used only when the httpx path returns no records.
#   Intercepts Algolia + GOAT API responses from within a real browser session.

from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from playwright.sync_api import Response, TimeoutError as PlaywrightTimeoutError, sync_playwright

from fetch_release_multisource_common import (
    _BROWSER_HEADERS,
    _STEALTH_UA,
    infer_brand,
    normalize_text,
    parse_date_flexible,
    window_filter,
)

try:
    import httpx as _httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False

SOURCE_NAME = "goat"
SOURCE_URL  = "https://www.goat.com/sneakers"

# GOAT's release-calendar API path fragment (changes occasionally; match loosely)
_GOAT_CAL_RE = re.compile(r"goat\.com/api/v1/product_variants/buy_bar_data", re.I)
_GOAT_SEARCH_RE = re.compile(
    r"(2fwotdvm2o-dsn\.algolia\.net|goat\.com/api/v\d+/(search|browse|trending|most_popular|featured|product_variants))",
    re.I,
)
_NEXT_DATA_RE = re.compile(r'<script\s+id="__NEXT_DATA__"[^>]*>([^<]+)</script>', re.S)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch upcoming sneaker releases from GOAT.")
    p.add_argument("--days",    type=int,  default=35)
    p.add_argument("--limit",   type=int,  default=300)
    p.add_argument("--timeout", type=int,  default=45, help="Page load timeout in seconds")
    p.add_argument("--output",  type=Path, default=Path("data/primary_releases.json"))
    return p.parse_args()


# ── Normalisation helpers ──────────────────────────────────────────────────────

def _parse_price(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(round(value)))
    try:
        return max(0, int(round(float(
            str(value).replace("$", "").replace(",", "").strip()
        ))))
    except ValueError:
        return 0


def _iso(value: Any) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    # Epoch ms
    if raw.isdigit() and len(raw) >= 13:
        try:
            return datetime.utcfromtimestamp(int(raw) / 1000).date().isoformat()
        except Exception:
            return None
    d = parse_date_flexible(raw)
    return d.isoformat() if d else None


def _normalize_record(obj: dict[str, Any]) -> dict[str, Any] | None:
    # Accept records from several GOAT API shapes
    name = normalize_text(
        obj.get("name") or obj.get("productTitle") or obj.get("title") or
        obj.get("product_title") or obj.get("localizedSpecialDisplayPriceCents") or ""
    )
    if not name:
        return None

    release_date = (
        _iso(obj.get("releaseDate"))
        or _iso(obj.get("release_date"))
        or _iso(obj.get("releaseDateYear"))
        or _iso(obj.get("first_release_date"))
    )
    if not release_date:
        return None

    retail = _parse_price(
        obj.get("retailPriceCents") or obj.get("retail_price_cents") or
        obj.get("msrp") or obj.get("retailPrice") or obj.get("retail_price")
    )
    # GOAT stores cents for some fields
    if retail > 5000:
        retail = retail // 100

    resale_raw = (
        obj.get("lowestPriceCents") or obj.get("lowest_price_cents") or
        obj.get("lowestAsk") or obj.get("lowest_ask") or
        obj.get("instantShipLowestPriceCents") or
        obj.get("marketPrice") or obj.get("market_price")
    )
    resale = _parse_price(resale_raw)
    if resale > 5000:
        resale = resale // 100
    if resale == 0:
        resale = None

    brand_raw = obj.get("brandName") or obj.get("brand_name") or obj.get("brand") or ""
    brand = infer_brand(name) if not brand_raw else normalize_text(brand_raw)

    image = normalize_text(
        obj.get("pictureUrl") or obj.get("picture_url") or
        obj.get("mainPictureUrl") or obj.get("main_picture_url") or
        obj.get("image") or obj.get("imageUrl") or ""
    ) or None

    slug = obj.get("slug") or obj.get("productSlug") or ""
    release_url = f"https://www.goat.com/sneakers/{slug}" if slug else None

    return {
        "releaseDate":            release_date,
        "shoeName":               name,
        "brand":                  brand,
        "retailPrice":            retail,
        "estimatedMarketValue":   resale,
        "imageUrl":               image,
        "sourcePrimary":          SOURCE_NAME,
        "sourceSecondary":        None,
        "sourceUrl":              SOURCE_URL,
        "releaseUrl":             release_url,
    }


# ── Extract from various API response shapes ──────────────────────────────────

def _extract_from_blob(data: Any) -> list[dict[str, Any]]:
    """Recursively find product arrays inside an arbitrary JSON blob."""
    records: list[dict[str, Any]] = []

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and ("name" in item or "productTitle" in item or "title" in item):
                r = _normalize_record(item)
                if r:
                    records.append(r)
            else:
                records.extend(_extract_from_blob(item))
        return records

    if isinstance(data, dict):
        # Algolia hits shape: {"results": [{"hits": [...]}]}
        if "results" in data and isinstance(data["results"], list):
            for result in data["results"]:
                if isinstance(result, dict) and "hits" in result:
                    records.extend(_extract_from_blob(result["hits"]))
            return records

        # Direct hits array
        if "hits" in data:
            return _extract_from_blob(data["hits"])

        # GOAT product-variants shape: {"productVariants": [...]}
        for key in ("productVariants", "product_variants", "products", "items", "data", "payload"):
            if key in data and isinstance(data[key], list):
                records.extend(_extract_from_blob(data[key]))
                if records:
                    return records

        # Single record
        if "name" in data or "productTitle" in data:
            r = _normalize_record(data)
            if r:
                records.append(r)

    return records


def _extract_from_next_data(html: str) -> list[dict[str, Any]]:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return []
    try:
        blob = json.loads(m.group(1))
        return _extract_from_blob(blob)
    except Exception:
        return []


# ── Deduplication ─────────────────────────────────────────────────────────────

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

        def score(x: dict[str, Any]) -> int:
            return (
                int(bool(x.get("imageUrl")))
                + int(_parse_price(x.get("retailPrice")) > 0)
                + int(_parse_price(x.get("estimatedMarketValue") or 0) > 0)
                + int(bool(x.get("releaseUrl")))
            )

        if score(r) > score(existing):
            best[key] = r

    return sorted(best.values(), key=lambda x: (x["releaseDate"], x.get("brand", ""), x["shoeName"].lower()))


# ── httpx fast path ───────────────────────────────────────────────────────────

_GOAT_URLS = [
    "https://www.goat.com/sneakers?sort=release_date_desc&priceRange=0-5000",
    "https://www.goat.com/sneakers",
]


async def _goat_httpx(timeout: int) -> list[dict[str, Any]]:
    """
    Attempt to retrieve GOAT releases via plain HTTP (no browser).
    GOAT is a Next.js app — the server renders release data into __NEXT_DATA__
    which is readable from the raw HTML without executing any JavaScript.
    Returns [] if the page is blocked or yields nothing useful.
    """
    async with _httpx.AsyncClient(
        headers=_BROWSER_HEADERS,
        follow_redirects=True,
        timeout=float(timeout),
        http2=True,
    ) as client:
        for url in _GOAT_URLS:
            try:
                r = await client.get(url)
                if r.status_code >= 400 or len(r.text) < 5_000:
                    continue
                records = _extract_from_next_data(r.text)
                if records:
                    print(f"GOAT httpx: {len(records)} records from {url} (no browser)")
                    return records
            except Exception:
                continue
    return []


# ── Playwright fetch ───────────────────────────────────────────────────────────

def fetch_goat(timeout_ms: int, limit: int) -> list[dict[str, Any]]:
    # ── Fast path: httpx (no browser) ─────────────────────────────────────────
    if _HTTPX_AVAILABLE:
        try:
            records = asyncio.run(_goat_httpx(timeout=min(timeout_ms // 1000, 20)))
            if records:
                return records[:limit]
        except Exception:
            pass
    print("GOAT: httpx returned nothing — falling back to Playwright network interception...")

    # ── Slow path: Playwright ──────────────────────────────────────────────────
    intercepted: list[dict[str, Any]] = []

    def on_response(response: Response) -> None:
        url = response.url
        if not (_GOAT_SEARCH_RE.search(url) or _GOAT_CAL_RE.search(url)):
            return
        if response.status not in (200, 201):
            return
        try:
            body = response.json()
            records = _extract_from_blob(body)
            intercepted.extend(records)
        except Exception:
            pass

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu"],
        )
        context = browser.new_context(
            user_agent=_STEALTH_UA,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        page = context.new_page()
        # Block heavy non-data assets
        page.route(
            "**/*.{mp4,webm,ogg,mp3,gif,woff,woff2,ttf,eot}",
            lambda route: route.abort(),
        )
        page.on("response", on_response)

        # ── Pages: try each GOAT URL until we get intercepted data ──
        page_html = ""
        for url in _GOAT_URLS:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 12000))
                except PlaywrightTimeoutError:
                    pass
                # Scroll to trigger lazy-loaded content
                for _ in range(3):
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    time.sleep(0.8)
                page_html = page.content()
                if intercepted:
                    break
            except PlaywrightTimeoutError:
                continue
            except Exception:
                continue

        browser.close()

    # Prefer intercepted API data; fall back to __NEXT_DATA__
    records = intercepted if intercepted else _extract_from_next_data(page_html)
    return records[:limit]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching from GOAT (timeout={args.timeout}s, limit={args.limit})…")

    try:
        raw = fetch_goat(timeout_ms=args.timeout * 1000, limit=args.limit)
    except Exception as e:
        print(f"GOAT fetch failed: {e}")
        raw = []

    print(f"Intercepted/extracted records: {len(raw)}")

    filtered = window_filter(raw, days=args.days)
    cleaned  = dedupe(filtered)

    args.output.write_text(json.dumps(cleaned, indent=2), encoding="utf-8")
    print(f"Filtered to window: {len(filtered)}")
    print(f"Deduped:            {len(cleaned)}")
    print(f"Output:             {args.output.resolve()}")


if __name__ == "__main__":
    main()
