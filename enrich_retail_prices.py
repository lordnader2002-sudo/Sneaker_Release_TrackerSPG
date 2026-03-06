# file: enrich_retail_prices.py

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

LABELED_PRICE_RE = re.compile(
    r"\b(?:retail\s*price|msrp|price)\b\s*[:\-]?\s*(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?",
    re.I,
)
PLAIN_PRICE_RE = re.compile(r"(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?", re.I)

# used for JSON-LD offers price
JSONLD_PRICE_RE = re.compile(r'"price"\s*:\s*"([0-9]{2,4})(?:\.[0-9]{2})?"', re.I)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich missing retailPrice by fetching releaseUrl pages.")
    p.add_argument("input_json", type=Path)
    p.add_argument("-o", "--output", type=Path, default=None)
    p.add_argument("--max", type=int, default=120, help="Max number of rows to enrich per run.")
    p.add_argument("--timeout", type=int, default=25)
    p.add_argument("--sleep", type=float, default=0.2, help="Delay between requests (seconds).")
    p.add_argument("--min-price", type=int, default=40)
    p.add_argument("--max-price", type=int, default=400)
    return p.parse_args()


def load_rows(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []


def save_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def _is_http_url(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.scheme in ("http", "https")
    except Exception:
        return False


def _absolutize(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if _is_http_url(url):
        return url

    # Common relative paths:
    #  - /release-calendar/...
    #  - /launch/...
    # We choose a base by inspecting the prefix.
    if url.startswith("/release-calendar"):
        return urljoin("https://www.footlocker.com", url)
    if url.startswith("/launch"):
        return urljoin("https://www.nike.com", url)
    if url.startswith("/"):
        # best-effort fallback
        return urljoin("https://www.footlocker.com", url)

    return url


def _price_ok(price: int, min_price: int, max_price: int) -> bool:
    return min_price <= price <= max_price


def _extract_price_from_jsonld(soup: BeautifulSoup) -> int | None:
    # Look for JSON-LD scripts and parse price fields via regex (fast + tolerant)
    for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        txt = (script.string or script.get_text(strip=False) or "").strip()
        if not txt:
            continue
        m = JSONLD_PRICE_RE.search(txt)
        if m:
            try:
                return int(float(m.group(1)))
            except ValueError:
                continue
    return None


def _extract_price_from_itemprop(soup: BeautifulSoup) -> int | None:
    # <meta itemprop="price" content="190">
    meta = soup.find(attrs={"itemprop": "price"})
    if meta and meta.get("content"):
        try:
            return int(float(str(meta["content"]).strip()))
        except ValueError:
            pass

    # Any element with itemprop=price
    node = soup.find(attrs={"itemprop": "price"})
    if node:
        txt = node.get_text(" ", strip=True)
        m = PLAIN_PRICE_RE.search(txt)
        if m:
            try:
                return int(float(m.group(1)))
            except ValueError:
                pass
    return None


def _extract_price_from_text(html: str) -> int | None:
    m = LABELED_PRICE_RE.search(html)
    if m:
        try:
            return int(float(m.group(1)))
        except ValueError:
            return None
    return None


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
        allow_redirects=True,
    )
    r.raise_for_status()
    return r.text


def extract_price(url: str, html: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")

    # 1) JSON-LD offers.price (most reliable)
    p = _extract_price_from_jsonld(soup)
    if p is not None:
        return p

    # 2) itemprop=price (common on ecommerce)
    p = _extract_price_from_itemprop(soup)
    if p is not None:
        return p

    # 3) labeled MSRP/Retail in text
    p = _extract_price_from_text(html)
    if p is not None:
        return p

    return None


def main() -> None:
    args = parse_args()
    out_path = args.output or args.input_json

    rows = load_rows(args.input_json)

    updated = 0
    attempted = 0

    for row in rows:
        if updated >= args.max:
            break

        retail = row.get("retailPrice") or 0
        if isinstance(retail, bool):
            retail = 0

        if int(retail) > 0:
            continue

        url = _absolutize(str(row.get("releaseUrl") or row.get("sourceUrl") or "").strip())
        if not url:
            continue

        attempted += 1
        try:
            html = fetch_html(url, timeout=args.timeout)
            price = extract_price(url, html)
            if price is None:
                continue
            if not _price_ok(price, args.min_price, args.max_price):
                continue

            row["retailPrice"] = int(price)
            row["retailSource"] = url
            updated += 1
        except Exception:
            continue
        finally:
            if args.sleep > 0:
                time.sleep(args.sleep)

    save_rows(out_path, rows)

    print(f"Enrich attempted={attempted} updated={updated} output={out_path.resolve()}")


if __name__ == "__main__":
    main()
