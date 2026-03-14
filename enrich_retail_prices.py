# file: enrich_retail_prices.py
#
# Enriches missing retailPrice fields by fetching each release's URL and
# extracting a price from JSON-LD, itemprop, or labeled MSRP text.
#
# Improvements over v1:
#   - Async httpx (up to 8 concurrent requests) instead of sequential requests
#   - SQLite price cache with 7-day TTL — skips URLs already fetched recently
#   - Same HTML parsing logic, just faster delivery

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

LABELED_PRICE_RE = re.compile(
    r"\b(?:retail\s*price|msrp|price)\b\s*[:\-]?\s*(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?",
    re.I,
)
PLAIN_PRICE_RE = re.compile(r"(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?", re.I)
JSONLD_PRICE_RE = re.compile(r'"price"\s*:\s*"([0-9]{2,4})(?:\.[0-9]{2})?"', re.I)

# ── SQLite price cache ─────────────────────────────────────────────────────────

_CACHE_PATH = Path("data/price_cache.db")
_CACHE_TTL_DAYS = 7


def _open_cache(path: Path = _CACHE_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS price_cache "
        "(url TEXT PRIMARY KEY, price INTEGER NOT NULL, ts TEXT NOT NULL)"
    )
    conn.commit()
    return conn


def _cache_get(conn: sqlite3.Connection, url: str) -> int | None:
    cutoff = (datetime.utcnow() - timedelta(days=_CACHE_TTL_DAYS)).isoformat()
    row = conn.execute(
        "SELECT price FROM price_cache WHERE url=? AND ts>=?", (url, cutoff)
    ).fetchone()
    return int(row[0]) if row else None


def _cache_put(conn: sqlite3.Connection, url: str, price: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO price_cache (url, price, ts) VALUES (?,?,?)",
        (url, price, datetime.utcnow().isoformat()),
    )
    conn.commit()


# ── Argument parsing ───────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich missing retailPrice by fetching releaseUrl pages.")
    p.add_argument("input_json", type=Path)
    p.add_argument("-o", "--output", type=Path, default=None)
    p.add_argument("--max",       type=int,   default=120, help="Max rows to enrich per run.")
    p.add_argument("--timeout",   type=int,   default=25)
    p.add_argument("--sleep",     type=float, default=0.15, help="Per-slot delay between requests (s).")
    p.add_argument("--concurrency", type=int, default=8,  help="Max concurrent HTTP requests.")
    p.add_argument("--min-price", type=int,   default=40)
    p.add_argument("--max-price", type=int,   default=400)
    return p.parse_args()


# ── I/O ────────────────────────────────────────────────────────────────────────

def load_rows(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []


def save_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


# ── URL helpers ────────────────────────────────────────────────────────────────

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
    if url.startswith("/release-calendar"):
        return urljoin("https://www.footlocker.com", url)
    if url.startswith("/launch"):
        return urljoin("https://www.nike.com", url)
    if url.startswith("/"):
        return urljoin("https://www.footlocker.com", url)
    return url


# ── Price extraction (HTML parsing) ───────────────────────────────────────────

def _extract_price_from_jsonld(soup: BeautifulSoup) -> int | None:
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
    meta = soup.find(attrs={"itemprop": "price"})
    if meta and meta.get("content"):
        try:
            return int(float(str(meta["content"]).strip()))
        except ValueError:
            pass
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
            pass
    return None


def _extract_price(url: str, html: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")
    p = _extract_price_from_jsonld(soup)
    if p is not None:
        return p
    p = _extract_price_from_itemprop(soup)
    if p is not None:
        return p
    return _extract_price_from_text(html)


# ── Async fetch ────────────────────────────────────────────────────────────────

async def _fetch_one(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    url: str,
    timeout: int,
    min_price: int,
    max_price: int,
    sleep: float,
) -> int | None:
    """Fetch one URL and return a valid price, or None."""
    async with sem:
        try:
            r = await client.get(url, timeout=float(timeout))
            if r.status_code >= 400:
                return None
            price = _extract_price(url, r.text)
            if price is not None and min_price <= price <= max_price:
                return price
        except Exception:
            return None
        finally:
            if sleep > 0:
                await asyncio.sleep(sleep)
    return None


async def _run(rows: list[dict[str, Any]], args: argparse.Namespace, conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Enrich rows in-place using async httpx.
    Returns (attempted, updated) counts.
    """
    sem = asyncio.Semaphore(args.concurrency)
    attempted = 0
    cache_hits = 0

    # Build work list: rows that need enrichment, up to --max
    work: list[tuple[int, str]] = []
    for idx, row in enumerate(rows):
        if attempted + cache_hits >= args.max:
            break
        if int(row.get("retailPrice") or 0) > 0:
            continue
        url = _absolutize(str(row.get("releaseUrl") or row.get("sourceUrl") or "").strip())
        if not url:
            continue

        # Cache hit — apply immediately without an HTTP request
        cached = _cache_get(conn, url)
        if cached is not None and args.min_price <= cached <= args.max_price:
            row["retailPrice"] = cached
            row["retailSource"] = url
            cache_hits += 1
            continue

        work.append((idx, url))
        attempted += 1

    # Dispatch all cache-miss fetches concurrently
    updated = 0
    async with httpx.AsyncClient(
        headers=_HEADERS,
        follow_redirects=True,
        http2=True,
    ) as client:
        tasks = [
            (idx, url, asyncio.create_task(
                _fetch_one(sem, client, url, args.timeout, args.min_price, args.max_price, args.sleep)
            ))
            for idx, url in work
        ]
        for idx, url, task in tasks:
            price = await task
            if price is not None:
                rows[idx]["retailPrice"] = price
                rows[idx]["retailSource"] = url
                _cache_put(conn, url, price)
                updated += 1

    return attempted, updated + cache_hits


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    out_path = args.output or args.input_json

    rows = load_rows(args.input_json)
    conn = _open_cache()

    attempted, updated = asyncio.run(_run(rows, args, conn))
    conn.close()

    save_rows(out_path, rows)
    print(f"Enrich attempted={attempted} updated={updated} output={out_path.resolve()}")


if __name__ == "__main__":
    main()
