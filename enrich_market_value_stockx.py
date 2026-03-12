# file: enrich_market_value_stockx.py
#
# Enriches estimatedMarketValue for each release using StockX's public search
# page.  StockX only lists authenticated DS pairs so the data is cleaner than
# eBay (no fakes, no worn pairs, no lot listings).
#
# Strategy:
#   1. Search StockX for the exact shoe name.
#   2. Parse __NEXT_DATA__ JSON embedded in the response.
#   3. Find the closest-matching product and return its lastSale price.
#   4. Fall back to a shorter model-only query if no close match found.
#
# Run BEFORE enrich_market_value_ebay.py so eBay only fills any remaining gaps.

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_STOCKX_SEARCH = "https://stockx.com/search/sneakers?s={query}"

# Minimum similarity (0–1) between search query and StockX product title to
# accept the result.  Prevents assigning a Jordan 1 price to a Dunk Low.
_TITLE_SIM_THRESHOLD = 0.40

# Require at least this many sales recorded before trusting the lastSale value
_MIN_SALES = 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich estimatedMarketValue from StockX.")
    p.add_argument("input_json", type=Path)
    p.add_argument("-o", "--output", type=Path, default=None)
    p.add_argument("--max",     type=int,   default=100)
    p.add_argument("--timeout", type=int,   default=15)
    p.add_argument("--sleep",   type=float, default=0.8)
    p.add_argument("--force",   action="store_true",
                   help="Re-enrich rows that already have a market value")
    return p.parse_args()


def load_rows(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []


def save_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def _token_sim(a: str, b: str) -> float:
    """Rough token overlap similarity (Jaccard on words)."""
    sa = set(re.sub(r"[^a-z0-9]", " ", a.lower()).split())
    sb = set(re.sub(r"[^a-z0-9]", " ", b.lower()).split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _build_queries(shoe_name: str, brand: str) -> list[str]:
    """Return [exact_query, shorter_model_query]."""
    name = shoe_name.strip()
    # Model-only fallback: take first 4 meaningful tokens after removing brand
    name_lower = name.lower()
    brand_lower = brand.lower() if brand else ""
    cleaned = name_lower.replace(brand_lower, "")
    tokens = [t for t in re.split(r"\W+", cleaned) if t and len(t) > 1][:4]
    fallback = f"{brand} {' '.join(tokens)}".strip()
    return [name, fallback]


def _extract_products(html: str) -> list[dict[str, Any]]:
    """
    Pull product listings from StockX's __NEXT_DATA__ JSON.
    Returns a list of dicts with keys: title, lastSale, lowestAsk, salesCount.
    """
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return []

    try:
        data = json.loads(tag.string)
    except json.JSONDecodeError:
        return []

    # Navigate to search results — path varies by StockX page version
    def _walk(obj: Any, *keys: str) -> Any:
        for k in keys:
            if isinstance(obj, dict):
                obj = obj.get(k)
            else:
                return None
        return obj

    edges = (
        _walk(data, "props", "pageProps", "results", "edges")
        or _walk(data, "props", "pageProps", "hits")
        or []
    )

    products = []
    for edge in edges:
        # edges may be dicts with a "node" wrapper
        node = edge.get("node", edge) if isinstance(edge, dict) else {}
        if not isinstance(node, dict):
            continue

        title = node.get("title") or node.get("name") or node.get("shoeName") or ""
        market = node.get("market") or {}
        last_sale = market.get("lastSale") or {}
        lowest_ask = market.get("lowestAsk") or {}
        sales_count = market.get("salesLast72Hours", 0) or market.get("salesThisPeriod", 0) or 0

        # lastSale / lowestAsk can be a dict {amount, currency} or a plain number
        def _price(v: Any) -> float | None:
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, dict):
                raw = v.get("amount") or v.get("value")
                try:
                    return float(str(raw).replace(",", ""))
                except (TypeError, ValueError):
                    return None
            try:
                return float(str(v).replace(",", "").replace("$", ""))
            except (TypeError, ValueError):
                return None

        last = _price(last_sale)
        ask = _price(lowest_ask)
        price = last or ask  # prefer last sale over current ask

        if title and price and price > 30:
            products.append(
                {
                    "title": title,
                    "price": price,
                    "salesCount": int(sales_count),
                }
            )

    return products


def _fetch_html(query: str, timeout: int) -> str:
    url = _STOCKX_SEARCH.format(query=quote_plus(query))
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": UA,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Referer": "https://stockx.com/",
            },
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.text
    except Exception:
        return ""


def get_market_value(shoe_name: str, brand: str, timeout: int, sleep: float) -> int | None:
    queries = _build_queries(shoe_name, brand)
    seen_queries: set[str] = set()

    for query in queries:
        if query in seen_queries:
            continue
        seen_queries.add(query)

        html = _fetch_html(query, timeout)
        time.sleep(sleep)

        if not html:
            continue

        products = _extract_products(html)
        if not products:
            continue

        # Pick the best-matching product above the similarity threshold
        best_price: float | None = None
        best_sim = 0.0
        for p in products:
            sim = _token_sim(shoe_name, p["title"])
            if sim > best_sim and sim >= _TITLE_SIM_THRESHOLD:
                best_sim = sim
                best_price = p["price"]

        if best_price is not None:
            return int(round(best_price))

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
        if not args.force and row.get("estimatedMarketValue") not in (None, 0, ""):
            continue

        shoe_name = str(row.get("shoeName") or "").strip()
        brand = str(row.get("brand") or "").strip()
        if not shoe_name:
            continue

        attempted += 1
        mv = get_market_value(shoe_name, brand, timeout=args.timeout, sleep=args.sleep)
        if mv is not None:
            row["estimatedMarketValue"] = mv
            updated += 1
            print(f"  ✓ {shoe_name[:50]:<50} → ${mv}")
        else:
            print(f"  – {shoe_name[:50]}")

    save_rows(out_path, rows)
    print(f"\nStockX enrich: attempted={attempted} updated={updated} → {out_path.resolve()}")


if __name__ == "__main__":
    main()
