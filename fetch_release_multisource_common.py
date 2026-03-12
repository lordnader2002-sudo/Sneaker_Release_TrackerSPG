# file: fetch_release_multisource_common.py

from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def parse_date_flexible(text: str, default_year: int | None = None) -> date | None:
    if not text:
        return None

    s = normalize_text(text).replace(",", "")
    if not s:
        return None

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        pass

    for fmt in ("%B %d %Y", "%b %d %Y", "%B %d %y", "%b %d %y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    if default_year:
        for fmt in ("%B %d", "%b %d"):
            try:
                d = datetime.strptime(s, fmt).date()
                return date(default_year, d.month, d.day)
            except ValueError:
                continue

    return None


def window_filter(records: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    start = date.today()
    end = start + timedelta(days=days)

    out: list[dict[str, Any]] = []
    for r in records:
        d = parse_date_flexible(str(r.get("releaseDate", "")))
        if d is None:
            continue
        if start <= d < end:
            r["releaseDate"] = d.isoformat()
            out.append(r)

    return out


_STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def render_html(url: str, timeout_ms: int, retries: int = 2) -> str:
    """Render a page with Playwright, retrying on timeout."""
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu"],
                )
                context = browser.new_context(
                    user_agent=_STEALTH_UA,
                    viewport={"width": 1280, "height": 800},
                    locale="en-US",
                )
                page = context.new_page()
                # Block heavy assets that don't affect content
                page.route(
                    "**/*.{png,jpg,jpeg,gif,webp,svg,mp4,woff,woff2,ttf,eot}",
                    lambda route: route.abort(),
                )
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 12000))
                except PlaywrightTimeoutError:
                    pass
                html = page.content()
                browser.close()
                return html
        except PlaywrightTimeoutError as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 ** attempt)  # exponential backoff
            continue
        except Exception as e:
            raise

    raise last_err or RuntimeError(f"render_html failed for {url}")


def infer_brand(name: str) -> str:
    n = name.lower()
    if "jordan" in n:
        return "Air Jordan"
    if any(x in n for x in ("nike", "dunk", "air max", "air force", "shox", "pegasus", "vomero", "zoom", "react")):
        return "Nike"
    if any(x in n for x in ("adidas", "yeezy", "samba", "gazelle", "superstar", "campus", "stan smith", "ultraboost")):
        return "Adidas"
    if "new balance" in n or re.search(r"\bnb\s*\d{3,4}\b", n) or re.search(r"\b(990|550|2002|1906|860|327|574|998|1300)\b", n):
        return "New Balance"
    if "asics" in n or "gel-" in n or "gel " in n:
        return "ASICS"
    if "onitsuka" in n:
        return "Onitsuka Tiger"
    if "puma" in n:
        return "Puma"
    if "reebok" in n or "classic leather" in n:
        return "Reebok"
    if "converse" in n or "chuck taylor" in n or "one star" in n:
        return "Converse"
    if "crocs" in n:
        return "Crocs"
    if "vans" in n or "old skool" in n or "sk8-hi" in n or "era " in n:
        return "Vans"
    if "saucony" in n or "jazz" in n or "shadow" in n:
        return "Saucony"
    if "hoka" in n or "clifton" in n or "bondi" in n or "mafate" in n:
        return "Hoka"
    if "salomon" in n or "xt-6" in n or "speedcross" in n:
        return "Salomon"
    if "timberland" in n:
        return "Timberland"
    if "under armour" in n or "curry" in n:
        return "Under Armour"
    if "lacoste" in n:
        return "Lacoste"
    return "Unknown"


# ---- price + title cleaning ----

_COUNTDOWN_RE = re.compile(r"\b\d{1,3}D:\d{1,2}H:\d{1,2}M:\d{1,2}S\b", re.I)
_PRICE_RE = re.compile(r"(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?", re.I)

_LABELED_PRICE_RE = re.compile(
    r"\b(?:retail\s*price|msrp|price)\b\s*[:\-]?\s*(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?",
    re.I,
)

# Valid retail sneaker price range (avoids stray page numbers, counts, zip codes)
_PRICE_MIN = 40
_PRICE_MAX = 650


def extract_retail_price(text: str) -> int:
    """
    Returns retail price ONLY if clearly labeled (Retail Price / MSRP / Price).
    Prevents garbage like "$180" showing everywhere.
    """
    if not text:
        return 0

    cleaned = text.replace(",", " ")
    m = _LABELED_PRICE_RE.search(cleaned)
    if not m:
        return 0
    try:
        return int(m.group(1))
    except ValueError:
        return 0


def extract_price_smart(text: str) -> int:
    """
    Smarter price extractor for scraper contexts.
    1. Tries labeled patterns first (Retail Price: $130, MSRP $130).
    2. Falls back to any bare $XXX value in [_PRICE_MIN, _PRICE_MAX].
       When multiple bare prices exist, picks the most-common value.
    Returns 0 if nothing plausible found.
    """
    if not text:
        return 0

    cleaned = text.replace(",", " ")

    # Labeled wins immediately
    m = _LABELED_PRICE_RE.search(cleaned)
    if m:
        try:
            val = int(m.group(1))
            if _PRICE_MIN <= val <= _PRICE_MAX:
                return val
        except ValueError:
            pass

    # Collect all bare $XXX in range
    candidates: list[int] = []
    for m in _PRICE_RE.finditer(cleaned):
        try:
            val = int(m.group(1))
            if _PRICE_MIN <= val <= _PRICE_MAX:
                candidates.append(val)
        except ValueError:
            pass

    if not candidates:
        return 0
    if len(candidates) == 1:
        return candidates[0]

    # Most common value wins (handles "$130 … $130 … was $150" → 130)
    from collections import Counter
    return Counter(candidates).most_common(1)[0][0]


# ---- image extraction ----

_IMG_SRC_ATTRS = ("data-src", "src", "data-lazy-src", "data-original", "data-srcset", "srcset")
_IMG_SKIP = ("1x1", "pixel", "placeholder", "blank", "spacer", "loading", "transparent", "logo", "icon")


def extract_image_url(container: Any, base_url: str = "") -> str | None:
    """
    Find the best product image URL from a BeautifulSoup element.
    Walks up to 7 ancestor levels looking for an <img> tag.
    Skips SVGs, data URIs, tracking pixels, and tiny icons.
    Returns an absolute URL or None.
    """
    if container is None or not hasattr(container, "find"):
        return None

    elem = container
    for _ in range(7):
        if elem is None or not hasattr(elem, "find_all"):
            break

        for img in elem.find_all("img"):
            for attr in _IMG_SRC_ATTRS:
                src = img.get(attr) or ""
                if isinstance(src, list):
                    src = src[0] if src else ""
                # srcset → take first URL before space/comma
                src = src.strip().split(",")[0].split(" ")[0].strip()
                if not src or src.startswith("data:") or src.lower().endswith(".svg"):
                    continue
                src_lower = src.lower()
                if any(skip in src_lower for skip in _IMG_SKIP):
                    continue
                # Resolve relative URLs
                if src.startswith("//"):
                    return "https:" + src
                if src.startswith("/") and base_url:
                    return base_url.rstrip("/") + src
                if src.startswith("http"):
                    return src

        elem = elem.parent  # type: ignore[assignment]

    return None


def clean_title(text: str) -> str:
    """
    Removes garbage that sources prepend/append:
    - leading date like "Mar 07 ..." / "March 7, 2026 ..."
    - countdowns like "01D:06H:12M:03S"
    - "COMING SOON"
    - inline prices like "$130.00"
    - trailing size markers like "(GS)" "GS" "(PS)" "(TD)" "WMNS"
    """
    t = normalize_text(text)
    if not t:
        return t

    t = _COUNTDOWN_RE.sub(" ", t)
    t = re.sub(r"\bCOMING\s+SOON\b", " ", t, flags=re.I)
    t = _PRICE_RE.sub(" ", t)

    # strip leading date text
    t = re.sub(
        r"^\s*(?:"
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)"
        r"|January|February|March|April|May|June|July|August|September|October|November|December"
        r")\.?\s+\d{1,2}(?:,\s*\d{4})?\s+",
        "",
        t,
        flags=re.I,
    )

    # strip trailing markers
    t = re.sub(r"\(\s*(gs|ps|td|w|wmns|mens|youth|kids)\s*\)\s*$", "", t, flags=re.I)
    t = re.sub(r"\b(gs|ps|td|wmns|womens|mens|youth|kids)\b\s*$", "", t, flags=re.I)

    t = normalize_text(t.strip(" -|:•"))
    return t
