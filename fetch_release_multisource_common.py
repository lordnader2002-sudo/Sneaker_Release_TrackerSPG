# file: fetch_release_multisource_common.py

from __future__ import annotations

import asyncio
import re
import time
from datetime import date, datetime, timedelta
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

try:
    import httpx as _httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False


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

    # Strip ordinal suffixes so "March 5th", "April 1st" etc. parse cleanly
    s = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", s, flags=re.I)

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        pass

    for fmt in ("%B %d %Y", "%b %d %Y", "%B %d %y", "%b %d %y", "%Y-%m-%d",
                "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    if default_year:
        for fmt in ("%B %d", "%b %d"):
            try:
                d = datetime.strptime(s, fmt).date()
                candidate = date(default_year, d.month, d.day)
                # If the candidate is more than 7 days in the past, the date likely
                # wraps into next year (e.g. "Jan 5" scraped in late December).
                if candidate < date.today() - timedelta(days=7):
                    candidate = date(default_year + 1, d.month, d.day)
                return candidate
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

# Full browser navigation headers — makes httpx look like a real page load
_BROWSER_HEADERS = {
    "User-Agent": _STEALTH_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}


def _is_bot_challenge(html: str) -> bool:
    """Return True if the response is a bot-challenge that requires a real browser."""
    if len(html) < 3_000:
        return True
    low = html.lower()
    # Cloudflare "Just a moment…" interstitial
    if "just a moment" in low and ("cloudflare" in low or "cf-ray" in low):
        return True
    # Generic JS-required gate (very short pages only — real SPAs are long)
    if "enable javascript" in low and len(html) < 8_000:
        return True
    return False


async def _httpx_get(url: str, timeout: int) -> str:
    """Async httpx fetch. Raises on HTTP 4xx/5xx."""
    async with _httpx.AsyncClient(
        headers=_BROWSER_HEADERS,
        follow_redirects=True,
        timeout=float(timeout),
        http2=True,
    ) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


def render_html(url: str, timeout_ms: int, retries: int = 2) -> str:
    """
    Fetch a page and return its rendered HTML.

    Fast path  – httpx (no browser, ~1–3 s):
        Works for any server-rendered page (Next.js __NEXT_DATA__, static HTML).
        Skipped automatically when the response is a Cloudflare/bot challenge.

    Slow path  – Playwright Chromium (~15–45 s):
        Used only when httpx returns a bot-challenge or an HTTP error.
    """
    timeout_s = max(timeout_ms // 1000, 10)

    # ── Fast path: plain HTTP ──────────────────────────────────────────────────
    if _HTTPX_AVAILABLE:
        try:
            html = asyncio.run(_httpx_get(url, timeout_s))
            if not _is_bot_challenge(html):
                return html
        except Exception:
            pass  # fall through to Playwright

    # ── Slow path: Playwright ──────────────────────────────────────────────────
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
                page.route(
                    "**/*.{png,jpg,jpeg,gif,webp,svg,mp4,woff,woff2,ttf,eot}",
                    lambda route: route.abort(),
                )
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 12_000))
                except PlaywrightTimeoutError:
                    pass
                html = page.content()
                browser.close()
                return html
        except PlaywrightTimeoutError as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 ** attempt)
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


# ── Price extraction ───────────────────────────────────────────────────────────

_COUNTDOWN_RE = re.compile(r"\b\d{1,3}D:\d{1,2}H:\d{1,2}M:\d{1,2}S\b", re.I)
_PRICE_RE = re.compile(r"(?:USD\s*|GBP\s*)?[$£]\s*([0-9]{2,4})(?:\.[0-9]{2})?", re.I)
_LABELED_PRICE_RE = re.compile(
    r"\b(?:retail\s*price|msrp|price)\b\s*[:\-]?\s*(?:USD\s*|GBP\s*)?[$£]\s*([0-9]{2,4})(?:\.[0-9]{2})?",
    re.I,
)
_PRICE_MIN = 40
_PRICE_MAX = 700


def extract_retail_price(text: str) -> int:
    """Returns retail price ONLY if clearly labeled (Retail Price / MSRP / Price)."""
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
    Extract retail price from a TIGHT per-card context (≤400 chars).
    Labeled pattern wins first; otherwise returns the first bare price in range.
    """
    if not text:
        return 0
    cleaned = text.replace(",", " ")
    m = _LABELED_PRICE_RE.search(cleaned)
    if m:
        try:
            val = int(m.group(1))
            if _PRICE_MIN <= val <= _PRICE_MAX:
                return val
        except ValueError:
            pass
    for m in _PRICE_RE.finditer(cleaned):
        try:
            val = int(m.group(1))
            if _PRICE_MIN <= val <= _PRICE_MAX:
                return val
        except ValueError:
            pass
    return 0


_PRICE_CLASS_RE = re.compile(r"price|cost|amount", re.I)
_EXACT_PRICE_RE = re.compile(r"^\s*[$£]\s*([0-9]{2,4})(?:\.[0-9]{2})?\s*$")


def find_card_price(container: Any, min_price: int = _PRICE_MIN, max_price: int = _PRICE_MAX) -> int:
    """
    Find a retail price from a release card DOM container.

    Tries three strategies in order, stopping at the first success:
      1. Element whose CSS class contains 'price', 'cost', or 'amount'
      2. Text node whose ENTIRE content is a single price (e.g., "$150.00")
      3. Labeled text fallback (Retail Price / MSRP / Price: $X)
    """
    if container is None or not hasattr(container, "find_all"):
        return 0

    # Strategy 1: dedicated price element by CSS class
    for elem in container.find_all(class_=_PRICE_CLASS_RE):
        text = " ".join(elem.get_text(" ", strip=True).split())
        m = _PRICE_RE.search(text)
        if m:
            try:
                val = int(float(m.group(1)))
                if min_price <= val <= max_price:
                    return val
            except (ValueError, TypeError):
                pass

    # Strategy 2: text node that IS exactly a price ("$150.00" with nothing else)
    for node in container.strings:
        m = _EXACT_PRICE_RE.match(node)
        if m:
            try:
                val = int(float(m.group(1)))
                if min_price <= val <= max_price:
                    return val
            except (ValueError, TypeError):
                pass

    # Strategy 3: labeled text fallback
    return extract_retail_price(" ".join(container.get_text(" ", strip=True).split())[:400])


# ── Image extraction ───────────────────────────────────────────────────────────

_IMG_SRC_ATTRS = ("data-src", "src", "data-lazy-src", "data-original", "data-srcset", "srcset")
_IMG_SKIP = (
    "1x1", "pixel", "placeholder", "blank", "spacer", "loading", "transparent", "logo", "icon",
    "silhouette", "coming-soon", "comingsoon", "tbd", "no-image", "noimage", "unavailable",
    "default-shoe", "generic", "missing",
)


def extract_image_url(container: Any, base_url: str = "") -> str | None:
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
                src = src.strip().split(",")[0].split(" ")[0].strip()
                if not src or src.startswith("data:") or src.lower().endswith(".svg"):
                    continue
                src_lower = src.lower()
                if any(skip in src_lower for skip in _IMG_SKIP):
                    continue
                if src.startswith("//"):
                    return "https:" + src
                if src.startswith("/") and base_url:
                    return base_url.rstrip("/") + src
                if src.startswith("http"):
                    return src
        elem = elem.parent  # type: ignore[assignment]
    return None


def purge_placeholder_images(rows: list[Any], max_repeat: int = 3) -> None:
    """Nullify imageUrl for any URL that appears on more than `max_repeat` rows."""
    from collections import Counter
    counts: Counter[str] = Counter(r["imageUrl"] for r in rows if r.get("imageUrl"))
    bad = {url for url, n in counts.items() if n > max_repeat}
    for r in rows:
        if r.get("imageUrl") in bad:
            r["imageUrl"] = None


def clean_title(text: str) -> str:
    """Remove dates, countdowns, prices, and size markers from scraped titles."""
    t = normalize_text(text)
    if not t:
        return t
    t = _COUNTDOWN_RE.sub(" ", t)
    t = re.sub(r"\bCOMING\s+SOON\b", " ", t, flags=re.I)
    t = re.sub(r"\bfrom\s+[$£]\s*\d{2,4}(?:\.\d{2})?\b", " ", t, flags=re.I)
    t = _PRICE_RE.sub(" ", t)
    t = re.sub(
        r"^\s*(?:"
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)"
        r"|January|February|March|April|May|June|July|August|September|October|November|December"
        r")\.?\s+\d{1,2}(?:,\s*\d{4})?\s+",
        "",
        t,
        flags=re.I,
    )
    t = re.sub(r"\(\s*(gs|ps|td|w|wmns|mens|youth|kids)\s*\)\s*$", "", t, flags=re.I)
    t = re.sub(r"\b(gs|ps|td|wmns|womens|mens|youth|kids)\b\s*$", "", t, flags=re.I)
    t = normalize_text(t.strip(" -|:•"))
    return t


_METHOD_APP_RE    = re.compile(r"\b(SNKRS|app\s+entr(?:y|ies)|app\s+exclusive|entries\s+open|app\s+only|Nike\s+app)\b", re.I)
_METHOD_RAFFLE_RE = re.compile(r"\b(raffle|draw|lottery|ballot)\b", re.I)
_METHOD_ONLINE_RE = re.compile(r"\b(online[\s-]+only|online[\s-]+exclusive|web[\s-]+only|e[\s-]?raffle)\b", re.I)
_METHOD_STORE_RE  = re.compile(r"\b(in[\s-]+store[\s-]+only|in[\s-]+store[\s-]+exclusive|select\s+stores?|retail\s+only)\b", re.I)


def infer_release_method(name: str, context: str = "") -> str:
    """Infer release channel from shoe name / context text.

    Returns one of: "App", "Raffle", "Online", "In-Store", or "" (unknown).
    """
    text = name + " " + context
    if _METHOD_APP_RE.search(text):
        return "App"
    if _METHOD_RAFFLE_RE.search(text):
        return "Raffle"
    if _METHOD_ONLINE_RE.search(text):
        return "Online"
    if _METHOD_STORE_RE.search(text):
        return "In-Store"
    return ""


def find_sibling_date(
    anchor: Any,
    date_re: "re.Pattern[str]",
    default_year: int,
    max_depth: int = 7,
    max_sib_text: int = 60,
) -> "tuple[date | None, Any]":
    """Locate a release date for *anchor* by scanning preceding siblings.

    Release-calendar pages (Foot Locker, Hibbett, Nike) typically display the
    date in a compact box that is a *sibling* of the product-details panel, not
    an ancestor of the product anchor.  Walking straight up and grabbing the
    entire container text then fails once the container spans multiple cards.

    This function walks up *max_depth* ancestor levels.  At each level it
    inspects the preceding siblings of the current node.  The first short-text
    sibling whose text matches *date_re* wins.  Returns ``(date, container)``
    where *container* is the ancestor element at which the date was found
    (useful for subsequent price / image extraction), or ``(None, None)``.
    """
    node = anchor
    for _ in range(max_depth):
        node = node.parent
        if node is None:
            break
        for sib in node.previous_siblings:
            if not hasattr(sib, "get_text"):
                continue
            text = normalize_text(sib.get_text(" ", strip=True))
            if not text or len(text) > max_sib_text:
                continue
            m = date_re.search(text)
            if not m:
                continue
            month = m.group(1)
            day   = m.group(2)
            year  = m.group(3) if m.lastindex and m.lastindex >= 3 else None
            date_str = f"{month} {day} {year}" if year else f"{month} {day}"
            d = parse_date_flexible(date_str, default_year=default_year)
            if d:
                return d, node
    return None, None
