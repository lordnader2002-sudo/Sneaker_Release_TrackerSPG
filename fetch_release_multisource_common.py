# file: fetch_release_multisource_common.py

from __future__ import annotations

import re
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


def render_html(url: str, timeout_ms: int) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 15000))
        except PlaywrightTimeoutError:
            pass
        html = page.content()
        browser.close()
        return html


def infer_brand(name: str) -> str:
    n = name.lower()
    if "jordan" in n:
        return "Air Jordan"
    if any(x in n for x in ("nike", "dunk", "air max", "air force", "shox", "pegasus", "vomero")):
        return "Nike"
    if any(x in n for x in ("adidas", "yeezy", "samba", "gazelle", "superstar")):
        return "Adidas"
    if "new balance" in n or n.startswith("nb "):
        return "New Balance"
    if "asics" in n:
        return "ASICS"
    if "puma" in n:
        return "Puma"
    if "reebok" in n:
        return "Reebok"
    if "converse" in n:
        return "Converse"
    if "crocs" in n:
        return "Crocs"
    return "Unknown"


# ---- price + title cleaning ----

_COUNTDOWN_RE = re.compile(r"\b\d{1,3}D:\d{1,2}H:\d{1,2}M:\d{1,2}S\b", re.I)
_PRICE_RE = re.compile(r"(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?", re.I)

_LABELED_PRICE_RE = re.compile(
    r"\b(?:retail\s*price|msrp|price)\b\s*[:\-]?\s*(?:USD\s*)?\$\s*([0-9]{2,4})(?:\.[0-9]{2})?",
    re.I,
)


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
