# file: fetch_release_fallback.py

from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


DEFAULT_SOURCES = (
    "https://www.nike.com/launch/upcoming",
    "https://www.nike.com/launch",
)

DATE_KEYS = (
    "publishDate",
    "startEntryDate",
    "startSellDate",
    "launchDate",
    "releaseDate",
    "date",
)

NAME_KEYS = (
    "fullTitle",
    "title",
    "threadTitle",
    "name",
    "label",
    "productName",
    "headline",
    "subtitle",
)

PRICE_KEYS = (
    "price",
    "fullPrice",
    "msrp",
    "retailPrice",
    "amount",
)

IMAGE_KEYS = (
    "imageUrl",
    "portraitURL",
    "image",
    "mainPictureUrl",
    "thumbnail",
    "hero",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch fallback release data with Playwright.")
    parser.add_argument("-o", "--output", type=Path, default=Path("data/fallback_releases.json"))
    parser.add_argument("--days", type=int, default=35)
    parser.add_argument("--timeout-ms", type=int, default=45000)
    return parser.parse_args()


def parse_date(value: Any) -> date | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, (int, float)):
        try:
            if value > 10_000_000_000:
                return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date()
            if value > 100_000_000:
                return datetime.fromtimestamp(value, tz=timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None

    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    normalized = (
        raw.replace("Z", "+00:00")
        .replace("/", "-")
        .replace(".", "-")
        .replace(",", "")
    )

    formats = (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%b %d %Y",
        "%B %d %Y",
        "%m-%d-%Y",
        "%m-%d-%y",
    )

    for fmt in formats:
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def parse_price(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return int(round(value)) if value >= 0 else None

    if not isinstance(value, str):
        return None

    cleaned = value.replace("$", "").replace(",", "").replace("USD", "").strip()
    if not cleaned:
        return None

    try:
        parsed = float(cleaned)
    except ValueError:
        return None

    return int(round(parsed)) if parsed >= 0 else None


def pick_first(record: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, "", [], {}):
            return record[key]
    return None


def normalize_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def infer_brand(name: str) -> str:
    lowered = name.lower()
    if "jordan" in lowered:
        return "Air Jordan"
    if (
        "nike" in lowered
        or "air max" in lowered
        or "dunk" in lowered
        or "zoom" in lowered
        or "kd " in lowered
        or "lebron" in lowered
        or "kobe" in lowered
        or "sabrina" in lowered
    ):
        return "Nike"
    if "adidas" in lowered or "samba" in lowered or "gazelle" in lowered:
        return "Adidas"
    if "new balance" in lowered:
        return "New Balance"
    if "asics" in lowered:
        return "ASICS"
    if "converse" in lowered:
        return "Converse"
    if "crocs" in lowered:
        return "Crocs"
    return "Unknown"


def find_price(record: dict[str, Any]) -> int | None:
    direct = parse_price(pick_first(record, PRICE_KEYS))
    if direct is not None:
        return direct

    for key in ("price", "pricing", "merchPrice", "sku"):
        nested = record.get(key)
        if isinstance(nested, dict):
            for value in nested.values():
                parsed = parse_price(value)
                if parsed is not None:
                    return parsed

    return None


def find_image(record: dict[str, Any]) -> str | None:
    direct = pick_first(record, IMAGE_KEYS)
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    for key in ("imageUrls", "images"):
        nested = record.get(key)
        if isinstance(nested, dict):
            for value in nested.values():
                if isinstance(value, str) and value.strip():
                    return value.strip()
        elif isinstance(nested, list):
            for item in nested:
                if isinstance(item, str) and item.strip():
                    return item.strip()
                if isinstance(item, dict):
                    for value in item.values():
                        if isinstance(value, str) and value.strip():
                            return value.strip()
    return None


def iter_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_dicts(child)
    elif isinstance(value, list):
        for item in value:
            yield from iter_dicts(item)


def extract_json_strings_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    for script in soup.find_all("script"):
        text = (script.string or script.get_text(strip=False) or "").strip()
        if not text:
            continue

        script_type = (script.get("type") or "").lower().strip()
        if script_type in {"application/json", "application/ld+json"}:
            candidates.append(text)
            continue

        if any(token in text for token in ("__NEXT_DATA__", "__PRELOADED_STATE__", "INITIAL_STATE")):
            candidates.append(text)

    patterns = [
        r"__NEXT_DATA__\s*=\s*(\{.*?\})\s*;",
        r"__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*;",
        r"INITIAL_STATE\s*=\s*(\{.*?\})\s*;",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, html, flags=re.DOTALL):
            candidates.append(match.group(1))

    return candidates


def json_load_loose(text: str) -> Any | None:
    text = text.strip()
    if not text:
        return None

    attempts = [text]
    if text.endswith(";"):
        attempts.append(text[:-1])

    for candidate in attempts:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def looks_like_release(record: dict[str, Any]) -> bool:
    release_date = parse_date(pick_first(record, DATE_KEYS))
    if release_date is None:
        return False

    name = normalize_text(pick_first(record, NAME_KEYS))
    if not name:
        return False

    lowered = name.lower()
    reject_tokens = ("episode", "article", "story", "feed", "podcast", "video")
    return not any(token in lowered for token in reject_tokens)


def normalize_release_from_dict(record: dict[str, Any], source: str) -> dict[str, Any] | None:
    if not looks_like_release(record):
        return None

    release_date = parse_date(pick_first(record, DATE_KEYS))
    shoe_name = normalize_text(pick_first(record, NAME_KEYS))
    if release_date is None or not shoe_name:
        return None

    return {
        "releaseDate": release_date.isoformat(),
        "shoeName": shoe_name,
        "brand": infer_brand(shoe_name),
        "retailPrice": find_price(record) or 0,
        "estimatedMarketValue": None,
        "imageUrl": find_image(record),
        "sourcePrimary": "playwright-fallback",
        "sourceSecondary": source,
        "sourceUrl": source,
    }


def parse_date_from_text(text: str) -> date | None:
    cleaned = text.replace(",", " ")
    patterns = [
        r"\b([A-Z][a-z]+ \d{1,2} \d{4})\b",
        r"\b([A-Z][a-z]+ \d{1,2})\b",
        r"\b(\d{4}-\d{2}-\d{2})\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if not match:
            continue

        value = match.group(1)
        parsed = parse_date(value)
        if parsed is not None:
            return parsed

        if re.fullmatch(r"[A-Z][a-z]+ \d{1,2}", value):
            parsed = parse_date(f"{value} {date.today().year}")
            if parsed is not None:
                return parsed

    return None


def normalize_release_from_link(link_text: str, href: str, source: str) -> dict[str, Any] | None:
    text = normalize_text(link_text)
    if not text:
        return None

    lowered = text.lower()
    if len(text) < 6:
        return None
    if any(token in lowered for token in ("shop now", "learn more", "view all", "men", "women", "kids")):
        return None

    target_tokens = (
        "jordan", "nike", "dunk", "air max", "zoom", "kobe",
        "lebron", "sabrina", "adidas", "samba", "gazelle", "crocs", "converse"
    )
    if not any(token in lowered for token in target_tokens):
        return None

    inferred_date = parse_date_from_text(text) or (date.today() + timedelta(days=1))

    return {
        "releaseDate": inferred_date.isoformat(),
        "shoeName": text,
        "brand": infer_brand(text),
        "retailPrice": 0,
        "estimatedMarketValue": None,
        "imageUrl": None,
        "sourcePrimary": "playwright-linkscan",
        "sourceSecondary": source,
        "sourceUrl": href,
    }


def scrape_page(page, url: str, timeout_ms: int) -> list[dict[str, Any]]:
    print(f"Opening: {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 12000))
    except PlaywrightTimeoutError:
        pass

    html = page.content()
    releases: list[dict[str, Any]] = []

    for candidate in extract_json_strings_from_html(html):
        payload = json_load_loose(candidate)
        if payload is None:
            continue

        for item in iter_dicts(payload):
            normalized = normalize_release_from_dict(item, source=url)
            if normalized is not None:
                releases.append(normalized)

    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        text = anchor.get_text(" ", strip=True)
        if "/launch/" not in href and "/t/" not in href:
            continue

        normalized = normalize_release_from_link(text, href, source=url)
        if normalized is not None:
            releases.append(normalized)

    return releases


def dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}

    for record in records:
        key = (record["releaseDate"], record["shoeName"].lower())
        existing = best.get(key)

        if existing is None:
            best[key] = record
            continue

        existing_score = int(bool(existing.get("imageUrl"))) + int(existing.get("retailPrice", 0) > 0)
        incoming_score = int(bool(record.get("imageUrl"))) + int(record.get("retailPrice", 0) > 0)

        if incoming_score > existing_score:
            best[key] = record

    return sorted(
        best.values(),
        key=lambda item: (item["releaseDate"], item["brand"].lower(), item["shoeName"].lower()),
    )


def filter_window(records: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    start = date.today()
    end = start + timedelta(days=days)

    out: list[dict[str, Any]] = []
    for record in records:
        parsed = parse_date(record.get("releaseDate"))
        if parsed is None:
            continue
        if start <= parsed < end:
            out.append(record)
    return out


def main() -> None:
    args = parse_args()
    collected: list[dict[str, Any]] = []

    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu"],
        )
        context = browser.new_context(
            user_agent=UA,
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        page = context.new_page()

        for source in DEFAULT_SOURCES:
            try:
                collected.extend(scrape_page(page, source, timeout_ms=args.timeout_ms))
            except Exception as error:
                print(f"Failed to scrape {source}: {error}")

        browser.close()

    filtered = filter_window(collected, days=args.days)
    cleaned = dedupe(filtered)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(cleaned, indent=2), encoding="utf-8")

    print(f"Collected fallback records: {len(collected)}")
    print(f"Filtered fallback records: {len(filtered)}")
    print(f"Saved fallback records: {len(cleaned)}")
    print(f"Output: {args.output.resolve()}")


if __name__ == "__main__":
    main()
