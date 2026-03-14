# file: merge_and_compare.py

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from fetch_release_multisource_common import infer_release_method


HIGH_BRANDS = {"air jordan", "nike", "yeezy", "new balance", "adidas"}
COLLAB_KEYWORDS = {
    # Celebrity / Artist collabs
    "travis scott",
    "off-white",
    "j balvin",
    "union",
    "supreme",
    "fear of god",
    "kith",
    "trophy room",
    "clot",
    "a ma maniere",
    "action bronson",
    "salehe bembury",
    "sacai",
    "fragment",
    "undefeated",
    "concepts",
    "bodega",
    "strangelove",
    "parra",
    "stussy",
    "patta",
    "futura",
    # High-profile athletes and celebrities
    "virgil abloh",
    "drake",
    "nocta",
    "kendrick lamar",
    "kanye",
    "pharrell",
    "bad bunny",
    "j. cole",
    "deion sanders",
    "luka doncic",
    "joe freshgoods",
    "social status",
    "mache",
    "atmos",
    "cactus plant flea market",
    "cpfm",
    "cactus jack",
    "sean wotherspoon",
    "ben and jerry",
    "ben & jerry",
    "grateful dead",
    "wu-tang",
    "wu tang",
    "slam jam",
    "end clothing",
    "size?",
    "offspring",
    "sneakersnstuff",
    "kicks lab",
    "mita",
    "whiz limited",
    "nonnative",
    "mastermind",
    "number nine",
    "undercover",
    "comme des garcons",
    "comme des garçons",
    "cdg",
    "bape",
    "neighborhood",
    "medicom toy",
    "beams",
    "palace",
    "the north face",
    "tnf",
    "dj khaled",
    "michael b jordan",
    "serena williams",
    "naomi osaka",
    "billie eilish",
    "aleali may",
    "ambush",
    "feng chen wang",
    "matthew m williams",
    "mmw",
    "kim jones",
    "heron preston",
    "samuel ross",
    "a-cold-wall",
    "acw",
    "pigalle",
    "nigo",
    "human made",
    "brain dead",
    "awake ny",
    "aime leon dore",
    "ald",
    "new balance x",
    # Charity / special programs
    "doernbecher",
    "livestrong",
    # Seasonal specials that consistently command premiums
    "year of the dragon",
    "year of the rabbit",
    "year of the snake",
    "lunar new year",
    "chinese new year",
}
LIMITED_KEYWORDS = {"limited", "exclusive", "special box", "qs", "pe", "promo", "friends and family", "sample", "lottery", "raffle", "invitation only"}
HOT_MODELS = {
    # Jordan line — high-demand silhouettes
    "jordan 1", "jordan 2", "jordan 3", "jordan 4", "jordan 5",
    "jordan 6", "jordan 9", "jordan 10", "jordan 11", "jordan 12", "jordan 13",
    # Nike
    "dunk", "sb dunk",
    "air max 95", "air max 97", "air max 1", "air max 90", "air max 360",
    "air force 1",
    "kobe", "kobe 4", "kobe 6", "kobe 8", "kobe 9", "kobe protro",
    "lebron",
    "blazer mid", "cortez",
    # Adidas / Yeezy
    "yeezy boost", "yeezy slide", "yeezy foam",
    "samba", "gazelle", "campus", "handball spezial", "sl 72",
    "forum low", "superstar",
    # New Balance
    "new balance 550", "new balance 990", "new balance 2002",
    "new balance 327", "new balance 574", "new balance 1906",
    # Other notable silhouettes
    "onitsuka tiger",
    "gel-lyte", "gel-kayano",
    "chuck taylor", "one star",
    "speedcross",
}

MONTH_WORDS = (
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec",
    "january", "february", "march", "april", "june", "july", "august", "september", "october", "november", "december"
)

STOPWORDS = {
    "the", "and", "with", "of", "for", "to", "in", "on", "a", "an",
    "mens", "men", "women", "woman", "wmns", "unisex",
    "gs", "ps", "td", "infant", "toddler", "kids", "youth", "baby",
    "grade", "school", "boys", "girls",
    "shoe", "shoes", "sneaker", "sneakers", "boot", "boots",
    "coming", "soon", "release", "calendar", "launch", "drop",
    "from", "by", "via", "at",
    "2025", "2026", "2027",
}

ROMAN_MAP = {
    "i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6", "vii": "7", "viii": "8",
    "ix": "9", "x": "10", "xi": "11", "xii": "12", "xiii": "13", "xiv": "14", "xv": "15",
    "xvi": "16", "xvii": "17", "xviii": "18", "xix": "19", "xx": "20",
}

PUNCT_PAT = re.compile(r"[^a-z0-9\s]+")
WS_PAT = re.compile(r"\s+")
CURRENCY_PAT = re.compile(r"(?:(?:usd|cad|aud|eur|gbp)\s*)?[$£€]\s*\d{2,4}(?:\.\d{2})?", re.I)
COUNTDOWN_PAT = re.compile(r"\b\d{1,3}d:\d{1,2}h:\d{1,2}m:\d{1,2}s\b", re.I)
LEADING_MONTH_PAT = re.compile(
    r"^\s*(?:(" + "|".join(MONTH_WORDS) + r")\s+\d{1,2})(?:\s+\d{4})?\s+",
    re.I
)
COLOR_BLOB_PAT = re.compile(
    r"\b(?:white|black|red|blue|green|grey|gray|pink|purple|orange|yellow|brown|tan|beige|cream|navy|sail)\b"
    r"(?:\s*/\s*\b(?:white|black|red|blue|green|grey|gray|pink|purple|orange|yellow|brown|tan|beige|cream|navy|sail)\b)+",
    re.I
)
JORDAN_PAT = re.compile(r"\b(?:air\s+)?jordan\b", re.I)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge sources, compare changes, archive snapshots, validate quality.")
    parser.add_argument("--primary", type=Path, required=True)
    parser.add_argument("--fallback", type=Path, action="append", default=[])
    parser.add_argument("--previous", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--changes", type=Path, default=None)
    parser.add_argument("--archive-dir", type=Path, default=None)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--min-records", type=int, default=3)
    # Dedup tuning
    parser.add_argument("--fuzzy-threshold", type=float, default=0.92)
    parser.add_argument("--date-fuzz-days", type=int, default=1)  # merge within +/- N days
    return parser.parse_args()


def load_json(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


_BRAND_MAPPING: dict[str, str] = {
    "jordan": "Air Jordan",
    "air jordan": "Air Jordan",
    "nike": "Nike",
    "adidas": "Adidas",
    "new balance": "New Balance",
    "nb": "New Balance",
    "asics": "ASICS",
    "crocs": "Crocs",
    "converse": "Converse",
    "puma": "Puma",
    "reebok": "Reebok",
    "vans": "Vans",
    "saucony": "Saucony",
    "hoka": "Hoka",
    "on": "On Running",
    "on running": "On Running",
    "brooks": "Brooks",
    "mizuno": "Mizuno",
    "salomon": "Salomon",
    "new era": "New Era",
    "timberland": "Timberland",
    "ugg": "UGG",
    "under armour": "Under Armour",
    "ua": "Under Armour",
    "skechers": "Skechers",
    "fila": "Fila",
    "merrell": "Merrell",
    "keen": "Keen",
    "dc shoes": "DC Shoes",
    "dc": "DC Shoes",
    "etnies": "Etnies",
    "lacoste": "Lacoste",
    "diadora": "Diadora",
    "le coq sportif": "Le Coq Sportif",
    "karhu": "Karhu",
    "onitsuka tiger": "Onitsuka Tiger",
}


def normalize_brand(value: Any, shoe_name: str) -> str:
    brand = normalize_text(value)
    if brand:
        lowered = brand.lower()
        mapped = _BRAND_MAPPING.get(lowered)
        if mapped:
            return mapped
        # Partial match for compound entries
        for key, canonical in _BRAND_MAPPING.items():
            if key in lowered:
                return canonical
        return brand.title()

    lowered = shoe_name.lower()
    if "jordan" in lowered:
        return "Air Jordan"
    if "nike" in lowered or "dunk" in lowered or "air max" in lowered or "air force" in lowered or "pegasus" in lowered or "vomero" in lowered or "shox" in lowered:
        return "Nike"
    if "adidas" in lowered or "samba" in lowered or "gazelle" in lowered or "yeezy" in lowered or "superstar" in lowered or "campus" in lowered:
        return "Adidas"
    if "new balance" in lowered or re.search(r"\bnb\s*\d{3,4}\b", lowered):
        return "New Balance"
    if "asics" in lowered or "gel-" in lowered:
        return "ASICS"
    if "onitsuka" in lowered:
        return "Onitsuka Tiger"
    if "puma" in lowered:
        return "Puma"
    if "reebok" in lowered:
        return "Reebok"
    if "crocs" in lowered:
        return "Crocs"
    if "converse" in lowered or "chuck taylor" in lowered or "one star" in lowered:
        return "Converse"
    if "vans" in lowered or "old skool" in lowered or "sk8-hi" in lowered:
        return "Vans"
    if "saucony" in lowered:
        return "Saucony"
    if "hoka" in lowered or "clifton" in lowered or "bondi" in lowered:
        return "Hoka"
    if "salomon" in lowered or "xt-6" in lowered or "speedcross" in lowered:
        return "Salomon"
    if "timberland" in lowered:
        return "Timberland"
    if "under armour" in lowered or "curry" in lowered:
        return "Under Armour"
    if "lacoste" in lowered:
        return "Lacoste"
    return "Unknown"


def parse_price(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(round(value)))
    try:
        return max(0, int(round(float(str(value).replace("$", "").replace(",", "").strip()))))
    except ValueError:
        return 0


def _basic_clean(s: str) -> str:
    s = s.lower().strip()
    s = s.replace("&", "and")

    s = COUNTDOWN_PAT.sub(" ", s)
    s = CURRENCY_PAT.sub(" ", s)
    s = COLOR_BLOB_PAT.sub(" ", s)
    s = LEADING_MONTH_PAT.sub("", s)

    s = JORDAN_PAT.sub("air jordan", s)

    s = PUNCT_PAT.sub(" ", s)
    s = WS_PAT.sub(" ", s).strip()
    return s


def canonicalize_shoe_name(name: str, brand: str) -> str:
    """
    Aggressive canonicalizer to collapse:
      - 'Jordan Retro 13 BOYS GRADE SCHOOL ... $165' -> 'air jordan 13 retro'
      - 'Air Jordan 13 Retro White and University Red' -> 'air jordan 13 retro white university red'
    """
    raw = _basic_clean(name)

    tokens: list[str] = []
    for tok in raw.split():
        tok = ROMAN_MAP.get(tok, tok)
        if tok in STOPWORDS:
            continue
        tokens.append(tok)

    # Collapse 'air jordan' into marker for ordering rules
    out: list[str] = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens) and tokens[i] == "air" and tokens[i + 1] == "jordan":
            out.append("airjordan")
            i += 2
            continue
        out.append(tokens[i])
        i += 1
    tokens = out

    # Jordan ordering: airjordan <num> retro/og ...
    if "airjordan" in tokens:
        nums = [t for t in tokens if t.isdigit()]
        num = nums[0] if nums else ""
        tokens_wo_nums = [t for t in tokens if not t.isdigit()]
        rest = [t for t in tokens_wo_nums if t != "airjordan"]
        retro_bits = [t for t in rest if t in ("retro", "og")]
        rest = [t for t in rest if t not in ("retro", "og")]

        rebuilt = ["airjordan"]
        if num:
            rebuilt.append(num)
        if retro_bits:
            rebuilt.extend(retro_bits)
        # Keep a few remaining tokens but sort them to reduce ordering noise
        rebuilt.extend(sorted(rest))

        return " ".join(rebuilt).replace("airjordan", "air jordan").strip()

    # Generic: sort tokens to reduce “wording” differences
    return " ".join(sorted(tokens)).strip()


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


MID_BRANDS = {"vans", "converse", "puma", "reebok", "saucony", "new balance", "hoka", "salomon", "onitsuka tiger", "asics"}

def score_hype(brand: str, style: str, retail: int, resale: int | None) -> tuple[int, str]:
    score = 0
    lowered_style = (style + " " + brand).lower()
    lowered_brand = brand.lower()

    if lowered_brand in HIGH_BRANDS:
        score += 12
    elif lowered_brand in MID_BRANDS:
        score += 7
    else:
        score += 3

    if any(token in lowered_style for token in COLLAB_KEYWORDS):
        score += 20
    if any(token in lowered_style for token in LIMITED_KEYWORDS):
        score += 10
    if any(token in lowered_style for token in HOT_MODELS):
        score += 12

    # Lifestyle/heritage bonus
    if any(x in lowered_style for x in ("retro", "og", "original", "vintage", "heritage")):
        score += 4

    if retail >= 250:
        score += 3
    elif 0 < retail <= 110:
        score += 3

    if resale is not None and retail > 0:
        ratio = resale / retail
        spread = resale - retail

        if ratio >= 2.5:
            score += 35
        elif ratio >= 2.0:
            score += 28
        elif ratio >= 1.5:
            score += 18
        elif ratio >= 1.2:
            score += 8

        if spread >= 200:
            score += 15
        elif spread >= 150:
            score += 10
        elif spread >= 75:
            score += 6
        elif spread >= 30:
            score += 2

    if score >= 42:
        return score, "HIGH"
    if score >= 20:
        return score, "MED"
    return score, "LOW"


def score_confidence(record: dict[str, Any]) -> tuple[int, str]:
    score = 0
    if record.get("sourcePrimary"):
        score += 25
    if record.get("sourceSecondary"):
        score += 15
    if record.get("retailPrice", 0) > 0:
        score += 15
    if record.get("imageUrl"):
        score += 10
    if record.get("releaseUrl"):
        score += 10
    matched = record.get("matchedSources", 0)
    if matched >= 3:
        score += 28
    elif matched >= 2:
        score += 18

    # A release seen by only one source can never be HIGH confidence —
    # cap just below threshold regardless of how many fields it has.
    if matched <= 1:
        score = min(score, 59)

    if score >= 60:
        return score, "HIGH"
    if score >= 32:
        return score, "MED"
    return score, "LOW"


def derive_priority(hype: str, confidence: str) -> str:
    if hype == "HIGH" and confidence == "HIGH":
        return "Must Watch"
    if (hype == "HIGH" and confidence == "MED") or (hype == "MED" and confidence == "HIGH"):
        return "Watch"
    return "Low Priority"


def derive_tags(style: str, brand: str = "") -> list[str]:
    lowered = (style + " " + brand).lower()
    tags: list[str] = []

    if any(token in lowered for token in COLLAB_KEYWORDS):
        tags.append("collab")
    if any(token in lowered for token in HOT_MODELS):
        tags.append("hot-model")
    if any(x in lowered for x in ("retro", "og", "original")):
        tags.append("retro")
    if any(token in lowered for token in ("running", "pegasus", "vomero", "air max", "zoom", "react", "infinity run", "clifton", "bondi", "speedcross")):
        tags.append("running")
    if any(token in lowered for token in ("lebron", "kobe", "kd ", "sabrina", "basketball", "kyrie", "curry", "boozer")):
        tags.append("basketball")
    if any(token in lowered for token in ("sb dunk", "skate", "pro sb", "etnies", "dc shoe", "janoski")):
        tags.append("skateboarding")
    if any(token in lowered for token in ("trail", "speedcross", "xt-6", "xt6", "wildcat", "speedgoat", "terrex", "salomon")):
        tags.append("trail")
    if any(token in lowered for token in ("samba", "gazelle", "campus", "superstar", "stan smith", "chuck", "one star", "old skool", "sk8")):
        tags.append("lifestyle")
    if any(token in lowered for token in ("women", "wmns", "woman")):
        tags.append("women")
    if any(token in lowered for token in LIMITED_KEYWORDS):
        tags.append("exclusive")
    if any(token in lowered for token in ("kids", "gs", "grade school", "infant", "toddler", "youth", "junior", "boys", "girls")):
        tags.append("kids")

    return tags


def make_key(record: dict[str, Any]) -> tuple[str, str]:
    d = normalize_text(record.get("releaseDate"))
    name = normalize_text(record.get("shoeName"))
    brand = normalize_text(record.get("brand"))
    canonical = canonicalize_shoe_name(name, brand)
    return (d, canonical)


def choose_better(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    def quality(item: dict[str, Any]) -> int:
        return (
            int(bool(item.get("imageUrl")))
            + int(parse_price(item.get("retailPrice")) > 0)
            + int(parse_price(item.get("estimatedMarketValue")) > 0)
            + int(bool(item.get("sourceSecondary")))
            + int(bool(item.get("releaseUrl")))
        )

    return b if quality(b) > quality(a) else a


def normalize_record(row: dict[str, Any], default_source: str) -> dict[str, Any] | None:
    release_date = normalize_text(row.get("releaseDate"))
    shoe_name = normalize_text(row.get("shoeName"))
    if not release_date or not shoe_name:
        return None
    if parse_date(release_date) is None:
        return None

    source_primary = normalize_text(row.get("sourcePrimary")) or normalize_text(row.get("source")) or default_source

    return {
        "releaseDate": release_date,
        "shoeName": shoe_name,
        "brand": normalize_brand(row.get("brand"), shoe_name),
        "retailPrice": parse_price(row.get("retailPrice")),
        "estimatedMarketValue": (
            parse_price(row.get("estimatedMarketValue")) if row.get("estimatedMarketValue") not in (None, "") else None
        ),
        "imageUrl": normalize_text(row.get("imageUrl")) or None,
        "sourcePrimary": source_primary,
        "sourceSecondary": normalize_text(row.get("sourceSecondary")) or None,
        "sourceUrl": normalize_text(row.get("sourceUrl")) or None,
        "releaseUrl": normalize_text(row.get("releaseUrl") or row.get("sourceUrl")) or None,
        "releaseMethod": normalize_text(row.get("releaseMethod")) or "",
    }


def _dates_within(a: str, b: str, days: int) -> bool:
    da = parse_date(a)
    db = parse_date(b)
    if da is None or db is None:
        return False
    return abs((da - db).days) <= days


def merge_records(
    primary: list[dict[str, Any]],
    fallbacks: list[list[dict[str, Any]]],
    fuzzy_threshold: float,
    date_fuzz_days: int,
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    sources = [("primary", primary)]
    sources.extend((f"fallback_{idx+1}", rows) for idx, rows in enumerate(fallbacks))

    for source_name, rows in sources:
        for row in rows:
            normalized = normalize_record(row, default_source=source_name)
            if normalized is None:
                continue

            key = make_key(normalized)

            # Fuzzy match across existing keys within +/- date_fuzz_days
            if key not in merged:
                best_k = None
                best_s = 0.0
                for k in merged.keys():
                    if not _dates_within(key[0], k[0], date_fuzz_days):
                        continue
                    s = similarity(key[1], k[1])
                    if s > best_s:
                        best_s = s
                        best_k = k
                if best_k and best_s >= fuzzy_threshold:
                    key = best_k

            existing = merged.get(key)

            if existing is None:
                normalized["matchedSources"] = 1
                normalized["_sources"] = {normalized["sourcePrimary"]}
                merged[key] = normalized
                continue

            picked = choose_better(existing, normalized)
            if picked is existing:
                if normalized["sourcePrimary"] and normalized["sourcePrimary"] != existing.get("sourcePrimary"):
                    existing["sourceSecondary"] = existing.get("sourceSecondary") or normalized["sourcePrimary"]
                existing["_sources"].add(normalized["sourcePrimary"])
                existing["matchedSources"] = len(existing["_sources"])
                if not existing.get("sourceUrl") and normalized.get("sourceUrl"):
                    existing["sourceUrl"] = normalized["sourceUrl"]
                if not existing.get("releaseUrl") and normalized.get("releaseUrl"):
                    existing["releaseUrl"] = normalized["releaseUrl"]
                if not existing.get("imageUrl") and normalized.get("imageUrl"):
                    existing["imageUrl"] = normalized["imageUrl"]
                continue

            # normalized wins — carry over the best fields from existing rather than discarding them
            picked["matchedSources"] = int(existing.get("matchedSources", 1)) + 1
            picked["_sources"] = existing.get("_sources", {existing.get("sourcePrimary", "")})
            picked["_sources"].add(picked.get("sourcePrimary", ""))
            picked["matchedSources"] = len(picked["_sources"])
            if existing.get("sourcePrimary") and existing.get("sourcePrimary") != picked.get("sourcePrimary"):
                picked["sourceSecondary"] = picked.get("sourceSecondary") or existing["sourcePrimary"]
            if not picked.get("retailPrice") and existing.get("retailPrice"):
                picked["retailPrice"] = existing["retailPrice"]
            if picked.get("estimatedMarketValue") is None and existing.get("estimatedMarketValue") is not None:
                picked["estimatedMarketValue"] = existing["estimatedMarketValue"]
            if not picked.get("imageUrl") and existing.get("imageUrl"):
                picked["imageUrl"] = existing["imageUrl"]
            if not picked.get("sourceUrl") and existing.get("sourceUrl"):
                picked["sourceUrl"] = existing["sourceUrl"]
            if not picked.get("releaseUrl") and existing.get("releaseUrl"):
                picked["releaseUrl"] = existing["releaseUrl"]
            if not picked.get("releaseMethod") and existing.get("releaseMethod"):
                picked["releaseMethod"] = existing["releaseMethod"]
            merged[key] = picked

    final_rows: list[dict[str, Any]] = []
    for row in merged.values():
        row.pop("_sources", None)  # internal tracking only
        hype_score, hype = score_hype(row["brand"], row["shoeName"], row["retailPrice"], row["estimatedMarketValue"])
        confidence_score, confidence = score_confidence(row)

        row["hypeScore"] = hype_score
        row["hype"] = hype
        row["confidenceScore"] = confidence_score
        row["confidence"] = confidence
        row["priority"] = derive_priority(hype, confidence)
        row["tags"] = derive_tags(row["shoeName"], row.get("brand", ""))

        # releaseMethod: keep scraper value if present, otherwise infer from name
        if not row.get("releaseMethod"):
            row["releaseMethod"] = infer_release_method(row["shoeName"])

        _retail = row.get("retailPrice") or 0
        _market = row.get("estimatedMarketValue") or 0
        row["flipScore"] = (
            round((_market - _retail) / _retail * 100)
            if _retail > 0 and _market > 0
            else None
        )

        row["recordHash"] = hashlib.sha256(
            json.dumps(
                {
                    "releaseDate": row["releaseDate"],
                    "shoeName": row["shoeName"],
                    "brand": row["brand"],
                    "retailPrice": row["retailPrice"],
                    "estimatedMarketValue": row["estimatedMarketValue"],
                    "sourcePrimary": row["sourcePrimary"],
                    "sourceSecondary": row["sourceSecondary"],
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

        final_rows.append(row)

    return sorted(final_rows, key=lambda i: (i["releaseDate"], i["brand"].lower(), i["shoeName"].lower()))


def compare_changes(previous: list[dict[str, Any]], current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    previous_map = {make_key(row): row for row in previous if make_key(row) != ("", "")}
    current_map = {make_key(row): row for row in current if make_key(row) != ("", "")}

    changes: list[dict[str, Any]] = []
    detected_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    for key, row in current_map.items():
        if key not in previous_map:
            changes.append(
                {
                    "changeType": "NEW",
                    "date": row.get("releaseDate"),
                    "brand": row.get("brand"),
                    "style": row.get("shoeName"),
                    "fieldChanged": "",
                    "oldValue": "",
                    "newValue": "",
                    "detectedAt": detected_at,
                }
            )
            continue

        old = previous_map[key]
        fields = [
            ("releaseDate", "DATE_CHANGED"),
            ("retailPrice", "RETAIL_CHANGED"),
            ("estimatedMarketValue", "MARKET_CHANGED"),
            ("sourcePrimary", "SOURCE_CHANGED"),
            ("sourceSecondary", "SOURCE_CHANGED"),
            ("confidence", "CONFIDENCE_CHANGED"),
            ("priority", "PRIORITY_CHANGED"),
        ]
        for field_name, change_type in fields:
            if old.get(field_name) != row.get(field_name):
                changes.append(
                    {
                        "changeType": change_type,
                        "date": row.get("releaseDate"),
                        "brand": row.get("brand"),
                        "style": row.get("shoeName"),
                        "fieldChanged": field_name,
                        "oldValue": old.get(field_name, ""),
                        "newValue": row.get(field_name, ""),
                        "detectedAt": detected_at,
                    }
                )

    for key, row in previous_map.items():
        if key not in current_map:
            changes.append(
                {
                    "changeType": "REMOVED",
                    "date": row.get("releaseDate"),
                    "brand": row.get("brand"),
                    "style": row.get("shoeName"),
                    "fieldChanged": "",
                    "oldValue": "",
                    "newValue": "",
                    "detectedAt": detected_at,
                }
            )

    return sorted(changes, key=lambda i: (i.get("date") or "", i.get("changeType") or "", (i.get("style") or "").lower()))


def validate_records(rows: list[dict[str, Any]], min_records: int) -> None:
    if len(rows) < min_records:
        raise SystemExit(f"Validation failed: only {len(rows)} record(s), expected at least {min_records}")

    low_confidence = sum(1 for row in rows if str(row.get("confidence", "")).upper() == "LOW")
    if rows and (low_confidence / len(rows)) > 0.9:
        raise SystemExit("Validation failed: more than 90% of rows are low confidence")


def write_json(path: Path | None, data: list[dict[str, Any]]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def archive_snapshot(archive_dir: Path | None, rows: list[dict[str, Any]]) -> None:
    if archive_dir is None:
        return
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    (archive_dir / f"final_releases_{stamp}.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()

    if args.validate_only:
        rows = load_json(args.primary)
        validate_records(rows, min_records=args.min_records)
        print(f"Validated rows: {len(rows)}")
        return

    primary_rows = load_json(args.primary)
    fallback_rows = [load_json(p) for p in (args.fallback or [])]
    previous_rows = load_json(args.previous)

    merged_rows = merge_records(
        primary_rows,
        fallback_rows,
        fuzzy_threshold=float(args.fuzzy_threshold),
        date_fuzz_days=int(args.date_fuzz_days),
    )

    # Carry forward any previous records with releaseDate >= today that the
    # current scrape missed (prevents same-day drops from disappearing mid-day).
    if previous_rows:
        today = date.today()
        merged_keys = {make_key(row) for row in merged_rows}
        carried = [
            row for row in previous_rows
            if (parse_date(row.get("releaseDate", "")) or date.min) >= today
            and make_key(row) not in merged_keys
        ]
        if carried:
            print(f"Carried forward from previous: {len(carried)}")
            merged_rows = sorted(
                merged_rows + carried,
                key=lambda i: (i["releaseDate"], i.get("brand", "").lower(), i.get("shoeName", "").lower()),
            )

    changes = compare_changes(previous_rows, merged_rows)

    validate_records(merged_rows, min_records=args.min_records)
    write_json(args.output, merged_rows)
    write_json(args.changes, changes)
    archive_snapshot(args.archive_dir, merged_rows)

    print(f"Primary rows: {len(primary_rows)}")
    print(f"Fallback files: {len(fallback_rows)}")
    print(f"Merged rows: {len(merged_rows)}")
    print(f"Detected changes: {len(changes)}")


if __name__ == "__main__":
    main()
