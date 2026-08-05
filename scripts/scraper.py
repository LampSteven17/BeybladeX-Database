"""
WBO Forum Scraper for Beyblade X Winning Combinations Thread.

Scrapes tournament results from:
https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX

Features:
- Deduplication via WBO post IDs
- Bit abbreviation expansion
- Improved tournament name/location parsing
"""

import re
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

import cloudscraper
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from db import get_connection, init_schema, normalize_data, parse_cx_blade, infer_region


BASE_URL = "https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX"

# Use browser-like headers to avoid Cloudflare blocking
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# =============================================================================
# Catalog-derived lookups (CANONICAL_BLADES, BLADE_NORMALIZATION, KNOWN_*,
# ASSIST_ABBREVIATIONS, BIT_ABBREVIATIONS) — all populated lazily from
# scripts/catalog.py:PartsCatalog. Adding a new wiki part = zero edits here.
# =============================================================================

_LAZY_CACHE: dict = {}


def _build_lazy(name: str):
    """Build one of the legacy module-level constants from the parts catalog.

    Cached. Call PartsCatalog.reload() and clear _LAZY_CACHE to refresh after
    a fandom scrape.
    """
    if name in _LAZY_CACHE:
        return _LAZY_CACHE[name]
    from catalog import PartsCatalog
    cat = PartsCatalog.get()

    if name == "CANONICAL_BLADES":
        # All BX/UX standalone blades + every CX full combo name
        value = set(cat.blades) | set(cat.cx_full_names)
    elif name == "KNOWN_BLADES":
        value = _build_lazy("CANONICAL_BLADES") | {"Venom"}
    elif name == "KNOWN_ASSISTS":
        value = set(cat.assists)
    elif name == "KNOWN_ASSISTS_LOWER":
        value = {a.lower(): a for a in cat.assists}
    elif name == "ASSIST_ABBREVIATIONS":
        # Catalog already merged parser_aliases.ASSIST_LETTER_ABBREVIATIONS in
        value = dict(cat.assist_aliases)
    elif name == "BIT_ABBREVIATIONS":
        value = dict(cat.bit_aliases)
    elif name == "BLADE_NORMALIZATION":
        # Build no-space + swapped + camelcase keys -> canonical
        canonical = _build_lazy("CANONICAL_BLADES")
        bn = {}
        for blade in canonical:
            bn[blade.lower().replace(" ", "")] = blade
            words = blade.split()
            if len(words) == 2:
                swapped = f"{words[1]} {words[0]}"
                bn[swapped.lower().replace(" ", "")] = blade
                bn[swapped.lower()] = blade
        bn["hellscythe"] = "Hells Scythe"  # missing 's' typo
        bn["dranzerspiral"] = "Dranzer Spiral"
        value = bn
    elif name == "KNOWN_BLADES_LOWER":
        kb = _build_lazy("KNOWN_BLADES")
        kbl = {b.lower(): b for b in kb}
        for blade in kb:
            no_space = blade.lower().replace(" ", "")
            kbl.setdefault(no_space, blade)
            words = blade.split()
            if len(words) == 2:
                swapped = f"{words[1]} {words[0]}"
                kbl.setdefault(swapped.lower(), blade)
                kbl.setdefault(swapped.lower().replace(" ", ""), blade)
        value = kbl
    else:
        raise AttributeError(name)

    _LAZY_CACHE[name] = value
    return value


def __getattr__(name):
    """PEP 562 module-level lazy attribute resolver."""
    if name in {
        "CANONICAL_BLADES",
        "BLADE_NORMALIZATION",
        "KNOWN_BLADES",
        "KNOWN_BLADES_LOWER",
        "KNOWN_ASSISTS",
        "KNOWN_ASSISTS_LOWER",
        "ASSIST_ABBREVIATIONS",
        "BIT_ABBREVIATIONS",
    }:
        return _build_lazy(name)
    raise AttributeError(f"module 'scraper' has no attribute {name!r}")


def normalize_blade(blade: str) -> str:
    """Normalize a blade name to its canonical form using the catalog.

    Handles swapped word order, no-space, and CamelCase variants.
    """
    blade = blade.strip().lstrip("-").strip()
    bn = _build_lazy("BLADE_NORMALIZATION")

    key = blade.lower().replace(" ", "")
    if key in bn:
        return bn[key]

    key_with_space = blade.lower()
    if key_with_space in bn:
        return bn[key_with_space]

    words = blade.split()
    if len(words) == 2:
        swapped_key = f"{words[1]}{words[0]}".lower()
        if swapped_key in bn:
            return bn[swapped_key]

    camel_split = re.sub(r"([a-z])([A-Z])", r"\1 \2", blade)
    camel_key = camel_split.lower().replace(" ", "")
    if camel_key in bn:
        return bn[camel_key]

    camel_words = camel_split.split()
    if len(camel_words) == 2:
        swapped_camel = f"{camel_words[1]}{camel_words[0]}".lower()
        if swapped_camel in bn:
            return bn[swapped_camel]

    if " " in camel_split:
        return camel_split.title()
    if " " in blade:
        return blade.title()
    return blade




@dataclass
class Combo:
    blade: str
    ratchet: str
    bit: str
    assist: Optional[str] = None
    lock_chip: Optional[str] = None
    over_blade: Optional[str] = None  # CX-only: Break, Flow, Guard
    stage: Optional[str] = None  # 'first', 'final', 'both', or None


@dataclass
class Placement:
    place: int
    player_name: str
    player_wbo_id: Optional[str]
    combos: list[Combo] = field(default_factory=list)


@dataclass
class Tournament:
    wbo_post_id: str
    name: str
    date: Optional[datetime]
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    format: Optional[str] = None
    ranked: Optional[bool] = None
    wbo_url: Optional[str] = None
    placements: list[Placement] = field(default_factory=list)


def expand_bit(bit: str) -> str:
    """Expand bit abbreviations to full names using the catalog."""
    bit = bit.strip()
    return _build_lazy("BIT_ABBREVIATIONS").get(bit, bit)


def split_blade_over_assist(blade_text: str) -> tuple[str, Optional[str], Optional[str]]:
    """
    Split blade text into (blade, over_blade, assist).

    CX combos format: "[LockChip] [MainBlade] [OverBlade?] [Assist?]"
      - Over blades (Break/Flow/Guard) sit between the main blade and the assist
      - Either or both of over_blade and assist may be missing
      - Both are CX-only

    Strategy: walk tokens from the end. The trailing 1-2 tokens may be an assist
    (multi-word like "Low Rush", or single-word like "Wheel", or letter like "S").
    The token immediately before that may be an over blade (Break/Flow/Guard).
    Whatever's left is the blade.

    Examples:
      "Pegasus Blast Wheel"            -> ("Pegasus Blast", None, "Wheel")
      "Pegasus Blast Guard Wheel"      -> ("Pegasus Blast", "Guard", "Wheel")
      "Pegasus Blast Guard"            -> ("Pegasus Blast", "Guard", None)
      "Eva Blast S"                    -> ("Eva Blast", None, "Slash")
      "Wizard Rod"                     -> ("Wizard Rod", None, None)
    """
    from catalog import PartsCatalog
    cat = PartsCatalog.get()
    over_blades_lower = cat.over_blades_lower
    assist_aliases = _build_lazy("ASSIST_ABBREVIATIONS")
    known_assists_lower = _build_lazy("KNOWN_ASSISTS_LOWER")
    known_assists = _build_lazy("KNOWN_ASSISTS")

    blade_text = blade_text.strip()
    words = blade_text.split()
    if len(words) <= 1:
        return normalize_blade(blade_text), None, None

    over_blade: Optional[str] = None
    assist: Optional[str] = None

    # Step 1: Strip a trailing assist (one or two words)
    # Try two-word assist like "Low Rush" first if there are enough words
    if len(words) >= 3:
        last_two = " ".join(words[-2:])
        if last_two.lower() in {a.lower() for a in known_assists}:
            assist = last_two
            words = words[:-2]

    if assist is None and words:
        last_word = words[-1]
        if (
            last_word in assist_aliases
            or last_word.lower() in known_assists_lower
            or (len(last_word) <= 2 and last_word.isupper())
        ):
            assist = assist_aliases.get(last_word, last_word)
            words = words[:-1]

    # Step 2: Strip a trailing over blade (Break / Flow / Guard)
    if words and words[-1].lower() in over_blades_lower:
        over_blade = over_blades_lower[words[-1].lower()]
        words = words[:-1]

    # Step 3: Whatever remains is the blade
    blade = normalize_blade(" ".join(words)) if words else blade_text
    return blade, over_blade, assist


def split_blade_assist(blade_text: str) -> tuple[str, Optional[str]]:
    """Legacy two-tuple shim around split_blade_over_assist().

    Kept so callers that don't care about over blades still work. New code
    should call split_blade_over_assist() and handle the over_blade slot.
    """
    blade, _over, assist = split_blade_over_assist(blade_text)
    return blade, assist


def parse_combo(combo_str: str) -> Optional[Combo]:
    """
    Parse a combo string like 'DranSword 3-60F' or 'Courage Dran S 6-60V'

    Format: [Blade] [Assist?] [Ratchet][Bit]
    - Blade: Main blade name (e.g., "Courage Dran", "Wizard Rod")
    - Assist: Optional assist blade, usually single letter or short name (e.g., "S", "Jaggy")
    - Ratchet: X-XX format (e.g., "3-60", "9-80")
    - Bit: Tip abbreviation or name (e.g., "F", "Ball", "Rush")

    Handles annotations like "(Both Stages)" or "(3on3 Finals Only)"
    """
    # Strip whitespace and leading dashes/bullets
    combo_str = combo_str.strip().lstrip("-•*").strip()
    if not combo_str:
        return None

    # Strip player name prefix (e.g., "geetster99: SolBlast..." -> "SolBlast...")
    # Also handles bare colon prefix (e.g., ": SolBlast..." from HTML parsing)
    colon_match = re.match(r'^[^:]*:\s*([A-Za-z].+)$', combo_str)
    if colon_match:
        combo_str = colon_match.group(1).strip()

    # Extract stage info before removing annotations
    stage = None
    stage_match = re.search(r"\(([^)]*(?:Stage|Finals)[^)]*)\)", combo_str, flags=re.I)
    if stage_match:
        stage_text = stage_match.group(1).lower()
        if "both" in stage_text or ("first" in stage_text and "final" in stage_text):
            stage = "both"
        elif "final" in stage_text:
            stage = "final"
        elif "first" in stage_text:
            stage = "first"
        # else: could be "3on3 Finals Only" etc - treat as 'final'
        elif "finals" in stage_text:
            stage = "final"

    # Remove stage/format annotations in parentheses
    combo_str = re.sub(
        r"\s*\([^)]*(?:Stage|Finals|Only|Match|Type)[^)]*\)", "", combo_str, flags=re.I
    )
    # Remove other parenthesized annotations (product codes, notes, etc.)
    # e.g., "(UX-03)", "(Upper Type)", "(Both)", "(Deck)"
    combo_str = re.sub(r"\s*\([^)]*\)\s*$", "", combo_str)
    # Strip leading list numbering (e.g., "1.) Phoenix Wing", "2. Shark Edge")
    combo_str = re.sub(r"^\s*\d*[.)\]]+\s*", "", combo_str)
    combo_str = combo_str.strip()
    if not combo_str:
        return None

    # Pattern: [Blade + optional Assist] [Ratchet][Bit]
    # Ratchet is X-XX format, bit can be attached or separate

    # Try: Everything + Ratchet + Bit (with space before bit)
    match = re.match(r"^(.+?)\s+((?:\d{1,2}|M)-\d{2,3})\s+([A-Za-z][A-Za-z\s]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        ratchet = match.group(2).strip()
        bit = expand_bit(match.group(3).strip())
        blade, over_blade, assist = split_blade_over_assist(blade_part)
        lock_chip, blade = parse_cx_blade(blade)
        return Combo(
            blade=blade,
            ratchet=ratchet,
            bit=bit,
            assist=assist,
            lock_chip=lock_chip,
            over_blade=over_blade,
            stage=stage,
        )

    # Try: Everything + Ratchet+Bit (no space, bit attached like 3-60F or 6-60V or 4-50Low Rush)
    match = re.match(r"^(.+?)\s+((?:\d{1,2}|M)-\d{2,3})([A-Z][A-Za-z\s]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        ratchet = match.group(2).strip()
        bit = expand_bit(match.group(3).strip())
        blade, over_blade, assist = split_blade_over_assist(blade_part)
        lock_chip, blade = parse_cx_blade(blade)
        return Combo(
            blade=blade,
            ratchet=ratchet,
            bit=bit,
            assist=assist,
            lock_chip=lock_chip,
            over_blade=over_blade,
            stage=stage,
        )

    # Try: Blade + AssistRatchetBit (assist concatenated with ratchet, e.g., "FoxBlast Wheel9-60Hexa")
    match = re.match(r"^(.+?)\s+([A-Za-z]+)((?:\d{1,2}|M)-\d{2,3})([A-Z][A-Za-z\s]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        potential_assist = match.group(2).strip()
        ratchet = match.group(3).strip()
        bit = expand_bit(match.group(4).strip())

        _assist_aliases = _build_lazy("ASSIST_ABBREVIATIONS")
        _known_assists_lower = _build_lazy("KNOWN_ASSISTS_LOWER")
        if potential_assist in _assist_aliases or potential_assist.lower() in _known_assists_lower:
            assist = _assist_aliases.get(potential_assist, potential_assist)
            # blade_part may contain an over blade trailing token
            blade, over_blade, _ = split_blade_over_assist(blade_part)
            lock_chip, blade = parse_cx_blade(blade)
            return Combo(
                blade=blade,
                ratchet=ratchet,
                bit=bit,
                assist=assist,
                lock_chip=lock_chip,
                over_blade=over_blade,
                stage=stage,
            )

    # Fallback: Ratchet Integrated Bits (Operate/Turbo) - no ratchet pattern
    RATCHET_INTEGRATED_BITS = {"Operate", "Turbo"}
    for rib in RATCHET_INTEGRATED_BITS:
        if combo_str.endswith(rib) or combo_str.endswith(rib.lower()):
            blade_part = combo_str[: -len(rib)].strip()
            if blade_part:
                blade, over_blade, assist = split_blade_over_assist(blade_part)
                lock_chip, blade = parse_cx_blade(blade)
                return Combo(
                    blade=blade,
                    ratchet=rib,
                    bit=rib,
                    assist=assist,
                    lock_chip=lock_chip,
                    over_blade=over_blade,
                    stage=stage,
                )

    return None


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date from various formats."""
    date_str = date_str.strip()
    # Normalize abbreviated months with periods: "Aug." -> "Aug"
    date_str = re.sub(r'\b([A-Z][a-z]{2})\.\s', r'\1 ', date_str)
    formats = [
        "%m/%d/%y",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%m-%d-%y",
        "%m-%d-%Y",
        "%m.%d.%y",
        "%m.%d.%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
        "%Y-%m-%d",
    ]
    now = datetime.now()
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)

            # Fix 2-digit years that weren't converted correctly
            # (can happen with %y in some locales or with %Y matching short years)
            if parsed.year < 100:
                parsed = parsed.replace(year=parsed.year + 2000)

            # Beyblade X era validation (2023 onwards, no future dates)
            if parsed.year < 2023:
                continue
            if parsed > now:
                continue

            return parsed
        except ValueError:
            continue
    return None


# infer_region is now imported from db.py for consistency across all scrapers


def extract_date_from_text(text: str) -> tuple[Optional[datetime], int, int]:
    """
    Extract date from text, returns (date, start_pos, end_pos).
    """
    # Try MM/DD/YY or MM/DD/YYYY or DD/MM/YYYY
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    if match:
        date = parse_date(match.group(1))
        if date:
            return date, match.start(), match.end()

    # Try "Month DD, YYYY" or "Month DD YYYY" (with optional abbreviated period)
    match = re.search(r"([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})", text)
    if match:
        date = parse_date(match.group(1))
        if date:
            return date, match.start(), match.end()

    # Try dash dates: M-D-YY, MM-DD-YYYY, etc.
    match = re.search(r"(\d{1,2}-\d{1,2}-\d{2,4})", text)
    if match:
        date = parse_date(match.group(1))
        if date:
            return date, match.start(), match.end()

    # Try dot dates: M.D.YY, MM.DD.YYYY, etc.
    match = re.search(r"(\d{1,2}\.\d{1,2}\.\d{2,4})", text)
    if match:
        date = parse_date(match.group(1))
        if date:
            return date, match.start(), match.end()

    # Try ISO: YYYY-MM-DD
    match = re.search(r"(\d{4}-\d{1,2}-\d{1,2})", text)
    if match:
        date = parse_date(match.group(1))
        if date:
            return date, match.start(), match.end()

    return None, -1, -1


def parse_location(text: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse location string into (city, state, country).
    Handles formats like:
    - "Toronto, ON, Canada"
    - "Burnaby, Canada"
    - "City | Country"
    """
    # Clean up separators - normalize | to ,
    text = text.replace("|", ",")

    # Remove common non-location words
    text = re.sub(
        r"\b(X Format|Ranked|Unranked|1on1|3on3|1v1|3v3|Experimental|Beyblade X)\b",
        "",
        text,
        flags=re.I,
    )
    text = text.strip(" -,")

    parts = [p.strip(" -") for p in text.split(",") if p.strip(" -")]

    # Clean up each part
    parts = [p for p in parts if p and len(p) > 0]

    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return parts[0], None, parts[1]
    elif len(parts) == 1 and len(parts[0]) > 1:
        return parts[0], None, None

    return None, None, None


def extract_format_info(text: str) -> tuple[Optional[str], Optional[bool]]:
    """Extract tournament format and ranked status from text."""
    text_lower = text.lower()

    format_type = None
    if "1on1" in text_lower or "1v1" in text_lower:
        format_type = "1on1"
    elif "3on3" in text_lower or "3v3" in text_lower:
        format_type = "3on3"

    ranked = None
    if "unranked" in text_lower:
        ranked = False
    elif "ranked" in text_lower:
        ranked = True

    return format_type, ranked


def is_beyblade_x_content(lines: list[str]) -> bool:
    """
    Check if the post content is about Beyblade X (not Metal Fight, Burst, etc).
    Look for X-format ratchet patterns (X-XX like 3-60, 4-80).

    Posts with BOTH BBX and MF indicators are accepted (multi-format events).
    Only reject posts with MF indicators and NO BBX indicators.
    """
    text = " ".join(lines[:30])  # Check first 30 lines

    # Beyblade X indicators (ratchet pattern X-XX)
    x_pattern = r"\b(?:\d{1,2}|M)-\d{2,3}[A-Z]"  # Like 3-60F, 4-80B
    has_bbx = bool(re.search(x_pattern, text))
    if not has_bbx:
        has_bbx = bool(re.search(r"Beyblade\s*X|X\s*Format", text, re.I))

    # If there's clear BBX content, accept it even if MF indicators are present
    if has_bbx:
        return True

    # No BBX content found - reject (whether or not MF indicators are present)
    return False


def parse_header_lines(lines: list[str]) -> dict:
    """
    Parse the first few lines to extract tournament header info.
    Handles multi-line headers where name, date, and location may be on separate lines.

    Common formats observed:
    1. "Tournament Name" / "- MM/DD/YY" / "City, State, Country - X Format - Unranked 1on1"
    2. "Tournament Name | X Format (Unranked)" / "City, Country | Month DD, YYYY"
    3. "Tournament Name" / "Month DD, YYYY" / "Beyblade X"
    """
    result = {
        "name": None,
        "date": None,
        "city": None,
        "state": None,
        "country": None,
        "format": None,
        "ranked": None,
    }

    if not lines:
        return result

    # Look at first 10 lines for header info (some posts have longer headers)
    header_lines = lines[:10]
    combined_text = " ".join(header_lines)
    # Strip "Date:" prefix so "Date: 2-4-2025" becomes "2-4-2025"
    combined_text_for_date = re.sub(r"\bDate:\s*", "", combined_text)

    # Extract date from combined text
    date, date_start, date_end = extract_date_from_text(combined_text_for_date)
    result["date"] = date

    # Extract format info from combined text
    fmt, ranked = extract_format_info(combined_text)
    result["format"] = fmt
    result["ranked"] = ranked

    # Now figure out tournament name and location
    # Strategy: first non-date, non-location line is likely the name

    name_line = None
    location_line = None

    for line in header_lines:
        line_clean = line.strip().lstrip("-").strip()
        if not line_clean:
            continue

        # Skip lines that are just dates
        if re.match(r"^[\d/]+$", line_clean) or re.match(
            r"^[A-Z][a-z]+ \d{1,2},? \d{4}$", line_clean
        ):
            continue

        # Skip lines that are just format indicators
        if re.match(
            r"^(Beyblade X|X Format|Ranked|Unranked|1on1|3on3)$", line_clean, re.I
        ):
            continue

        # Skip placement lines (any format)
        if re.match(r"^(1st|2nd|3rd|First|Second|Third|[🥇🥈🥉]|[123]\.)", line_clean, re.I):
            break

        # Skip common noise
        if line_clean.lower() in [
            "winning combos",
            "top 3 photo",
            "(click to view)",
            "top 3 deck combos",
            "!",
        ]:
            continue

        # Check if this line looks like a location (has commas and country-like words)
        has_location_pattern = bool(
            re.search(
                r",\s*(Canada|USA|US|UK|Japan|Australia|Germany|France)",
                line_clean,
                re.I,
            )
        )
        has_date_in_line = bool(
            re.search(
                r"\d{1,2}/\d{1,2}/\d{2,4}|[A-Z][a-z]+ \d{1,2},? \d{4}", line_clean
            )
        )

        if has_location_pattern or (has_date_in_line and "," in line_clean):
            # This is likely a location line (possibly with date)
            if location_line is None:
                # Extract location part (before or after date)
                loc_text = re.sub(r"\d{1,2}/\d{1,2}/\d{2,4}", "", line_clean)
                loc_text = re.sub(r"[A-Z][a-z]+ \d{1,2},? \d{4}", "", loc_text)
                city, state, country = parse_location(loc_text)
                if city or country:
                    result["city"] = city
                    result["state"] = state
                    result["country"] = country
                    location_line = line_clean
        elif name_line is None and len(line_clean) > 3:
            # First substantial line that's not location/date is probably the name
            # Clean it up
            name = line_clean
            # Remove trailing format indicators
            name = re.sub(
                r"\s*[-|]\s*(X Format|Ranked|Unranked|1on1|3on3).*$",
                "",
                name,
                flags=re.I,
            )
            name = re.sub(r"\s*\|\s*$", "", name)
            name = name.strip()
            if name and len(name) > 2:
                result["name"] = name
                name_line = line_clean

    return result


def parse_post(post_element) -> list[Tournament]:
    """
    Parse a forum post element and extract tournament data.
    Returns list of tournaments (a post may contain multiple).
    Only extracts Beyblade X content, filters out Metal Fight etc.
    """
    tournaments = []

    # Get post ID
    post_id = post_element.get("id", "")
    if not post_id.startswith("pid"):
        return tournaments

    # Get post body
    body = post_element.find("div", class_="post_body")
    if not body:
        return tournaments

    # Get text content preserving some structure
    text = body.get_text(separator="\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # Skip the first post (it's instructions)
    if "This thread is for Beyblade X combinations" in text:
        return tournaments

    # Filter out non-Beyblade X content
    if not is_beyblade_x_content(lines):
        return tournaments

    # Parse header info from first few lines
    header_info = parse_header_lines(lines)

    # Fallback: extract date from WBO post timestamp if text parsing failed
    if header_info["date"] is None:
        post_date_el = post_element.find("span", class_="post_date")
        if post_date_el:
            post_date_text = post_date_el.get_text().strip()
            # Format: "Aug. 08, 2025  9:45 PM" — extract just the date part
            date_part = re.match(r"([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})", post_date_text)
            if date_part:
                header_info["date"] = parse_date(date_part.group(1))

    current_tournament = None
    current_placements = []
    current_place = None
    current_player = None
    current_combos = []

    # Track if we've created the initial tournament
    tournament_created = False
    tournament_index = 0

    for i, line in enumerate(lines):
        # Check for date pattern that might indicate a NEW tournament within same post
        # (some posts contain multiple tournaments)
        has_date = re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", line) or re.search(
            r"[A-Z][a-z]+ \d{1,2},? \d{4}", line
        )

        # Only treat as new tournament if we already have one and this looks like a header
        if has_date and tournament_created and current_tournament:
            # Check if this looks like a tournament header (not just a date mention)
            # Headers typically have the date near the start or alone
            is_header_line = (
                line.strip().startswith("-")  # "- 07/29/23"
                or re.match(r"^[A-Z][a-z]+ \d{1,2},? \d{4}$", line.strip())  # Just date
                or re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", line.strip())  # Just date
            )

            if is_header_line:
                # Save current tournament
                if current_place is not None and current_player and current_combos:
                    current_placements.append(
                        Placement(
                            place=current_place,
                            player_name=current_player,
                            player_wbo_id=None,
                            combos=current_combos,
                        )
                    )

                if current_placements:
                    current_tournament.placements = current_placements
                    tournaments.append(current_tournament)
                    tournament_index += 1

                # Start new tournament - re-parse header from this point
                remaining_lines = lines[max(0, i - 1) : i + 5]
                header_info = parse_header_lines(remaining_lines)

                unique_post_id = f"{post_id}_{tournament_index}"
                current_tournament = Tournament(
                    wbo_post_id=unique_post_id,
                    name=header_info["name"] or "Unknown Tournament",
                    date=header_info["date"],
                    city=header_info["city"],
                    state=header_info["state"],
                    country=header_info["country"],
                    format=header_info["format"],
                    ranked=header_info["ranked"],
                )
                current_placements = []
                current_place = None
                current_player = None
                current_combos = []
                continue

        # Create initial tournament if not yet created
        if not tournament_created:
            current_tournament = Tournament(
                wbo_post_id=post_id,
                name=header_info["name"] or "Unknown Tournament",
                date=header_info["date"],
                city=header_info["city"],
                state=header_info["state"],
                country=header_info["country"],
                format=header_info["format"],
                ranked=header_info["ranked"],
            )
            tournament_created = True
            current_placements = []
            current_place = None
            current_player = None
            current_combos = []
            # Don't continue - still need to check this line for placements

        # Check for placement lines - supports many formats:
        #   "1st Place: PlayerName", "1st: PlayerName", "1st PlayerName"
        #   "First Place: PlayerName", "FIRST PLACE", "First:"
        #   "🥇 PlayerName", "🥈 PlayerName", "🥉 PlayerName"
        #   "1. PlayerName", "2. PlayerName", "3. PlayerName"
        #   "PlayerName - 1st Place", "PlayerName 1st"
        #   "PlayerName 1st Place:", "2ed", "3ed" (typos)
        #   ", 1st Place:"
        detected_place = None
        remainder = ""
        inline_combo_str = None

        # Clean leading punctuation for placement detection
        line_clean_place = line.lstrip(",-–—•*").strip()

        # Ordinal mapping (includes common typos)
        ORDINAL_MAP = {
            "1st": 1, "2nd": 2, "3rd": 3, "4th": 4,
            "2ed": 2, "3ed": 3, "4rth": 4,
        }

        # Pattern 1: Ordinal format - "1st (Place)? ..."
        place_match = re.match(
            r"^(1st|2nd|3rd|4th|2ed|3ed|4rth)\s*(Place)?[:\s-]*(.*)$", line_clean_place, re.IGNORECASE
        )
        if place_match:
            place_str = place_match.group(1).lower()
            detected_place = ORDINAL_MAP.get(place_str)
            remainder = place_match.group(3).strip() if place_match.group(3) else ""

        # Pattern 2: Word format - "First (Place)? ..."
        if detected_place is None:
            place_match = re.match(
                r"^(First|Second|Third|Fourth)\s*(Place)?[:\s-]*(.*)$", line_clean_place, re.IGNORECASE
            )
            if place_match:
                place_str = place_match.group(1).lower()
                detected_place = {"first": 1, "second": 2, "third": 3, "fourth": 4}.get(place_str)
                remainder = place_match.group(3).strip() if place_match.group(3) else ""

        # Pattern 3: Emoji medals - "🥇 PlayerName"
        if detected_place is None:
            place_match = re.match(r"^(🥇|🥈|🥉)\s*(.*)$", line)
            if place_match:
                emoji = place_match.group(1)
                detected_place = {"🥇": 1, "🥈": 2, "🥉": 3}.get(emoji)
                remainder = place_match.group(2).strip() if place_match.group(2) else ""

        # Pattern 4: Numbered format - "1. PlayerName", bare "1.", or "#1 PlayerName"
        if detected_place is None:
            place_match = re.match(r"^([1234])\.\s*(.*)$", line_clean_place)
            if not place_match:
                place_match = re.match(r"^#([1234])\s*(.*)$", line_clean_place)
            if place_match:
                detected_place = int(place_match.group(1))
                remainder = place_match.group(2).strip() if place_match.group(2) else ""

        # Pattern 5: "PlayerName - 1st Place" or "PlayerName - 1st"
        if detected_place is None:
            place_match = re.match(
                r"^(.+?)\s*[-–—]\s*(1st|2nd|3rd|4th|2ed|3ed)\s*(Place)?\s*:?\s*$", line, re.IGNORECASE
            )
            if place_match:
                place_str = place_match.group(2).lower()
                detected_place = ORDINAL_MAP.get(place_str)
                remainder = place_match.group(1).strip()

        # Pattern 6: "PlayerName 1st (Place)?:?" (player name before ordinal, no separator)
        # Only match if line does NOT contain a ratchet pattern (to avoid combo lines)
        if detected_place is None and not re.search(r"\d-\d{2}", line):
            place_match = re.match(
                r"^(.+?)\s+(1st|2nd|3rd|4th|2ed|3ed)\s*(Place)?\s*:?\s*$", line, re.IGNORECASE
            )
            if place_match:
                place_str = place_match.group(2).lower()
                detected_place = ORDINAL_MAP.get(place_str)
                remainder = place_match.group(1).strip()

        # Pattern 7: "Winner:" - treat as 1st place
        if detected_place is None:
            place_match = re.match(r"^Winner\s*:\s*(.*)$", line_clean_place, re.IGNORECASE)
            if place_match:
                detected_place = 1
                remainder = place_match.group(1).strip() if place_match.group(1) else ""

        # Pattern 8: Inline "PlayerName N combo" (e.g., "Cyrus25 1st DranDagger 4-80Flat")
        # Player + ordinal + combo all on one line - split into placement + immediate combo
        if detected_place is None and re.search(r"\d-\d{2}", line):
            place_match = re.match(
                r"^(.+?)\s+(1st|2nd|3rd|4th|2ed|3ed|[123])\s+(\S+\s+(?:\d{1,2}|M)-\d{2,3}.*)$", line, re.IGNORECASE
            )
            if place_match:
                place_val = place_match.group(2).lower()
                detected_place = ORDINAL_MAP.get(place_val) or (int(place_val) if place_val.isdigit() else None)
                remainder = place_match.group(1).strip()  # Player name
                # We'll need to parse the combo part after setting up the placement
                inline_combo_str = place_match.group(3).strip()

        if detected_place is not None and detected_place <= 3:
            # Save previous placement
            if current_place is not None and current_player and current_combos:
                current_placements.append(
                    Placement(
                        place=current_place,
                        player_name=current_player,
                        player_wbo_id=None,
                        combos=current_combos,
                    )
                )

            current_place = detected_place

            # Player name might be on same line (in remainder)
            if remainder and not re.search(r"\d-\d{2}", remainder):
                # No ratchet pattern, so this is probably the player name
                current_player = remainder
            elif remainder and re.search(r"\d-\d{2}", remainder):
                # Remainder has a combo (ratchet pattern) - try to find player from prev line
                # Handles: "PlayerName\n1st ComboString" format
                current_player = None
                if i > 0:
                    prev_line = lines[i - 1].strip()
                    if (
                        prev_line
                        and len(prev_line) < 50
                        and not re.search(r"\d-\d{2}", prev_line)
                        and not re.match(r"^(1st|2nd|3rd|4th|2ed|3ed|First|Second|Third|Fourth|[🥇🥈🥉]|[1234]\.|#[1234])", prev_line, re.I)
                        and not any(noise.lower() in prev_line.lower() for noise in ["winning combo", "top 3", "stage", "format", "ranked", "http"])
                        and not prev_line.startswith("(")
                    ):
                        current_player = prev_line
                # Parse the remainder as a combo so we don't lose it
                inline_combo_str = remainder
            else:
                # No remainder at all - try looking back at the previous line for player name
                # Handles format: "PlayerName\n1st\ncombo1\ncombo2"
                current_player = None
                if i > 0:
                    prev_line = lines[i - 1].strip()
                    if (
                        prev_line
                        and len(prev_line) < 50
                        and not re.search(r"\d-\d{2}", prev_line)
                        and not re.match(r"^(1st|2nd|3rd|4th|2ed|3ed|First|Second|Third|Fourth|[🥇🥈🥉]|[1234]\.|#[1234])", prev_line, re.I)
                        and not any(noise.lower() in prev_line.lower() for noise in ["winning combo", "top 3", "stage", "format", "ranked", "http"])
                        and not prev_line.startswith("(")
                    ):
                        current_player = prev_line
            current_combos = []
            # If Pattern 8 matched, parse the inline combo immediately
            if inline_combo_str:
                combo = parse_combo(inline_combo_str)
                if combo:
                    current_combos.append(combo)
                inline_combo_str = None
            continue

        # If we're in a placement section
        if current_place is not None:
            # Check if this looks like a player name (short, no ratchet pattern)
            if current_player is None and not re.search(r"\d-\d{2}", line):
                # Filter out noise and stage annotations
                noise_patterns = [
                    "!",
                    "(Click to View)",
                    "WINNING COMBOS",
                    "Top 3 Photo",
                    "Top 3 Deck Combos",
                    "Both Stages",
                    "First Stage",
                    "Final Stage",
                    "Finals Only",
                    "First Stage Only",
                    "3on3 Match Finals Only",
                    "3on3 Finals Only",
                    "Match Finals Only",
                ]
                if any(noise.lower() in line.lower() for noise in noise_patterns):
                    continue
                if line.startswith("(") or len(line) <= 1:
                    continue
                if len(line) < 50:
                    current_player = line
                    continue

            # Try to parse as combo
            combo = parse_combo(line)
            if combo:
                current_combos.append(combo)
            elif current_player is not None and re.search(r"\d-\d{2}", line):
                # Line has a ratchet pattern but didn't parse - might have player prefix
                # e.g., "Possiblyamelon LeonCrest 9-60Ball" (player + combo inline)
                # Try stripping the known player name from the front
                if line.startswith(current_player):
                    combo_part = line[len(current_player):].strip()
                    combo = parse_combo(combo_part)
                    if combo:
                        current_combos.append(combo)

    # Don't forget the last placement and tournament
    if current_place is not None and current_player and current_combos:
        current_placements.append(
            Placement(
                place=current_place,
                player_name=current_player,
                player_wbo_id=None,
                combos=current_combos,
            )
        )

    if current_tournament and current_placements:
        current_tournament.placements = current_placements
        tournaments.append(current_tournament)

    return tournaments


# Global scraper session for Cloudflare bypass
_scraper = None


def get_scraper():
    """Get or create a cloudscraper session to bypass Cloudflare."""
    global _scraper
    if _scraper is None:
        _scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    return _scraper


def fetch_page(page_num: int = 1) -> str:
    """Fetch a page from the WBO thread."""
    url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
    scraper = get_scraper()
    response = scraper.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def get_total_pages(page_html: str) -> int:
    """Get total number of pages in the thread."""
    soup = BeautifulSoup(page_html, "lxml")

    # Find all links with page= in href
    page_links = soup.find_all("a", href=re.compile(r"page=(\d+)"))
    if page_links:
        pages = []
        for link in page_links:
            href = link.get("href", "")
            match = re.search(r"page=(\d+)", href)
            if match:
                pages.append(int(match.group(1)))
        if pages:
            return max(pages)
    return 1


def get_processed_post_ids(conn) -> set[str]:
    """Get all post IDs we've already processed."""
    result = conn.execute(
        "SELECT wbo_post_id FROM tournaments WHERE wbo_post_id IS NOT NULL"
    ).fetchall()
    return {row[0] for row in result}


def insert_tournament(conn, tournament: Tournament) -> Optional[int]:
    """Insert a tournament and its placements. Returns tournament ID or None if skipped."""
    if not tournament.date:
        return None

    if not tournament.placements:
        return None

    # Check if already processed (by post ID)
    existing = conn.execute(
        "SELECT id FROM tournaments WHERE wbo_post_id = ?", [tournament.wbo_post_id]
    ).fetchone()

    if existing:
        return None  # Skip, already processed

    # Infer region
    region = infer_region(tournament.country)

    # Insert tournament
    result = conn.execute(
        """
        INSERT INTO tournaments (wbo_post_id, name, date, city, state, country, region, format, ranked, wbo_thread_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
    """,
        [
            tournament.wbo_post_id,
            tournament.name,
            tournament.date.strftime("%Y-%m-%d"),
            tournament.city,
            tournament.state,
            tournament.country,
            region,
            tournament.format,
            tournament.ranked,
            tournament.wbo_url,
        ],
    )

    tournament_id = result.fetchone()[0]

    # Insert placements
    for placement in tournament.placements:
        if not placement.combos:
            continue

        combos = placement.combos[:3]

        try:
            conn.execute(
                """
                INSERT INTO placements (
                    tournament_id, place, player_name, player_wbo_id,
                    blade_1, ratchet_1, bit_1, assist_1, lock_chip_1, over_blade_1, stage_1,
                    blade_2, ratchet_2, bit_2, assist_2, lock_chip_2, over_blade_2, stage_2,
                    blade_3, ratchet_3, bit_3, assist_3, lock_chip_3, over_blade_3, stage_3
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    tournament_id,
                    placement.place,
                    placement.player_name,
                    placement.player_wbo_id,
                    combos[0].blade if len(combos) > 0 else None,
                    combos[0].ratchet if len(combos) > 0 else None,
                    combos[0].bit if len(combos) > 0 else None,
                    combos[0].assist if len(combos) > 0 else None,
                    combos[0].lock_chip if len(combos) > 0 else None,
                    combos[0].over_blade if len(combos) > 0 else None,
                    combos[0].stage if len(combos) > 0 else None,
                    combos[1].blade if len(combos) > 1 else None,
                    combos[1].ratchet if len(combos) > 1 else None,
                    combos[1].bit if len(combos) > 1 else None,
                    combos[1].assist if len(combos) > 1 else None,
                    combos[1].lock_chip if len(combos) > 1 else None,
                    combos[1].over_blade if len(combos) > 1 else None,
                    combos[1].stage if len(combos) > 1 else None,
                    combos[2].blade if len(combos) > 2 else None,
                    combos[2].ratchet if len(combos) > 2 else None,
                    combos[2].bit if len(combos) > 2 else None,
                    combos[2].assist if len(combos) > 2 else None,
                    combos[2].lock_chip if len(combos) > 2 else None,
                    combos[2].over_blade if len(combos) > 2 else None,
                    combos[2].stage if len(combos) > 2 else None,
                ],
            )
        except Exception as e:
            print(f"Error inserting placement for {placement.player_name}: {e}")

    return tournament_id


def scrape_all(
    max_pages: Optional[int] = None, delay: float = 1.0, fresh: bool = False
):
    """
    Scrape all pages from the WBO thread.

    Args:
        max_pages: Maximum number of pages to scrape (None for all)
        delay: Delay between requests in seconds
        fresh: If True, clear existing data and start fresh
    """
    conn = get_connection()
    init_schema(conn)

    if fresh:
        print("Fresh scrape requested - clearing existing tournament data...")
        conn.execute("DELETE FROM placements")
        conn.execute("DELETE FROM tournaments")
        conn.commit()

    # Get already processed post IDs
    processed_ids = get_processed_post_ids(conn)
    print(f"Already processed {len(processed_ids)} posts")

    print("Fetching first page to get total page count...")
    first_page = fetch_page(1)
    total_pages = get_total_pages(first_page)

    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"Scraping {total_pages} pages...")

    tournaments_added = 0
    tournaments_skipped = 0

    for page_num in tqdm(range(1, total_pages + 1), desc="Pages"):
        try:
            if page_num == 1:
                page_html = first_page
            else:
                time.sleep(delay)
                page_html = fetch_page(page_num)

            soup = BeautifulSoup(page_html, "lxml")
            posts = soup.find_all("div", class_="post")

            for post in posts:
                post_id = post.get("id", "")

                # Quick skip if base post ID already processed
                if post_id in processed_ids:
                    tournaments_skipped += 1
                    continue

                tournaments = parse_post(post)

                for tournament in tournaments:
                    if tournament.wbo_post_id in processed_ids:
                        tournaments_skipped += 1
                        continue

                    try:
                        result = insert_tournament(conn, tournament)
                        if result:
                            tournaments_added += 1
                            processed_ids.add(tournament.wbo_post_id)
                        else:
                            tournaments_skipped += 1
                    except Exception as e:
                        print(f"Error inserting tournament {tournament.name}: {e}")

            conn.commit()

        except Exception as e:
            print(f"Error on page {page_num}: {e}")
            continue

    # Normalize data to fix any typos
    print("Normalizing data...")
    fixed_count = normalize_data(conn)
    if fixed_count > 0:
        print(f"Fixed {fixed_count} typos/inconsistencies")
        conn.commit()

    conn.close()
    print(
        f"\nDone! Added {tournaments_added} tournaments, skipped {tournaments_skipped} (already processed or invalid)"
    )


def test_parse():
    """Test parsing with sample data."""
    sample_html = """
    <div class="post" id="pid1850344">
        <div class="post_body">
            X MARKS THE SPOT! - 07/29/23
            Toronto, ON, Canada - X Format - Unranked 1on1

            1st Place: Wombat
            DranSword 3-60F

            2nd Place: henwooja1
            HellScythe Phoenix Wing 4-80B

            3rd Place: 1234beyblade
            KnightShield 3-80N
        </div>
    </div>
    """

    soup = BeautifulSoup(sample_html, "lxml")
    post = soup.find("div", class_="post")
    tournaments = parse_post(post)

    for t in tournaments:
        print(f"\nTournament: {t.name}")
        print(f"Post ID: {t.wbo_post_id}")
        print(f"Date: {t.date}")
        print(f"Location: {t.city}, {t.state}, {t.country}")
        print(f"Format: {t.format}, Ranked: {t.ranked}")
        print("Placements:")
        for p in t.placements:
            print(f"  {p.place}: {p.player_name}")
            for c in p.combos:
                assist_str = f" + {c.assist}" if c.assist else ""
                lock_chip_str = f" [{c.lock_chip}]" if c.lock_chip else ""
                print(f"    - {c.blade}{lock_chip_str}{assist_str} {c.ratchet} {c.bit}")


def scrape_local(fresh: bool = False):
    """
    Scrape from locally downloaded HTML files.

    This reads HTML pages from data/wbo_pages/ that were downloaded
    by wbo_downloader.py on Windows.

    Args:
        fresh: If True, clear existing WBO data and start fresh
    """
    from pathlib import Path

    pages_dir = Path(__file__).parent.parent / "data" / "wbo_pages"

    if not pages_dir.exists():
        print(f"ERROR: No downloaded pages found at {pages_dir}")
        print()
        print("To download pages:")
        print("1. Copy wbo_downloader.py to Windows")
        print("2. Edit it to paste your browser cookies")
        print("3. Run: python wbo_downloader.py")
        print("4. Copy the wbo_pages folder to data/wbo_pages/")
        return

    # Find all page files
    page_files = sorted(pages_dir.glob("page_*.html"))
    if not page_files:
        print(f"ERROR: No page_*.html files found in {pages_dir}")
        return

    print(f"Found {len(page_files)} downloaded pages")

    conn = get_connection()
    init_schema(conn)

    if fresh:
        print("Fresh import requested - clearing existing WBO tournament data...")
        # Only delete WBO data, keep JP/DE data
        conn.execute(
            "DELETE FROM placements WHERE tournament_id IN (SELECT id FROM tournaments WHERE wbo_post_id IS NOT NULL AND wbo_post_id NOT LIKE 'jp_%' AND wbo_post_id NOT LIKE 'de_%')"
        )
        conn.execute(
            "DELETE FROM tournaments WHERE wbo_post_id IS NOT NULL AND wbo_post_id NOT LIKE 'jp_%' AND wbo_post_id NOT LIKE 'de_%'"
        )
        conn.commit()

    # Get already processed post IDs
    processed_ids = get_processed_post_ids(conn)
    print(f"Already processed {len(processed_ids)} posts")

    tournaments_added = 0
    tournaments_skipped = 0

    for page_file in tqdm(page_files, desc="Processing pages"):
        try:
            with open(page_file, "r", encoding="utf-8") as f:
                page_html = f.read()

            soup = BeautifulSoup(page_html, "lxml")
            posts = soup.find_all("div", class_="post")

            for post in posts:
                post_id = post.get("id", "")

                # Quick skip if base post ID already processed
                if post_id in processed_ids:
                    tournaments_skipped += 1
                    continue

                tournaments = parse_post(post)

                for tournament in tournaments:
                    if tournament.wbo_post_id in processed_ids:
                        tournaments_skipped += 1
                        continue

                    try:
                        result = insert_tournament(conn, tournament)
                        if result:
                            tournaments_added += 1
                            processed_ids.add(tournament.wbo_post_id)
                        else:
                            tournaments_skipped += 1
                    except Exception as e:
                        print(f"Error inserting tournament {tournament.name}: {e}")

            conn.commit()

        except Exception as e:
            print(f"Error processing {page_file.name}: {e}")
            continue

    # Normalize data to fix any typos
    print("Normalizing data...")
    fixed_count = normalize_data(conn)
    if fixed_count > 0:
        print(f"Fixed {fixed_count} typos/inconsistencies")
        conn.commit()

    conn.close()
    print(
        f"\nDone! Added {tournaments_added} tournaments, skipped {tournaments_skipped} (already processed or invalid)"
    )


def show_stats():
    """Show current database statistics."""
    conn = get_connection()

    result = conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()
    tournaments = result[0] if result else 0
    result = conn.execute("SELECT COUNT(*) FROM placements").fetchone()
    placements = result[0] if result else 0

    print(f"\n=== DATABASE STATS ===")
    print(f"Tournaments: {tournaments}")
    print(f"Placements: {placements}")

    print(f"\n=== TOP 10 BLADES ===")
    for row in conn.execute(
        "SELECT part_name, total_placements, win_rate FROM part_stats WHERE part_type = 'blade' ORDER BY total_placements DESC LIMIT 10"
    ).fetchall():
        print(f"  {row[0]}: {row[1]} uses, {row[2]:.1%} win rate")

    print(f"\n=== TOP 10 COMBOS ===")
    for row in conn.execute(
        "SELECT combo, total_placements, win_rate FROM combo_stats ORDER BY total_placements DESC LIMIT 10"
    ).fetchall():
        print(f"  {row[0]}: {row[1]} uses, {row[2]:.1%} win rate")

    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "test":
            test_parse()
        elif cmd == "stats":
            show_stats()
        elif cmd == "local":
            # Parse from locally downloaded HTML files
            fresh = len(sys.argv) > 2 and sys.argv[2] == "fresh"
            scrape_local(fresh=fresh)
        elif cmd == "fresh":
            # Fresh scrape - clear and rescrape
            max_pages = int(sys.argv[2]) if len(sys.argv) > 2 else None
            scrape_all(max_pages=max_pages, fresh=True)
        elif cmd.isdigit():
            # Scrape N pages incrementally
            scrape_all(max_pages=int(cmd))
        else:
            print("Usage:")
            print("  python scraper.py test       - Test parsing")
            print("  python scraper.py stats      - Show database stats")
            print(
                "  python scraper.py N          - Scrape N pages (incremental, needs Cloudflare bypass)"
            )
            print(
                "  python scraper.py fresh N    - Fresh scrape N pages (clears existing)"
            )
            print()
            print(
                "  python scraper.py local      - Parse from downloaded HTML files (recommended)"
            )
            print("  python scraper.py local fresh- Fresh parse from downloaded files")
            print()
            print("Recommended workflow:")
            print("  1. On Windows: python wbo_downloader.py")
            print("  2. Copy data/wbo_pages/ to this machine")
            print("  3. Run: python scraper.py local")
    else:
        # Default: try local files first, fall back to scraping
        from pathlib import Path

        pages_dir = Path(__file__).parent.parent / "data" / "wbo_pages"
        if pages_dir.exists() and list(pages_dir.glob("page_*.html")):
            print("Found downloaded pages, using local mode...")
            scrape_local()
        else:
            print(
                "No downloaded pages found, attempting online scrape (may fail due to Cloudflare)..."
            )
            scrape_all(max_pages=5)
