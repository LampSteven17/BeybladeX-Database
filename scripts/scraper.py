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

# Canonical blade names - the "correct" order for two-word blade names
# This is the authoritative list of blade names in proper order
CANONICAL_BLADES = {
    # BX Series
    "Dran Sword",
    "Hells Scythe",
    "Knight Shield",
    "Knight Lance",
    "Wizard Arrow",
    "Shark Edge",
    "Viper Tail",
    "Dran Dagger",
    "Hells Hammer",
    "Leon Claw",
    "Dran Buster",
    "Hells Chain",
    "Rhino Horn",
    "Black Shell",
    "Tyranno Beat",
    "Unicorn Sting",
    "Phoenix Wing",
    "Wizard Rod",
    "Aero Pegasus",
    "Silver Wolf",
    "Shark Slayer",
    "Samurai Saber",
    "Weiss Tiger",
    "Talon Ptera",
    "Spike Cadeus",
    "Roar Tyranno",
    # CX Series
    "Cobalt Dragoon",
    "Cobalt Drake",
    "Steel Samurai",
    "Burn Wyvern",
    "Prominence Phoenix",
    # UX Series
    "Phoenix Wing",
    "Whale Wave",
    "Knight Mail",
    "Leon Crest",
    "Wyvern Gale",
    "Phoenix Feather",
    "Tusk Mammoth",
    "Phoenix Rudder",
    "Dranzer Spiral",
    # Newer blades
    "Pegasus Blast",
    "Wizard Arc",
    "Fox Brush",
    "Hells Blast",
    "Valkyrie Volt",
    "Blast Emperor",
    "Emperor Brave",
    "Dran Brave",
    "Might Blast",
    "Hover Wyvern",
    "Shark Scale",
    "Meteor Dragoon",
    "Impact Drake",
    "Rock Golem",
    "Samurai Calibur",
    "Crimson Garuda",
    "Scorpio Spear",
    "Shinobi Shadow",
    "Ghost Circle",
    "Bite Croc",
    "Keel Shark",
    "Knife Shinobi",
    "Shelter Drake",
    "Sphinx Cowl",
    "Tide Whale",
    "Tricera Press",
    "Bear Scratch",
    "Chain Incendio",
    "Clock Mirage",
    "Dragoon Storm",
    "Driger Slash",
    "Gill Shark",
    "Mummy Curse",
    "Scythe Incendio",
    "Sting Unicorn",
    "Wand Wizard",
    "Xeno Xcalibur",
    "Cerberus Blast",
}

# Build normalization map automatically from canonical names
# This handles: no-space versions, swapped word order, and common typos
BLADE_NORMALIZATION = {}

for blade in CANONICAL_BLADES:
    key = blade.lower().replace(" ", "")
    BLADE_NORMALIZATION[key] = blade

    # Handle swapped word order (e.g., "Wyvern Hover" -> "Hover Wyvern")
    words = blade.split()
    if len(words) == 2:
        swapped = f"{words[1]} {words[0]}"
        swapped_key = swapped.lower().replace(" ", "")
        BLADE_NORMALIZATION[swapped_key] = blade
        # Also with space
        BLADE_NORMALIZATION[swapped.lower()] = blade

# Add common typo variations
BLADE_NORMALIZATION.update(
    {
        "hellscythe": "Hells Scythe",  # Missing 's'
        "dranzerspiral": "Dranzer Spiral",
    }
)


def normalize_blade(blade: str) -> str:
    """Normalize blade name to canonical form.

    Handles:
    - Swapped word order (e.g., "Wyvern Hover" -> "Hover Wyvern")
    - No spaces (e.g., "HoverWyvern" -> "Hover Wyvern")
    - CamelCase (e.g., "HellsScythe" -> "Hells Scythe")
    """
    # Strip leading/trailing whitespace and dashes
    blade = blade.strip().lstrip("-").strip()

    # Try direct lookup (lowercase, no spaces)
    key = blade.lower().replace(" ", "")
    if key in BLADE_NORMALIZATION:
        return BLADE_NORMALIZATION[key]

    # Try with spaces preserved (for swapped names like "Wyvern Hover")
    key_with_space = blade.lower()
    if key_with_space in BLADE_NORMALIZATION:
        return BLADE_NORMALIZATION[key_with_space]

    # Try swapping words if it's a two-word name
    words = blade.split()
    if len(words) == 2:
        swapped_key = f"{words[1]}{words[0]}".lower()
        if swapped_key in BLADE_NORMALIZATION:
            return BLADE_NORMALIZATION[swapped_key]

    # Try to split CamelCase into words
    # e.g., "HellsScythe" -> "Hells Scythe"
    camel_split = re.sub(r"([a-z])([A-Z])", r"\1 \2", blade)
    camel_key = camel_split.lower().replace(" ", "")
    if camel_key in BLADE_NORMALIZATION:
        return BLADE_NORMALIZATION[camel_key]

    # Try swapped CamelCase
    camel_words = camel_split.split()
    if len(camel_words) == 2:
        swapped_camel = f"{camel_words[1]}{camel_words[0]}".lower()
        if swapped_camel in BLADE_NORMALIZATION:
            return BLADE_NORMALIZATION[swapped_camel]

    # If CamelCase split worked, use it as title case
    if " " in camel_split:
        return camel_split.title()

    # If already has spaces, return as title case
    if " " in blade:
        return blade.title()

    return blade


# Bit abbreviation mappings
# Known blade names for assist detection - use CANONICAL_BLADES plus any single-word blades
KNOWN_BLADES = CANONICAL_BLADES | {"Venom"}  # Add single-word blades here

# Assist blade names (the gear parts that attach to main blades)
KNOWN_ASSISTS = {
    "Jaggy",
    "Slash",
    "Wheel",
    "Bumper",
    "Heavy",
    "Assault",
    "Rush",
    "Low",
}

# Assist abbreviations -> full names
ASSIST_ABBREVIATIONS = {
    "W": "Wheel",
    "H": "Heavy",
    "S": "Slash",
    "J": "Jaggy",
    "K": "Kick",  # Not sure if this is assist or bit
    "Z": "Zap",
    "RA": "Rush Assault",
    "LO": "Low",
    "UN": "Unite",
    "TP": "Taper",
    "BS": "Bumper Slash",
    "UF": "Upper Flat",
    "GR": "Gear Rush",
    "WB": "Wheel Bumper",
    "V": "Vanguard",
}

# Lowercase lookup for efficient matching (includes no-space and swapped variants)
KNOWN_BLADES_LOWER = {b.lower(): b for b in KNOWN_BLADES}
# Add no-space variants and swapped word order
for blade in KNOWN_BLADES:
    # No-space variant
    no_space = blade.lower().replace(" ", "")
    if no_space not in KNOWN_BLADES_LOWER:
        KNOWN_BLADES_LOWER[no_space] = blade

    # Swapped word order (e.g., "Wyvern Hover" -> "Hover Wyvern")
    words = blade.split()
    if len(words) == 2:
        swapped = f"{words[1]} {words[0]}"
        swapped_lower = swapped.lower()
        swapped_no_space = swapped_lower.replace(" ", "")
        if swapped_lower not in KNOWN_BLADES_LOWER:
            KNOWN_BLADES_LOWER[swapped_lower] = blade
        if swapped_no_space not in KNOWN_BLADES_LOWER:
            KNOWN_BLADES_LOWER[swapped_no_space] = blade

# Add assists to the lookup
KNOWN_ASSISTS_LOWER = {a.lower(): a for a in KNOWN_ASSISTS}


BIT_ABBREVIATIONS = {
    # Single letter
    "B": "Ball",
    "F": "Flat",
    "N": "Needle",
    "P": "Point",
    "T": "Taper",
    "S": "Spike",
    "O": "Orb",
    "D": "Dot",
    "A": "Accel",
    "R": "Rush",
    "H": "Hexa",
    "C": "Cyclone",
    "U": "Unite",
    "L": "Level",
    "E": "Elevate",
    "G": "Glide",
    "Q": "Quake",
    "K": "Kick",
    "V": "Vanguard",
    # Two letter
    "HN": "High Needle",
    "LF": "Low Flat",
    "LR": "Low Rush",
    "LN": "Low Needle",
    "GF": "GearFlat",
    "GB": "GearBall",
    "GN": "GearNeedle",
    "GP": "GearPoint",
    "MN": "Metal Needle",
    "HT": "High Taper",
    "HA": "High Accel",
    "DB": "Disc Ball",
    "HS": "High Sword",
    "SN": "Spiral Needle",
    "Lv": "Level",
    "El": "Elevate",
    "Un": "Unite",
    "Br": "Brake",
    "Bd": "Bound",
    "Gl": "Glide",
    # Inconsistent naming
    "Hex": "Hexa",
    # Common variations
    "Level": "Level",
    "Elevate": "Elevate",
    "Ball": "Ball",
    "Flat": "Flat",
    "Needle": "Needle",
    "Point": "Point",
    "Taper": "Taper",
    "Rush": "Rush",
    "Unite": "Unite",
    "Accel": "Accel",
    "GearFlat": "GearFlat",
    "GearBall": "GearBall",
    "High Needle": "High Needle",
    "Low Flat": "Low Flat",
    "Low Rush": "Low Rush",
    "LowFlat": "Low Flat",
    "LowRush": "Low Rush",
    "LowNeedle": "Low Needle",
    "HighNeedle": "High Needle",
    "HighTaper": "High Taper",
    "HighAccel": "High Accel",
    "DiscBall": "Disc Ball",
    "MetalNeedle": "Metal Needle",
    # Full names (for consistency)
    "Kick": "Kick",
    "Vanguard": "Vanguard",
}


@dataclass
class Combo:
    blade: str
    ratchet: str
    bit: str
    assist: Optional[str] = None
    lock_chip: Optional[str] = None
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
    """Expand bit abbreviations to full names."""
    bit = bit.strip()
    return BIT_ABBREVIATIONS.get(bit, bit)


def split_blade_assist(blade_text: str) -> tuple[str, Optional[str]]:
    """
    Split blade text into (blade, assist) if there's an assist code/name.
    Format: "[Blade Name] [Assist]" where assist is typically a single letter or short word.

    Examples:
    - "Courage Dran S" -> ("Courage Dran", "S")
    - "Pegasus Blast Jaggy" -> ("Pegasus Blast", "Jaggy")
    - "Wizard Rod" -> ("Wizard Rod", None)
    """
    blade_text = blade_text.strip()

    # Split into words
    words = blade_text.split()
    if len(words) <= 1:
        return normalize_blade(blade_text), None

    # Check if the last word is an assist (single letter, abbreviation, or known assist name)
    last_word = words[-1]

    # Check if it's a known assist abbreviation or name
    is_assist = (
        last_word in ASSIST_ABBREVIATIONS
        or last_word.lower() in KNOWN_ASSISTS_LOWER
        or (
            len(last_word) <= 2 and last_word.isupper()
        )  # Single/double letter uppercase
    )

    if is_assist:
        # The last word is the assist, everything before is the blade
        blade_words = words[:-1]
        blade = normalize_blade(" ".join(blade_words))
        assist = ASSIST_ABBREVIATIONS.get(last_word, last_word)
        return blade, assist

    # Check if second-to-last + last could be an assist (e.g., "Low Rush" as assist name)
    if len(words) >= 3:
        last_two = " ".join(words[-2:])
        if last_two.lower() in [a.lower() for a in KNOWN_ASSISTS]:
            blade_words = words[:-2]
            blade = normalize_blade(" ".join(blade_words))
            return blade, last_two

    # No assist found, normalize the whole text as the blade
    return normalize_blade(blade_text), None


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
    combo_str = combo_str.strip()
    if not combo_str:
        return None

    # Pattern: [Blade + optional Assist] [Ratchet][Bit]
    # Ratchet is X-XX format, bit can be attached or separate

    # Try: Everything + Ratchet + Bit (with space before bit)
    match = re.match(r"^(.+?)\s+(\d{1,2}-\d{2,3})\s+([A-Za-z][A-Za-z\s]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        ratchet = match.group(2).strip()
        bit = expand_bit(match.group(3).strip())
        blade, assist = split_blade_assist(blade_part)
        # Parse CX blade to extract lock chip
        lock_chip, blade = parse_cx_blade(blade)
        return Combo(
            blade=blade,
            ratchet=ratchet,
            bit=bit,
            assist=assist,
            lock_chip=lock_chip,
            stage=stage,
        )

    # Try: Everything + Ratchet+Bit (no space, bit attached like 3-60F or 6-60V)
    match = re.match(r"^(.+?)\s+(\d{1,2}-\d{2,3})([A-Z][A-Za-z]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        ratchet = match.group(2).strip()
        bit = expand_bit(match.group(3).strip())
        blade, assist = split_blade_assist(blade_part)
        # Parse CX blade to extract lock chip
        lock_chip, blade = parse_cx_blade(blade)
        return Combo(
            blade=blade,
            ratchet=ratchet,
            bit=bit,
            assist=assist,
            lock_chip=lock_chip,
            stage=stage,
        )

    # Try: Blade + AssistRatchetBit (assist concatenated with ratchet, e.g., "FoxBlast Wheel9-60Hexa")
    # Pattern: blade_part + assist_prefix + ratchet + bit (no space between assist and ratchet)
    match = re.match(r"^(.+?)\s+([A-Za-z]+)(\d{1,2}-\d{2,3})([A-Z][A-Za-z]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        potential_assist = match.group(2).strip()
        ratchet = match.group(3).strip()
        bit = expand_bit(match.group(4).strip())

        # Check if potential_assist is a known assist
        if potential_assist in ASSIST_ABBREVIATIONS or potential_assist.lower() in KNOWN_ASSISTS_LOWER:
            assist = ASSIST_ABBREVIATIONS.get(potential_assist, potential_assist)
            blade = normalize_blade(blade_part)
            # Parse CX blade to extract lock chip
            lock_chip, blade = parse_cx_blade(blade)
            return Combo(
                blade=blade,
                ratchet=ratchet,
                bit=bit,
                assist=assist,
                lock_chip=lock_chip,
                stage=stage,
            )

    return None


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date from various formats."""
    date_str = date_str.strip()
    formats = [
        "%m/%d/%y",
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
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
    # Try MM/DD/YY or MM/DD/YYYY
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    if match:
        date = parse_date(match.group(1))
        if date:
            return date, match.start(), match.end()

    # Try "Month DD, YYYY" or "Month DD YYYY"
    match = re.search(r"([A-Z][a-z]+ \d{1,2},? \d{4})", text)
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
    """
    text = " ".join(lines[:30])  # Check first 30 lines

    # Metal Fight indicators (should reject)
    mf_indicators = [
        r"\b\d{2,3}(RF|WD|RB|MB|CS|B:D|SF|RSF|MF)\b",  # Metal Fight tips
        r"\bMF-[FLH]\b",  # MF prefix
        r"\b(L-Drago|Pegasis|Leone|Sagittario)\b",  # Classic MF names
    ]
    for pattern in mf_indicators:
        if re.search(pattern, text, re.I):
            return False

    # Beyblade X indicators (ratchet pattern X-XX)
    x_pattern = r"\b\d{1,2}-\d{2,3}[A-Z]"  # Like 3-60F, 4-80B
    if re.search(x_pattern, text):
        return True

    # Also accept if "Beyblade X" or "X Format" is mentioned
    if re.search(r"Beyblade\s*X|X\s*Format", text, re.I):
        return True

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

    # Look at first 6 lines for header info
    header_lines = lines[:6]
    combined_text = " ".join(header_lines)

    # Extract date from combined text
    date, date_start, date_end = extract_date_from_text(combined_text)
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

        # Skip placement lines
        if re.match(r"^(1st|2nd|3rd)", line_clean, re.I):
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

        # Check for placement lines
        place_match = re.match(
            r"^(1st|2nd|3rd)\s*(Place)?[:\s-]*(.*)$", line, re.IGNORECASE
        )
        if place_match:
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

            place_str = place_match.group(1).lower()
            current_place = {"1st": 1, "2nd": 2, "3rd": 3}.get(place_str)

            # Player name might be on same line
            remainder = place_match.group(3).strip() if place_match.group(3) else ""
            if remainder and not re.search(r"\d-\d{2}", remainder):
                # No ratchet pattern, so this is probably the player name
                current_player = remainder
            else:
                current_player = None
            current_combos = []
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
                    blade_1, ratchet_1, bit_1, assist_1, lock_chip_1, stage_1,
                    blade_2, ratchet_2, bit_2, assist_2, lock_chip_2, stage_2,
                    blade_3, ratchet_3, bit_3, assist_3, lock_chip_3, stage_3
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    combos[0].stage if len(combos) > 0 else None,
                    combos[1].blade if len(combos) > 1 else None,
                    combos[1].ratchet if len(combos) > 1 else None,
                    combos[1].bit if len(combos) > 1 else None,
                    combos[1].assist if len(combos) > 1 else None,
                    combos[1].lock_chip if len(combos) > 1 else None,
                    combos[1].stage if len(combos) > 1 else None,
                    combos[2].blade if len(combos) > 2 else None,
                    combos[2].ratchet if len(combos) > 2 else None,
                    combos[2].bit if len(combos) > 2 else None,
                    combos[2].assist if len(combos) > 2 else None,
                    combos[2].lock_chip if len(combos) > 2 else None,
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
