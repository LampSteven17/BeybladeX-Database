"""
Fandom Wiki Scraper - Scrapes official Takara Tomy stats from Beyblade Fandom Wiki.

Extracts Attack, Defense, Stamina, Weight, Burst Resistance, and Dash stats
from individual part pages on beyblade.fandom.com.

Discovery pages:
- https://beyblade.fandom.com/wiki/List_of_Basic_Line_parts
- https://beyblade.fandom.com/wiki/List_of_Unique_Line_parts
- https://beyblade.fandom.com/wiki/List_of_Custom_Line_parts
"""

import re
import time
from typing import Optional

import cloudscraper
from bs4 import BeautifulSoup

from base_scraper import BaseScraper

# Rate limit between requests (seconds)
RATE_LIMIT = 0.5

# Base URL
WIKI_BASE = "https://beyblade.fandom.com"

# Discovery pages mapping system -> URL
DISCOVERY_PAGES = {
    "BX": f"{WIKI_BASE}/wiki/List_of_Basic_Line_parts",
    "UX": f"{WIKI_BASE}/wiki/List_of_Unique_Line_parts",
    "CX": f"{WIKI_BASE}/wiki/List_of_Custom_Line_parts",
}

# Category pages — authoritative source for the parts catalog.
# Each category lists every part of its type that exists on the wiki.
# Used by populate_parts_catalog() to build parts_catalog table.
CATEGORY_PAGES = {
    "lock_chip":  (f"{WIKI_BASE}/wiki/Category:Lock_Chips",   "CX"),
    "main_blade": (f"{WIKI_BASE}/wiki/Category:Main_Blades",  "CX"),
    "over_blade": (f"{WIKI_BASE}/wiki/Category:Over_Blades",  "CX"),
    "assist":     (f"{WIKI_BASE}/wiki/Category:Assist_Blades","CX"),
    "metal_blade":(f"{WIKI_BASE}/wiki/Category:Metal_Blades", "CX"),
    "bit":        (f"{WIKI_BASE}/wiki/Category:Bits",         None),
    "ratchet":    (f"{WIKI_BASE}/wiki/Category:Ratchets",     None),
}

# Lock chips that are made of metal (affects competitive performance).
# This isn't on the wiki — kept here as a small static list.
METAL_LOCK_CHIPS = {"Emperor", "Valkyrie"}

# Fallback parts list for BX line (in case the list page is blocked by CloudFlare)
# These are the most commonly used BX parts that might not be discovered from UX/CX pages
BX_FALLBACK_PARTS = [
    # Blades
    {"url": f"{WIKI_BASE}/wiki/Blade_-_DranSword", "part_type": "blade", "name": "Dran Sword", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_HellsScythe", "part_type": "blade", "name": "Hells Scythe", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_WizardArrow", "part_type": "blade", "name": "Wizard Arrow", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_KnightShield", "part_type": "blade", "name": "Knight Shield", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_KnightLance", "part_type": "blade", "name": "Knight Lance", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_LeonClaw", "part_type": "blade", "name": "Leon Claw", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_SharkEdge", "part_type": "blade", "name": "Shark Edge", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_ViperTail", "part_type": "blade", "name": "Viper Tail", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_DranDagger", "part_type": "blade", "name": "Dran Dagger", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_RhinoHorn", "part_type": "blade", "name": "Rhino Horn", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_PhoenixWing", "part_type": "blade", "name": "Phoenix Wing", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_HellsChain", "part_type": "blade", "name": "Hells Chain", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_UnicornSting", "part_type": "blade", "name": "Unicorn Sting", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_BlackShell", "part_type": "blade", "name": "Black Shell", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_TyrannoBeat", "part_type": "blade", "name": "Tyranno Beat", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_WeissTiger", "part_type": "blade", "name": "Weiss Tiger", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_CobaltDragoon", "part_type": "blade", "name": "Cobalt Dragoon", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_CobaltDrake", "part_type": "blade", "name": "Cobalt Drake", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_CrimsonGaruda", "part_type": "blade", "name": "Crimson Garuda", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_TalonPtera", "part_type": "blade", "name": "Talon Ptera", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_RoarTyranno", "part_type": "blade", "name": "Roar Tyranno", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_SphinxCowl", "part_type": "blade", "name": "Sphinx Cowl", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_WyvernGale", "part_type": "blade", "name": "Wyvern Gale", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_ShelterDrake", "part_type": "blade", "name": "Shelter Drake", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_TriceraPress", "part_type": "blade", "name": "Tricera Press", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_SamuraiCalibur", "part_type": "blade", "name": "Samurai Calibur", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Blade_-_BearScratch", "part_type": "blade", "name": "Bear Scratch", "system": "BX"},
    # Ratchets (BX line)
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_1-70", "part_type": "ratchet", "name": "1-70", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_2-60", "part_type": "ratchet", "name": "2-60", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_2-80", "part_type": "ratchet", "name": "2-80", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_3-60", "part_type": "ratchet", "name": "3-60", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_3-80", "part_type": "ratchet", "name": "3-80", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_4-60", "part_type": "ratchet", "name": "4-60", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_4-70", "part_type": "ratchet", "name": "4-70", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_4-80", "part_type": "ratchet", "name": "4-80", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_5-60", "part_type": "ratchet", "name": "5-60", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_5-80", "part_type": "ratchet", "name": "5-80", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_6-70", "part_type": "ratchet", "name": "6-70", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_7-60", "part_type": "ratchet", "name": "7-60", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_7-80", "part_type": "ratchet", "name": "7-80", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_9-60", "part_type": "ratchet", "name": "9-60", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_9-80", "part_type": "ratchet", "name": "9-80", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Ratchet_-_M-85", "part_type": "ratchet", "name": "M-85", "system": "BX"},
    # Bits (BX line)
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Ball", "part_type": "bit", "name": "Ball", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Cyclone", "part_type": "bit", "name": "Cyclone", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Dot", "part_type": "bit", "name": "Dot", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Elevate", "part_type": "bit", "name": "Elevate", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Flat", "part_type": "bit", "name": "Flat", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Gear_Ball", "part_type": "bit", "name": "Gear Ball", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Gear_Flat", "part_type": "bit", "name": "Gear Flat", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Gear_Needle", "part_type": "bit", "name": "Gear Needle", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Gear_Point", "part_type": "bit", "name": "Gear Point", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_High_Needle", "part_type": "bit", "name": "High Needle", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_High_Taper", "part_type": "bit", "name": "High Taper", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Kick", "part_type": "bit", "name": "Kick", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Low_Flat", "part_type": "bit", "name": "Low Flat", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Low_Needle", "part_type": "bit", "name": "Low Needle", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Merge", "part_type": "bit", "name": "Merge", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Needle", "part_type": "bit", "name": "Needle", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Orb", "part_type": "bit", "name": "Orb", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Point", "part_type": "bit", "name": "Point", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Quake", "part_type": "bit", "name": "Quake", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Rush", "part_type": "bit", "name": "Rush", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Spike", "part_type": "bit", "name": "Spike", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Taper", "part_type": "bit", "name": "Taper", "system": "BX"},
    {"url": f"{WIKI_BASE}/wiki/Bit_-_Unite", "part_type": "bit", "name": "Unite", "system": "BX"},
]

# URL prefix -> part_type mapping
# Wiki URLs follow: /wiki/{Type}_-_{Name}
URL_TYPE_MAP = {
    "Blade_-_": "blade",
    "Main_Blade_-_": "blade",
    "Lock_Chip_-_": "lock_chip",
    "Assist_Blade_-_": "assist",
    "Metal_Blade_-_": "metal_blade",
    "Over_Blade_-_": "over_blade",
    "Ratchet_-_": "ratchet",
    "Bit_-_": "bit",
    "Ratchet_Integrated_Bit_-_": "bit",
}

# CamelCase wiki names -> canonical display names (spaced)
# These match the names used in the tournament database
WIKI_NAME_TO_DISPLAY = {
    # BX Blades
    "BlackShell": "Black Shell",
    "CobaltDragoon": "Cobalt Dragoon",
    "CobaltDrake": "Cobalt Drake",
    "CrimsonGaruda": "Crimson Garuda",
    "DranDagger": "Dran Dagger",
    "DranSword": "Dran Sword",
    "HellsChain": "Hells Chain",
    "HellsScythe": "Hells Scythe",
    "KnightLance": "Knight Lance",
    "KnightShield": "Knight Shield",
    "LeonClaw": "Leon Claw",
    "PhoenixWing": "Phoenix Wing",
    "RhinoHorn": "Rhino Horn",
    "SamuraiCalibur": "Samurai Calibur",
    "SharkEdge": "Shark Edge",
    "ShelterDrake": "Shelter Drake",
    "SphinxCowl": "Sphinx Cowl",
    "TriceraPress": "Tricera Press",
    "TyrannoBeat": "Tyranno Beat",
    "UnicornSting": "Unicorn Sting",
    "ViperTail": "Viper Tail",
    "WeissTiger": "Weiss Tiger",
    "WizardArrow": "Wizard Arrow",
    "WyvernGale": "Wyvern Gale",
    "TalonPtera": "Talon Ptera",
    "RoarTyranno": "Roar Tyranno",
    "BearScratch": "Bear Scratch",
    "WhaleWave": "Whale Wave",
    "DragoonStorm": "Dragoon Storm",
    "DrigerSlash": "Driger Slash",
    "XenoXcalibur": "Xeno Xcalibur",
    "SteelSamurai": "Steel Samurai",
    "ChainIncendio": "Chain Incendio",
    "ScytheIncendio": "Scythe Incendio",
    "GillShark": "Gill Shark",
    "BiteCroc": "Bite Croc",
    "KnifeShinobi": "Knife Shinobi",
    "KeelShark": "Keel Shark",
    "OptimusPrimal": "Optimus Primal",
    # UX Blades
    "AeroPegasus": "Aero Pegasus",
    "ClockMirage": "Clock Mirage",
    "DranBuster": "Dran Buster",
    "GhostCircle": "Ghost Circle",
    "GolemRock": "Golem Rock",
    "HellsHammer": "Hells Hammer",
    "ImpactDrake": "Impact Drake",
    "KnightMail": "Knight Mail",
    "LeonCrest": "Leon Crest",
    "MeteorDragoon": "Meteor Dragoon",
    "MummyCurse": "Mummy Curse",
    "OrochiCluster": "Orochi Cluster",
    "PhoenixRudder": "Phoenix Rudder",
    "PhoenixFeather": "Phoenix Feather",
    "SamuraiSaber": "Samurai Saber",
    "ScorpioSpear": "Scorpio Spear",
    "SharkScale": "Shark Scale",
    "ShinobiShadow": "Shinobi Shadow",
    "SilverWolf": "Silver Wolf",
    "WizardRod": "Wizard Rod",
    "TuskMammoth": "Tusk Mammoth",
    "DranzerSpiral": "Dranzer Spiral",
    "HoverWyvern": "Hover Wyvern",
    # Bits with spaces
    "Bound_Spike": "Bound Spike",
    "Disk_Ball": "Disc Ball",
    "Disk_Spike": "Disc Spike",
    "Free_Ball": "Free Ball",
    "Free_Flat": "Free Flat",
    "Gear_Ball": "Gear Ball",
    "Gear_Flat": "Gear Flat",
    "Gear_Needle": "Gear Needle",
    "Gear_Point": "Gear Point",
    "Gear_Rush": "Gear Rush",
    "High_Needle": "High Needle",
    "High_Taper": "High Taper",
    "Low_Flat": "Low Flat",
    "Low_Needle": "Low Needle",
    "Low_Orb": "Low Orb",
    "Low_Rush": "Low Rush",
    "Metal_Needle": "Metal Needle",
    "Rubber_Accel": "Rubber Accel",
    "Trans_Kick": "Trans Kick",
    "Trans_Point": "Trans Point",
    "Under_Flat": "Under Flat",
    "Under_Needle": "Under Needle",
    "Wall_Ball": "Wall Ball",
    "Wall_Wedge": "Wall Wedge",
}


# Shared session for CloudFlare bypass
_scraper_session = None

def _get_session():
    """Get or create a cloudscraper session."""
    global _scraper_session
    if _scraper_session is None:
        _scraper_session = cloudscraper.create_scraper()
    return _scraper_session


def _fetch_page(url: str) -> Optional[BeautifulSoup]:
    """Fetch a wiki page and return parsed BeautifulSoup, or None on error."""
    try:
        session = _get_session()
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        print(f"  Warning: Failed to fetch {url}: {e}")
        return None


def _extract_name_from_url(url: str) -> tuple[str, str]:
    """Extract (part_type, display_name) from a wiki URL path.

    E.g., '/wiki/Blade_-_DranSword' -> ('blade', 'Dran Sword')
    """
    # Get the path after /wiki/
    path = url.split("/wiki/")[-1] if "/wiki/" in url else url

    part_type = "blade"  # default
    raw_name = path

    for prefix, ptype in URL_TYPE_MAP.items():
        if path.startswith(prefix):
            part_type = ptype
            raw_name = path[len(prefix):]
            break

    # Convert to display name
    display_name = WIKI_NAME_TO_DISPLAY.get(raw_name, None)
    if display_name is None:
        # Try with underscores replaced by spaces
        display_name = raw_name.replace("_", " ")

    return part_type, display_name


def _parse_stat_value(text: str) -> Optional[int]:
    """Parse a stat value from text, handling various formats."""
    if not text:
        return None
    # Extract first number from text
    match = re.search(r'(\d+)', text.strip())
    return int(match.group(1)) if match else None


def _parse_weight(text: str) -> Optional[float]:
    """Parse weight in grams from text like '34.6 grams (first mold)'."""
    if not text:
        return None
    # Match patterns like "34.6 grams", "6.4g", "2.2 grams"
    match = re.search(r'([\d.]+)\s*(?:grams?|g\b)', text.strip(), re.IGNORECASE)
    return float(match.group(1)) if match else None


def _extract_stats_from_page(soup: BeautifulSoup) -> dict:
    """Extract stats from a wiki page's portable-infobox.

    Fandom wiki uses pi-horizontal-group tables inside the infobox:
    - Stats table: Attack, Defense, Stamina (thead th[data-source] + tbody td)
    - Bit extra table: Dash, Burst Resistance
    - pi-data items: Weight, Spin Direction

    Returns dict with keys: attack, defense, stamina, dash, burst_resistance, weight, spin_direction
    """
    stats = {
        "attack": None,
        "defense": None,
        "stamina": None,
        "dash": None,
        "burst_resistance": None,
        "weight": None,
        "spin_direction": None,
    }

    if soup is None:
        return stats

    # Maps for matching stats by data-source attribute or header text
    DATA_SOURCE_MAP = {
        "AttackStat": "attack",
        "DefenseStat": "defense",
        "StaminaStat": "stamina",
        "DashStat": "dash",
        "BurstResistanceStat": "burst_resistance",
    }
    HEADER_TEXT_MAP = {
        "attack": "attack",
        "defense": "defense",
        "stamina": "stamina",
        "dash": "dash",
        "burst resistance": "burst_resistance",
    }

    infobox = soup.find("aside", class_="portable-infobox")
    if not infobox:
        return stats

    # Strategy 1: Parse pi-horizontal-group tables (Attack/Defense/Stamina + Dash/BurstResistance)
    for table in infobox.find_all("table", class_="pi-horizontal-group"):
        thead = table.find("thead")
        tbody = table.find("tbody")
        if not thead or not tbody:
            continue

        headers = thead.find_all("th")
        values = tbody.find_all("td")

        for th, td in zip(headers, values):
            # Try data-source attribute first
            data_source = th.get("data-source", "")
            stat_key = DATA_SOURCE_MAP.get(data_source)
            # Fall back to header text
            if not stat_key:
                header_text = th.get_text(strip=True).lower()
                stat_key = HEADER_TEXT_MAP.get(header_text)
            if stat_key:
                stats[stat_key] = _parse_stat_value(td.get_text(strip=True))

    # Strategy 2: Parse pi-data items (Weight, Spin Direction)
    for item in infobox.find_all("div", class_="pi-data"):
        label_el = item.find("h3", class_="pi-data-label")
        value_el = item.find("div", class_="pi-data-value")
        if not label_el or not value_el:
            continue
        label = label_el.get_text(strip=True).lower()
        value = value_el.get_text(strip=True)

        if label == "weight":
            stats["weight"] = _parse_weight(value)
        elif label == "spin direction":
            val_lower = value.lower()
            if "right" in val_lower:
                stats["spin_direction"] = "right"
            elif "left" in val_lower:
                stats["spin_direction"] = "left"

    return stats


def _normalize_wiki_name(raw: str) -> str:
    """Convert a wiki page slug or display title to the canonical part name.

    'Lock_Chip_-_Eva' / 'Lock Chip - Eva' -> 'Eva'
    'Bit_-_Disk_Ball' -> 'Disc Ball' (with Disk -> Disc spelling fix)
    'Main_Blade_-_Hells Hammer' -> 'Hells Hammer'
    """
    name = raw.replace("_", " ").strip()
    # Strip any "Foo - " prefix
    if " - " in name:
        name = name.split(" - ", 1)[1].strip()
    # Try the explicit display map first (handles CamelCase entries)
    if name in WIKI_NAME_TO_DISPLAY:
        return WIKI_NAME_TO_DISPLAY[name]
    # Wiki spells some bits "Disk" but tournament data uses "Disc"
    name = name.replace("Disk ", "Disc ")
    return name


def _parse_category_soup(soup, part_type: str, default_system: Optional[str]) -> list[dict]:
    """Pure parser — extracts category members from an already-parsed soup.

    Split out from _discover_from_category so the same logic works against
    HTML provided by the bookmarklet upload (no fetch involved).
    """
    if not soup:
        return []

    members_container = soup.find("div", class_="category-page__members")
    if not members_container:
        return []

    parts = []
    seen_names = set()
    for link in members_container.find_all("a", class_="category-page__member-link"):
        title = link.get_text(strip=True)
        if not title or title.startswith("Category:"):
            continue
        href = link.get("href", "")
        if not href.startswith("/wiki/"):
            continue
        name = _normalize_wiki_name(title)
        if name in seen_names:
            continue
        seen_names.add(name)
        parts.append({
            "name": name,
            "part_type": part_type,
            "system": default_system,
            "wiki_url": f"{WIKI_BASE}{href}",
        })
    return parts


def _discover_from_category(url: str, part_type: str, default_system: Optional[str]) -> list[dict]:
    """Discover all parts from a Fandom Category page (network fetch path)."""
    return _parse_category_soup(_fetch_page(url), part_type, default_system)


def _upsert_catalog_part(conn, name: str, part_type: str, system: Optional[str],
                         wiki_url: Optional[str], is_metal: bool = False) -> None:
    existing = conn.execute(
        "SELECT name, status FROM parts_catalog WHERE name = ? AND part_type = ?",
        [name, part_type],
    ).fetchone()
    if existing:
        conn.execute("""
            UPDATE parts_catalog SET
                wiki_url = COALESCE(?, wiki_url),
                system = COALESCE(system, ?),
                status = 'accepted',
                source = 'wiki',
                metal = ?,
                accepted_at = COALESCE(accepted_at, current_timestamp)
            WHERE name = ? AND part_type = ?
        """, [wiki_url, system, is_metal, name, part_type])
    else:
        conn.execute("""
            INSERT INTO parts_catalog
                (name, part_type, system, wiki_url, status, source, metal, accepted_at)
            VALUES (?, ?, ?, ?, 'accepted', 'wiki', ?, current_timestamp)
        """, [name, part_type, system, wiki_url, is_metal])


# Filename prefix → part_type, used by the RAGFlow fallback when the
# wiki Category fetches are Cloudflare-blocked. Mirrors the same convention
# used by scripts/ragflow_ingest.py and scripts/catalog.py:_parse_wiki_filename.
_RAGFLOW_FILENAME_PREFIX = {
    "lock_chip":   "Lock_Chip_-_",
    "main_blade":  "Main_Blade_-_",
    "over_blade":  "Over_Blade_-_",
    "assist":      "Assist_Blade_-_",
    "metal_blade": "Metal_Blade_-_",
    "bit":         "Bit_-_",
    "ratchet":     "Ratchet_-_",
    "blade":       "Blade_-_",
}


def _ragflow_fallback_for(part_type: str, ragflow_docs: Optional[list[dict]]) -> list[dict]:
    """Recover parts of a given type from a cached RAGFlow document listing
    when the wiki Category page fetch returns nothing (e.g., Cloudflare
    block on the prod box's IP).

    The dataset already contains every wiki page with a structured filename
    like 'Lock_Chip_-_Eva.md'. We just filter and parse — no semantic
    retrieval needed (which would be unreliable for short list queries).
    """
    if not ragflow_docs:
        return []
    prefix = _RAGFLOW_FILENAME_PREFIX.get(part_type)
    if not prefix:
        return []
    parts: list[dict] = []
    for doc in ragflow_docs:
        name = doc.get("name") or ""
        if not name.startswith(prefix) or not name.endswith(".md"):
            continue
        base = name[len(prefix):-3]
        canonical = base.replace("_", " ").replace("Disk ", "Disc ").strip()
        if not canonical:
            continue
        parts.append({
            "name": canonical,
            "part_type": part_type,
            "system": "CX" if part_type in {"lock_chip", "main_blade", "over_blade",
                                            "assist", "metal_blade"} else None,
            "wiki_url": None,  # we don't have it via this path
        })
    return parts


def populate_parts_catalog(
    conn,
    verbose: bool = False,
    pages: Optional[dict[str, str]] = None,
) -> dict:
    """Discover every Beyblade X part and upsert into the parts_catalog table.

    Discovery sources, tried in order per part type:
      1. Pre-supplied HTML bundle (`pages`) — used by the bookmarklet upload
         path. The user's browser fetches the wiki pages (no Cloudflare
         issue from a residential IP) and POSTs them to the API server,
         which calls this function with `pages={url: html_content}`.
      2. Category:* pages on the wiki — direct fetch, authoritative source.
         Used by local refresh_all.py runs.
      3. RAGFlow dataset document listing — fallback for (2) when the box's
         IP is Cloudflare-blocked. Uses chunks already ingested via
         scripts/ragflow_ingest.py.
      4. List_of_*_parts pages on the wiki — used for BX/UX standalone
         blades (which the wiki doesn't categorize together).

    Returns {part_type: count} of accepted parts written.
    """
    counts: dict[str, int] = {}

    # Helper: parse a soup directly from `pages` if the URL is in the bundle
    def _from_bundle_category(url: str, part_type: str, system: Optional[str]):
        if not pages or url not in pages:
            return None
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(pages[url], "lxml")
        return _parse_category_soup(soup, part_type, system)

    def _from_bundle_list(url: str, system: str):
        if not pages or url not in pages:
            return None
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(pages[url], "lxml")
        return _parse_list_soup(soup, system)

    # Lazily fetch RAGFlow document listing only if we actually need it
    ragflow_docs: Optional[list[dict]] = None

    def _ragflow_docs_lazy() -> Optional[list[dict]]:
        nonlocal ragflow_docs
        if ragflow_docs is not None:
            return ragflow_docs
        try:
            from ragflow import RAGFlowClient  # local import to avoid hard dep
        except Exception:
            return None
        client = RAGFlowClient()
        if not client.is_configured():
            return None
        try:
            docs = client.list_documents()
            ragflow_docs = docs
            if verbose:
                print(f"    [RAGFlow fallback] cached {len(docs)} docs from dataset")
            return docs
        except Exception as e:
            if verbose:
                print(f"    [RAGFlow fallback] list_documents failed: {e}")
            return None

    # 1. Category pages
    for part_type, (cat_url, system) in CATEGORY_PAGES.items():
        # Pass 0: in-memory bundle from bookmarklet upload
        parts = _from_bundle_category(cat_url, part_type, system)
        source = "bundle" if parts is not None else None

        if parts is None:
            if verbose:
                print(f"  Fetching {cat_url}")
            parts = _discover_from_category(cat_url, part_type, system)
            source = "fetch"

            # Stage 4 fallback: if Cloudflare blocked us, recover from RAGFlow
            if not parts:
                docs = _ragflow_docs_lazy()
                if docs:
                    parts = _ragflow_fallback_for(part_type, docs)
                    if parts and verbose:
                        print(f"    [RAGFlow fallback] recovered {len(parts)} {part_type}")
                    source = "ragflow"

        if verbose:
            print(f"    Found {len(parts)} {part_type} (via {source})")
        counts[part_type] = len(parts)

        for p in parts:
            is_metal = (part_type == "lock_chip" and p["name"] in METAL_LOCK_CHIPS)
            _upsert_catalog_part(conn, p["name"], part_type, p["system"], p["wiki_url"], is_metal)

        if source == "fetch":
            time.sleep(RATE_LIMIT)

    # 2. BX/UX standalone blades from list pages
    blade_count = 0
    for system in ("BX", "UX"):
        list_url = DISCOVERY_PAGES[system]

        # Pass 0: in-memory bundle from bookmarklet upload
        parts = _from_bundle_list(list_url, system)
        bundle_hit = parts is not None

        if parts is None:
            if verbose:
                print(f"  Fetching {list_url}")
            parts = _discover_parts_from_list_page(list_url, system)

        # Stage 4 fallback: list page blocked → recover blade list from RAGFlow.
        # We can't tell BX from UX in the RAGFlow listing alone, so we only
        # use this fallback if BOTH list pages failed AND the hardcoded
        # BX_FALLBACK_PARTS isn't enough.
        if not parts:
            docs = _ragflow_docs_lazy()
            if docs:
                rag_blades = _ragflow_fallback_for("blade", docs)
                # All RAGFlow-recovered blades get the system we're currently
                # iterating; not perfect, but the parser only cares that they
                # exist in the catalog at all.
                if rag_blades:
                    for rb in rag_blades:
                        rb["system"] = system
                    parts = rag_blades
                    if verbose:
                        print(f"    [RAGFlow fallback] recovered {len(parts)} blades for {system}")
            if not parts and system == "BX":
                if verbose:
                    print(f"    BX list page blocked, using fallback ({len(BX_FALLBACK_PARTS)})")
                parts = list(BX_FALLBACK_PARTS)

        for p in parts:
            # Only standalone blades (not lock chips, not main blades, not bits, etc.)
            if p.get("part_type") not in (None, "blade"):
                continue
            url = p.get("url") or p.get("wiki_url")
            _upsert_catalog_part(conn, p["name"], "blade", p.get("system") or system, url, False)
            blade_count += 1
        if not bundle_hit:
            time.sleep(RATE_LIMIT)
    counts["blade"] = blade_count
    if verbose:
        print(f"    Found {blade_count} BX/UX blades from list pages")

    conn.commit()
    return counts


def _discover_parts_from_list_page(url: str, system: str) -> list[dict]:
    """Discover parts from a list page (network fetch path)."""
    return _parse_list_soup(_fetch_page(url), system)


def _parse_list_soup(soup, system: str) -> list[dict]:
    """Pure parser — extracts part links from an already-parsed list page soup.

    Split out from _discover_parts_from_list_page so the same logic works
    against HTML provided by the bookmarklet upload.
    """
    if not soup:
        return []

    parts = []
    seen_urls = set()

    content = soup.find("div", class_="mw-parser-output")
    if not content:
        content = soup

    for link in content.find_all("a", href=True):
        href = link["href"]
        if not href.startswith("/wiki/"):
            continue

        path = href[6:]
        matched = False
        for prefix in URL_TYPE_MAP:
            if path.startswith(prefix):
                matched = True
                break

        if not matched:
            continue

        full_url = f"{WIKI_BASE}{href}"
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        part_type, display_name = _extract_name_from_url(href)
        parts.append({
            "url": full_url,
            "part_type": part_type,
            "name": display_name,
            "system": system,
        })

    return parts


class FandomScraper(BaseScraper):
    """Scrapes part attributes (official TT stats) from Beyblade Fandom Wiki."""

    @property
    def source_name(self) -> str:
        return "Fandom Wiki"

    @property
    def source_prefix(self) -> str:
        return "fandom_"

    @property
    def default_region(self) -> Optional[str]:
        return None

    def scrape(self, conn, verbose: bool = False) -> tuple[int, int]:
        """Scrape part catalog and stats from Fandom wiki.

        Phase 0: Populate parts_catalog from Category:* pages (source of truth for the parser)
        Phase 1: Discovery — collect all part URLs from List_of_*_parts pages (for stats)
        Phase 2: Extraction — fetch each part page and extract stats into part_attributes
        Phase 3: Emit site/public/data/parts_catalog.json for the frontend
        """
        # Phase 0: Catalog refresh
        if verbose:
            print("  Refreshing parts_catalog from wiki Category pages...")
        catalog_counts = populate_parts_catalog(conn, verbose=verbose)
        if verbose:
            total = sum(catalog_counts.values())
            print(f"  Catalog: {total} parts across {len(catalog_counts)} types")

        # Reload the in-memory PartsCatalog singleton so any subsequent
        # parsing uses the freshly-discovered wiki entries.
        from catalog import PartsCatalog
        PartsCatalog._instance = PartsCatalog.load(conn)

        # Phase 3 (executed at the end): export JSON for the frontend.
        # We do it inside scrape() so a single fandom run keeps the file fresh.
        try:
            from catalog import export_catalog_json  # local import to avoid cycles
            export_catalog_json(conn)
            if verbose:
                print("  Exported parts_catalog.json for frontend")
        except Exception as e:
            if verbose:
                print(f"  Warning: failed to export catalog JSON: {e}")

        # Phase 1: Discovery - collect all part URLs from list pages
        all_parts = []
        for system, url in DISCOVERY_PAGES.items():
            if verbose:
                print(f"  Discovering {system} parts from {url}")
            parts = _discover_parts_from_list_page(url, system)
            if not parts and system == "BX":
                # BX list page is often blocked by CloudFlare; use fallback list
                if verbose:
                    print(f"    BX list page blocked, using fallback list ({len(BX_FALLBACK_PARTS)} parts)")
                parts = list(BX_FALLBACK_PARTS)
            all_parts.extend(parts)
            if verbose:
                print(f"    Found {len(parts)} parts")
            time.sleep(RATE_LIMIT)

        if verbose:
            print(f"\n  Total parts discovered: {len(all_parts)}")

        # Deduplicate by URL
        seen = set()
        unique_parts = []
        for part in all_parts:
            if part["url"] not in seen:
                seen.add(part["url"])
                unique_parts.append(part)

        if verbose:
            print(f"  Unique parts after dedup: {len(unique_parts)}")
            by_type = {}
            for p in unique_parts:
                by_type.setdefault(p["part_type"], []).append(p["name"])
            for ptype, names in sorted(by_type.items()):
                print(f"    {ptype}: {len(names)}")

        # Phase 2: Extraction - fetch each part page and extract stats
        added = 0
        skipped = 0

        if verbose:
            from tqdm import tqdm
            iterator = tqdm(unique_parts, desc="  Scraping parts")
        else:
            iterator = unique_parts

        for part_info in iterator:
            name = part_info["name"]
            part_type = part_info["part_type"]
            system = part_info["system"]
            url = part_info["url"]

            # Check if already scraped with same data
            existing = conn.execute(
                "SELECT name FROM part_attributes WHERE name = ?", [name]
            ).fetchone()

            soup = _fetch_page(url)
            if soup is None:
                skipped += 1
                time.sleep(RATE_LIMIT)
                continue

            stats = _extract_stats_from_page(soup)

            # Skip if no stats were found at all
            has_any_stat = any(
                stats[k] is not None
                for k in ["attack", "defense", "stamina", "weight"]
            )
            if not has_any_stat:
                if verbose and not isinstance(iterator, type(unique_parts)):
                    pass  # tqdm handles output
                skipped += 1
                time.sleep(RATE_LIMIT)
                continue

            # Upsert into part_attributes
            if existing:
                conn.execute("""
                    UPDATE part_attributes SET
                        part_type = ?,
                        system = ?,
                        attack = ?,
                        defense = ?,
                        stamina = ?,
                        dash = ?,
                        burst_resistance = ?,
                        weight = ?,
                        spin_direction = ?,
                        scraped_at = current_timestamp
                    WHERE name = ?
                """, [
                    part_type, system,
                    stats["attack"], stats["defense"], stats["stamina"],
                    stats["dash"], stats["burst_resistance"],
                    stats["weight"], stats["spin_direction"],
                    name,
                ])
            else:
                conn.execute("""
                    INSERT INTO part_attributes
                        (name, part_type, system, attack, defense, stamina,
                         dash, burst_resistance, weight, spin_direction)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    name, part_type, system,
                    stats["attack"], stats["defense"], stats["stamina"],
                    stats["dash"], stats["burst_resistance"],
                    stats["weight"], stats["spin_direction"],
                ])

            added += 1
            time.sleep(RATE_LIMIT)

        conn.commit()

        if verbose:
            print(f"\n  Part attributes: {added} added/updated, {skipped} skipped")

        return added, skipped

    def clear_source_data(self, conn) -> int:
        """Clear all part attributes data."""
        count = conn.execute("SELECT COUNT(*) FROM part_attributes").fetchone()[0]
        conn.execute("DELETE FROM part_attributes")
        return count

    def get_processed_ids(self, conn) -> set[str]:
        """Get all part names already in part_attributes."""
        result = conn.execute("SELECT name FROM part_attributes").fetchall()
        return {row[0] for row in result}

    def get_stats(self, conn) -> dict:
        """Get statistics for part attributes data."""
        total = conn.execute("SELECT COUNT(*) FROM part_attributes").fetchone()[0]
        by_type = conn.execute("""
            SELECT part_type, COUNT(*) FROM part_attributes GROUP BY part_type ORDER BY COUNT(*) DESC
        """).fetchall()

        return {
            "source": self.source_name,
            "tournaments": 0,  # Not tournament data
            "placements": total,  # Reuse field for total parts
        }
