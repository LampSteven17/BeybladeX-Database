"""
WBO Forum Scraper - Windows Version (Standalone)

Run this directly on Windows PowerShell with your cookies.

Usage:
    python wbo_scraper_windows.py

It will save scraped data to wbo_data.json which can then be imported into the database.
"""

import re
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

# Check for required packages
try:
    import requests
    from bs4 import BeautifulSoup
    from tqdm import tqdm
except ImportError as e:
    print(f"Missing package: {e}")
    print("\nInstall required packages with:")
    print("  pip install requests beautifulsoup4 tqdm")
    exit(1)


BASE_URL = "https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX"
OUTPUT_FILE = Path("wbo_data.json")


# ============================================================================
# Cookie parsing
# ============================================================================


def parse_cookie_string(cookie_str: str) -> dict:
    """Parse cookie string from browser into dict."""
    cookies = {}
    # Handle both formats: "name=value; name2=value2" and JSON-like
    cookie_str = cookie_str.strip().strip('"').strip("'")

    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()
    return cookies


# ============================================================================
# Bit/Ratchet expansion
# ============================================================================

BIT_ABBREVIATIONS = {
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
    "J": "Jolt",
    "HN": "High Needle",
    "LF": "Low Flat",
    "LR": "Low Rush",
    "LN": "Low Needle",
    "GF": "Gear Flat",
    "GB": "Gear Ball",
    "GN": "Gear Needle",
    "GP": "Gear Point",
    "MN": "Metal Needle",
    "HT": "High Taper",
    "HA": "High Accel",
    "DB": "Disc Ball",
    "UN": "Under Needle",
    "RA": "Rubber Accel",
    "FB": "Free Ball",
    "WB": "Wall Ball",
}


def expand_bit(bit: str) -> str:
    """Expand bit abbreviations to full names."""
    return BIT_ABBREVIATIONS.get(bit.strip(), bit.strip())


# ============================================================================
# CX Blade parsing
# ============================================================================

CX_BLADE_COMPONENTS = {
    "Dran Brave": ("Dran", "Brave"),
    "Emperor Brave": ("Emperor", "Brave"),
    "Wizard Arc": ("Wizard", "Arc"),
    "Perseus Dark": ("Perseus", "Dark"),
    "Hells Reaper": ("Hells", "Reaper"),
    "Fox Brush": ("Fox", "Brush"),
    "Pegasus Blast": ("Pegasus", "Blast"),
    "Cerberus Blast": ("Cerberus", "Blast"),
    "Hells Blast": ("Hells", "Blast"),
    "Emperor Blast": ("Emperor", "Blast"),
    "Wolf Blast": ("Wolf", "Blast"),
    "Dran Blast": ("Dran", "Blast"),
    "Valkyrie Blast": ("Valkyrie", "Blast"),
    "Sol Blast": ("Sol", "Blast"),
    "Kraken Blast": ("Kraken", "Blast"),
    "Sol Eclipse": ("Sol", "Eclipse"),
    "Emperor Eclipse": ("Emperor", "Eclipse"),
    "Hells Eclipse": ("Hells", "Eclipse"),
    "Wolf Hunt": ("Wolf", "Hunt"),
    "Emperor Hunt": ("Emperor", "Hunt"),
    "Perseus Hunt": ("Perseus", "Hunt"),
    "Emperor Might": ("Emperor", "Might"),
    "Cerberus Might": ("Cerberus", "Might"),
    "Dran Might": ("Dran", "Might"),
    "Whale Might": ("Whale", "Might"),
    "Phoenix Flare": ("Phoenix", "Flare"),
    "Valkyrie Volt": ("Valkyrie", "Volt"),
}

CX_LOCK_CHIPS = {
    "Dran",
    "Emperor",
    "Wizard",
    "Perseus",
    "Hells",
    "Fox",
    "Pegasus",
    "Cerberus",
    "Wolf",
    "Valkyrie",
    "Sol",
    "Phoenix",
    "Whale",
    "Kraken",
}
CX_MAIN_BLADES = {
    "Brave",
    "Arc",
    "Dark",
    "Reaper",
    "Brush",
    "Blast",
    "Eclipse",
    "Hunt",
    "Might",
    "Flare",
    "Volt",
    "Storm",
}

_CX_LOCK_CHIPS_LOWER = {c.lower(): c for c in CX_LOCK_CHIPS}
_CX_MAIN_BLADES_LOWER = {b.lower(): b for b in CX_MAIN_BLADES}


def parse_cx_blade(blade_name: str) -> tuple:
    """Parse CX blade into (lock_chip, main_blade) or (None, blade_name)."""
    if blade_name in CX_BLADE_COMPONENTS:
        return CX_BLADE_COMPONENTS[blade_name]

    # Strip suffixes like "W", "S", "A"
    normalized = blade_name.strip()
    if len(normalized) > 2 and normalized[-2] == " " and normalized[-1] in "WSAFHT":
        stripped = normalized[:-2]
        if stripped in CX_BLADE_COMPONENTS:
            return CX_BLADE_COMPONENTS[stripped]
        normalized = stripped

    # Try case-insensitive
    for full_name, components in CX_BLADE_COMPONENTS.items():
        if full_name.lower() == normalized.lower():
            return components

    # Try splitting by space
    parts = normalized.split()
    if len(parts) >= 2:
        first_lower, second_lower = parts[0].lower(), parts[1].lower()
        if (
            first_lower in _CX_LOCK_CHIPS_LOWER
            and second_lower in _CX_MAIN_BLADES_LOWER
        ):
            return (
                _CX_LOCK_CHIPS_LOWER[first_lower],
                _CX_MAIN_BLADES_LOWER[second_lower],
            )
        if (
            first_lower in _CX_MAIN_BLADES_LOWER
            and second_lower in _CX_LOCK_CHIPS_LOWER
        ):
            return (
                _CX_LOCK_CHIPS_LOWER[second_lower],
                _CX_MAIN_BLADES_LOWER[first_lower],
            )

    return (None, blade_name)


# ============================================================================
# Blade/Bit Normalization - Add spaces to CamelCase, fix common issues
# ============================================================================


def normalize_blade_name(blade: str) -> str:
    """Normalize blade name - add spaces to CamelCase like 'WizardRod' -> 'Wizard Rod'"""
    if not blade:
        return blade
    # If already has space, return as-is
    if " " in blade:
        return blade
    # Insert space before uppercase letters (except at start)
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", blade)
    return spaced


def normalize_bit_name(bit: str) -> str:
    """Normalize bit name - expand abbreviations and add spaces"""
    if not bit:
        return bit

    # First expand any remaining abbreviations not caught by expand_bit
    extra_expansions = {
        "LowOrb": "Low Orb",
        "WallBall": "Wall Ball",
        "FreeBall": "Free Ball",
        "HighNeedle": "High Needle",
        "LowFlat": "Low Flat",
        "LowRush": "Low Rush",
        "LowNeedle": "Low Needle",
        "GearFlat": "Gear Flat",
        "GearBall": "Gear Ball",
        "GearNeedle": "Gear Needle",
        "GearPoint": "Gear Point",
        "MetalNeedle": "Metal Needle",
        "HighTaper": "High Taper",
        "HighAccel": "High Accel",
        "DiscBall": "Disc Ball",
        "RubberAccel": "Rubber Accel",
        "UnderNeedle": "Under Needle",
        "UpperFlat": "Upper Flat",
        "RushAccel": "Rush Accel",
    }

    if bit in extra_expansions:
        return extra_expansions[bit]

    # Add spaces to CamelCase
    if " " not in bit:
        bit = re.sub(r"([a-z])([A-Z])", r"\1 \2", bit)

    return bit


# ============================================================================
# Assist parsing
# ============================================================================

# Known assist abbreviations
ASSIST_ABBREVIATIONS = {
    "W": "Wheel",
    "H": "Heavy",
    "S": "Slash",
    "J": "Jaggy",
    "B": "Bumper",
    "L": "Low",
    "A": "Assault",
    "R": "Rush",
}

KNOWN_ASSISTS = {"Jaggy", "Slash", "Wheel", "Bumper", "Heavy", "Assault", "Rush", "Low"}
KNOWN_ASSISTS_LOWER = {a.lower() for a in KNOWN_ASSISTS}


def split_blade_assist(blade_text: str) -> tuple:
    """
    Split blade text into (blade, assist) if there's an assist code.
    Examples:
    - "Pegasus Blast Jaggy" -> ("Pegasus Blast", "Jaggy")
    - "Dran Sword S" -> ("Dran Sword", "Slash")
    - "Wizard Rod" -> ("Wizard Rod", None)
    """
    blade_text = blade_text.strip()
    words = blade_text.split()

    if len(words) <= 1:
        return blade_text, None

    last_word = words[-1]

    # Check if last word is an assist
    is_assist = (
        last_word in ASSIST_ABBREVIATIONS
        or last_word.lower() in KNOWN_ASSISTS_LOWER
        or (
            len(last_word) == 1
            and last_word.isupper()
            and last_word in ASSIST_ABBREVIATIONS
        )
    )

    if is_assist:
        blade = " ".join(words[:-1])
        assist = ASSIST_ABBREVIATIONS.get(last_word, last_word)
        return blade, assist

    return blade_text, None


# ============================================================================
# Combo parsing
# ============================================================================


def parse_combo(combo_str: str) -> dict | None:
    """Parse a combo string like 'Dran Sword 3-60F' into components."""
    combo_str = combo_str.strip().lstrip("-•*").strip()
    if not combo_str:
        return None

    # Remove stage annotations
    combo_str = re.sub(
        r"\s*\([^)]*(?:Stage|Finals|Only|Match)[^)]*\)", "", combo_str, flags=re.I
    )
    combo_str = combo_str.strip()
    if not combo_str:
        return None

    # Pattern: [Blade] [Ratchet][Bit] or [Blade] [Ratchet] [Bit]
    # Try with space before bit
    match = re.match(r"^(.+?)\s+(\d{1,2}-\d{2,3})\s+([A-Za-z][A-Za-z\s]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        ratchet = match.group(2).strip()
        bit = expand_bit(match.group(3).strip())
        bit = normalize_bit_name(bit)
        # Split blade and assist
        blade_only, assist = split_blade_assist(blade_part)
        lock_chip, blade = parse_cx_blade(blade_only)
        blade = normalize_blade_name(blade)
        return {
            "blade": blade,
            "lock_chip": lock_chip,
            "ratchet": ratchet,
            "bit": bit,
            "assist": assist,
        }

    # Try with bit attached to ratchet
    match = re.match(r"^(.+?)\s+(\d{1,2}-\d{2,3})([A-Z][A-Za-z]*)$", combo_str)
    if match:
        blade_part = match.group(1).strip()
        ratchet = match.group(2).strip()
        bit = expand_bit(match.group(3).strip())
        bit = normalize_bit_name(bit)
        # Split blade and assist
        blade_only, assist = split_blade_assist(blade_part)
        lock_chip, blade = parse_cx_blade(blade_only)
        blade = normalize_blade_name(blade)
        return {
            "blade": blade,
            "lock_chip": lock_chip,
            "ratchet": ratchet,
            "bit": bit,
            "assist": assist,
        }

    return None


# ============================================================================
# Tournament parsing
# ============================================================================


def parse_date(date_str: str) -> str | None:
    """Parse date string into ISO format."""
    formats = ["%m/%d/%y", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"]
    now = datetime.now()

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)

            # For 2-digit years, Python assumes 1969-2068 range
            # BeybladeX started in 2023, so valid range is 2023 to present
            if fmt == "%m/%d/%y":
                # If year is before 2023, it was probably meant to be 2023+
                if dt.year < 2023:
                    dt = dt.replace(year=dt.year + 100)
                # If year is more than 1 month in the future, subtract 100
                # (allows for some tournaments posted slightly ahead)
                elif dt > now + timedelta(days=60):
                    dt = dt.replace(year=dt.year - 100)

            # Final sanity check - must be between 2023 and present + 60 days
            if dt.year < 2023:
                continue
            if dt > now + timedelta(days=60):
                continue

            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_date(text: str) -> str | None:
    """Extract date from text."""
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    if match:
        return parse_date(match.group(1))
    match = re.search(r"([A-Z][a-z]+ \d{1,2},? \d{4})", text)
    if match:
        return parse_date(match.group(1))
    return None


def is_beyblade_x(lines: list) -> bool:
    """Check if content is Beyblade X (not Metal Fight, Burst)."""
    text = " ".join(lines[:30])
    # Reject Metal Fight
    if re.search(r"\b\d{2,3}(RF|WD|RB|MB|CS|B:D|SF)\b", text, re.I):
        return False
    # Accept X format ratchets
    if re.search(r"\b\d{1,2}-\d{2,3}[A-Z]", text):
        return True
    if re.search(r"Beyblade\s*X|X\s*Format", text, re.I):
        return True
    return False


def parse_post(post_element) -> list:
    """Parse a forum post and extract tournaments."""
    tournaments = []

    post_id = post_element.get("id", "")
    if not post_id.startswith("pid"):
        return tournaments

    body = post_element.find("div", class_="post_body")
    if not body:
        return tournaments

    text = body.get_text(separator="\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # Skip instructions post
    if "This thread is for Beyblade X combinations" in text:
        return tournaments

    if not is_beyblade_x(lines):
        return tournaments

    # Extract tournament info from header
    header_text = " ".join(lines[:6])
    date = extract_date(header_text)

    # Find tournament name (first non-date, non-place line)
    name = None
    for line in lines[:6]:
        if re.match(r"^(1st|2nd|3rd)", line, re.I):
            break
        if not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", line):
            if len(line) > 3 and not line.lower().startswith(
                ("beyblade", "x format", "ranked")
            ):
                name = line.strip()
                break

    current_tournament = None
    placements = []
    current_place = None
    current_player = None
    current_combos = []

    for line in lines:
        # Check for placement (1st, 2nd, 3rd, etc.)
        place_match = re.match(
            r"^(1st|2nd|3rd|\d+(?:st|nd|rd|th))\s*(?:Place)?[:\s-]*(.*)$", line, re.I
        )
        if place_match:
            # Save previous placement
            if current_place and current_player and current_combos:
                placements.append(
                    {
                        "place": current_place,
                        "player": current_player,
                        "combos": current_combos,
                    }
                )

            place_str = place_match.group(1).lower()
            place_map = {"1st": 1, "2nd": 2, "3rd": 3}
            current_place = place_map.get(place_str)
            if not current_place:
                num_match = re.search(r"\d+", place_str)
                current_place = int(num_match.group()) if num_match else 0

            rest = place_match.group(2).strip()
            current_player = rest.split()[0] if rest else None
            current_combos = []

            # Check if combo on same line
            if rest:
                combo = parse_combo(rest)
                if combo:
                    current_combos.append(combo)
            continue

        # If we have a place but no player, this might be player name
        if current_place and not current_player:
            if re.match(r"^[A-Za-z0-9_\[\]]+$", line) and len(line) <= 30:
                current_player = line
                continue

        # Try parsing as combo
        if current_place:
            combo = parse_combo(line)
            if combo:
                current_combos.append(combo)

    # Save last placement
    if current_place and current_player and current_combos:
        placements.append(
            {"place": current_place, "player": current_player, "combos": current_combos}
        )

    if placements:
        tournaments.append(
            {
                "wbo_post_id": post_id,
                "name": name or "Unknown Tournament",
                "date": date,
                "placements": placements,
            }
        )

    return tournaments


# ============================================================================
# Scraping
# ============================================================================


def get_total_pages(html: str) -> int:
    """Get total page count from pagination."""
    matches = re.findall(r"page=(\d+)", html)
    return max(int(m) for m in matches) if matches else 1


def scrape_page(html: str) -> list:
    """Scrape all tournaments from a page."""
    soup = BeautifulSoup(html, "html.parser")
    tournaments = []

    for post in soup.find_all("div", class_="post"):
        try:
            tournaments.extend(parse_post(post))
        except Exception as e:
            print(f"  Warning: {e}")

    return tournaments


def main():
    print("=" * 60)
    print("WBO Scraper - Windows Version")
    print("=" * 60)

    # Hardcoded cookies
    cookies = {
        "cf_clearance": "ABF36KFCAWGM0bjOodBC4XSoQwrpeY65P0sseymuVQE-1769181208-1.2.1.1-1MEGNUOvSP.aQK6ZMbQTCcz_ZQGlXTLh9kZZIiC9qlH0i9TAQQp.71tRHaDReZ5VMPxyGBujpIPAy2TIJcefbw9TXJsElJmT2y.U.l2DPg.UfCNZPSPhpeXgpZ9C_l2WE0pDzpd5NL6ln0oqWBVcXnw4jLIWfVBnPqP7oQqOUtbERfI8aEvQJfyfMYOuRtT0LqgNH1cK5u5kvalDm5DqaVy4O62_JblVMDVCp.glvMzb0fiEcvw5kJ5cjmKiO5IB",
        "mybb[lastvisit]": "1769122975",
        "mybb[lastactive]": "1769181208",
        "mybb[threadread]": "a%3A3%3A%7Bi%3A110113%3Bi%3A1769181207%3Bi%3A122672%3Bi%3A1768938547%3Bi%3A123107%3Bi%3A1769122974%3B%7D",
        "mybb[announcements]": "0",
    }
    print("Using hardcoded cookies...")

    # Create session - must match Firefox 147.0.1 exactly
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
    )
    # Note: removed Accept-Encoding to let requests handle decompression automatically
    session.cookies.update(cookies)

    print("\nTesting connection...")
    response = session.get(BASE_URL, timeout=30)

    print(f"Status: {response.status_code}")
    print(f"Content length: {len(response.text)}")

    # Debug: show what we got
    if "Winning Combinations" in response.text:
        print("SUCCESS! Connected to WBO")
    elif "Just a moment" in response.text:
        print("ERROR: Cloudflare is still blocking.")
        return
    elif "login" in response.text.lower() and "password" in response.text.lower():
        print("ERROR: Got a login page instead")
        return
    else:
        # Show first part of title to debug
        import re

        title_match = re.search(r"<title>([^<]+)</title>", response.text)
        if title_match:
            print(f"Page title: {title_match.group(1)}")
        print(f"First 500 chars: {response.text[:500]}")

        # Check if it's actually the WBO page with different text
        if "worldbeyblade" in response.text.lower() or "WBO" in response.text:
            print("\nLooks like WBO page - continuing anyway...")
        else:
            print("ERROR: Unknown page content")
            return

    total_pages = get_total_pages(response.text)
    print(f"Found {total_pages} pages to scrape")

    all_tournaments = []

    for page_num in tqdm(range(1, total_pages + 1), desc="Scraping"):
        try:
            if page_num == 1:
                html = response.text
            else:
                url = f"{BASE_URL}&page={page_num}"
                response = session.get(url, timeout=30)
                html = response.text
                time.sleep(0.3)

            if "Just a moment" in html:
                print(f"\nBlocked on page {page_num}!")
                break

            tournaments = scrape_page(html)
            all_tournaments.extend(tournaments)

        except Exception as e:
            print(f"\nError on page {page_num}: {e}")

    # Save to JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_tournaments, f, indent=2, ensure_ascii=False)

    print()
    print("=" * 60)
    print("COMPLETE!")
    print("=" * 60)
    print(f"Tournaments scraped: {len(all_tournaments)}")
    print(f"Data saved to: {OUTPUT_FILE.absolute()}")
    print()
    print("To import into the database, copy wbo_data.json to WSL and run:")
    print("  python scripts/import_wbo_json.py")


if __name__ == "__main__":
    main()
