"""
German Tournament Scraper for Blader League Germany (BLG)

Scrapes Beyblade X tournament results from Instagram:
https://www.instagram.com/bladerleaguegermany/

Features:
- Instaloader-based Instagram scraping
- Parses "Winning Combos" posts with placement data
- Supports 3on3 format (3 combos per player)
- EU region classification
"""

import re
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

import instaloader
from tqdm import tqdm

from db import get_connection, init_schema, normalize_data, parse_cx_blade

# =============================================================================
# Configuration
# =============================================================================

INSTAGRAM_USERNAME = "bladerleaguegermany"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Source identifier prefix for German tournaments
DE_SOURCE_PREFIX = "blg_"


# =============================================================================
# Data Classes (matching WBO scraper structure)
# =============================================================================

@dataclass
class Combo:
    blade: str
    ratchet: str
    bit: str
    assist: Optional[str] = None
    lock_chip: Optional[str] = None


@dataclass
class Placement:
    place: int
    player_name: str
    player_wbo_id: Optional[str]
    combos: list[Combo] = field(default_factory=list)


@dataclass
class Tournament:
    wbo_post_id: str  # Using this field for source tracking (with blg_ prefix)
    name: str
    date: Optional[datetime]
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = "Germany"
    format: Optional[str] = None
    ranked: Optional[bool] = None
    wbo_url: Optional[str] = None
    placements: list[Placement] = field(default_factory=list)


# =============================================================================
# Bit Abbreviation Expansion
# =============================================================================

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
    "LO": "Low Orb",
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
    "FB": "Free Ball",
    "RA": "Rush Accel",
}


def expand_bit(bit: str) -> str:
    """Expand bit abbreviations to full names."""
    bit = bit.strip().upper()
    return BIT_ABBREVIATIONS.get(bit, bit)


# =============================================================================
# Combo Parsing
# =============================================================================

def parse_combo(combo_str: str) -> Optional[Combo]:
    """
    Parse a German combo string into a Combo object.

    Format: "Blade Name X-XXY" where X-XX is ratchet, Y is bit
    Examples:
    - "T. Rex 1-70B"
    - "Wolf Blast F4-50H"
    - "Shark Scale 1-70LR"
    - "Emperor Blast H3-60LR"
    """
    combo_str = combo_str.strip()
    if not combo_str:
        return None

    # Pattern: [Blade Name] [Ratchet][Bit]
    # Ratchet can be: digit-digit or letter+digit-digit (e.g., "1-70", "F4-50", "H3-60")
    # Bit is 1-2 letters at the end
    match = re.match(
        r'^(.+?)\s+([A-Z]?\d{1,2}-\d{2,3})([A-Z]{1,2})$',
        combo_str,
        re.IGNORECASE
    )

    if not match:
        return None

    blade = match.group(1).strip()
    ratchet = match.group(2)
    bit_abbrev = match.group(3).upper()

    bit = expand_bit(bit_abbrev)

    # Parse CX blade for lock chip
    lock_chip, blade = parse_cx_blade(blade)

    return Combo(
        blade=blade,
        ratchet=ratchet,
        bit=bit,
        lock_chip=lock_chip
    )


# =============================================================================
# Date Parsing
# =============================================================================

# German month names
GERMAN_MONTHS = {
    "januar": 1, "jan": 1,
    "februar": 2, "feb": 2,
    "märz": 3, "mar": 3, "maerz": 3,
    "april": 4, "apr": 4,
    "mai": 5,
    "juni": 6, "jun": 6,
    "juli": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "oktober": 10, "okt": 10, "oct": 10,
    "november": 11, "nov": 11,
    "dezember": 12, "dez": 12, "dec": 12,
}


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date from tournament name/caption.

    Handles formats:
    - "18.01.2026" (German format: DD.MM.YYYY)
    - "17.01.2026"
    - "2026-01-18" (ISO)
    """
    date_str = date_str.strip()

    # German format: DD.MM.YYYY
    german_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
    if german_match:
        day = int(german_match.group(1))
        month = int(german_match.group(2))
        year = int(german_match.group(3))
        try:
            return datetime(year, month, day)
        except ValueError:
            pass

    # ISO format: YYYY-MM-DD
    iso_match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
    if iso_match:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        day = int(iso_match.group(3))
        try:
            return datetime(year, month, day)
        except ValueError:
            pass

    return None


def extract_city_from_name(name: str) -> Optional[str]:
    """Extract city name from tournament name."""
    # Common German cities in tournament names
    cities = [
        "Berlin", "Hamburg", "München", "Munich", "Köln", "Cologne",
        "Frankfurt", "Stuttgart", "Düsseldorf", "Dortmund", "Essen",
        "Leipzig", "Bremen", "Dresden", "Hannover", "Nürnberg", "Nuremberg",
        "Duisburg", "Bochum", "Wuppertal", "Bielefeld", "Bonn", "Münster",
        "Karlsruhe", "Mannheim", "Augsburg", "Wiesbaden", "Braunschweig",
        "Bad Homburg", "Walldorf", "Kaiserslautern", "Erfurt", "Kiel",
    ]

    for city in cities:
        if city.lower() in name.lower():
            return city

    return None


# =============================================================================
# Instagram Post Parsing
# =============================================================================

def parse_instagram_post(post) -> Optional[Tournament]:
    """
    Parse an Instagram post into a Tournament object.

    Expected format:
    Winning Combos – BEYBLADE X [Tournament Name] [Date]

    🥇 1. Platz | PlayerName
    Combo1
    Combo2
    Combo3

    🥈 2. Platz | PlayerName
    ...
    """
    caption = post.caption
    if not caption:
        return None

    # Only process "Winning Combos" posts
    if "Winning Combos" not in caption and "winning combos" not in caption.lower():
        return None

    lines = [line.strip() for line in caption.split('\n') if line.strip()]

    if not lines:
        return None

    # Parse first line for tournament name and date
    first_line = lines[0]

    # Extract tournament name (remove "Winning Combos – BEYBLADE X" prefix)
    name_match = re.match(
        r'^Winning Combos\s*[–-]\s*(?:BEYBLADE X\s*)?(.+)$',
        first_line,
        re.IGNORECASE
    )

    if name_match:
        tournament_name = name_match.group(1).strip()
    else:
        tournament_name = first_line

    # Try to extract date from tournament name or use post date
    tournament_date = parse_date(tournament_name)
    if not tournament_date:
        # Use Instagram post date
        tournament_date = post.date_local

    # Clean tournament name (remove date if present)
    tournament_name = re.sub(r'\s*\d{1,2}\.\d{1,2}\.\d{4}\s*', ' ', tournament_name).strip()

    # Extract city
    city = extract_city_from_name(tournament_name)

    # Generate unique ID from post shortcode
    post_id = f"{DE_SOURCE_PREFIX}{post.shortcode}"

    # Parse placements
    placements = []
    current_player = None
    current_place = None
    current_combos = []

    # Patterns for placement headers
    place_patterns = [
        (r'🥇\s*1\.?\s*Platz\s*\|?\s*(.+)', 1),
        (r'🥈\s*2\.?\s*Platz\s*\|?\s*(.+)', 2),
        (r'🥉\s*3\.?\s*Platz\s*\|?\s*(.+)', 3),
        (r'1\.?\s*Platz\s*\|?\s*(.+)', 1),
        (r'2\.?\s*Platz\s*\|?\s*(.+)', 2),
        (r'3\.?\s*Platz\s*\|?\s*(.+)', 3),
    ]

    for line in lines[1:]:  # Skip first line (title)
        # Check if this is a placement header
        matched_place = False
        for pattern, place in place_patterns:
            match = re.match(pattern, line, re.IGNORECASE)
            if match:
                # Save previous placement if exists
                if current_player and current_combos:
                    placements.append(Placement(
                        place=current_place,
                        player_name=current_player,
                        player_wbo_id=None,
                        combos=current_combos[:3]  # Max 3 combos
                    ))

                current_place = place
                current_player = match.group(1).strip()
                current_combos = []
                matched_place = True
                break

        if matched_place:
            continue

        # Skip non-combo lines
        if line.startswith('#') or line.startswith('Lizenz') or line.startswith('Beyblade Database'):
            continue

        # Try to parse as combo
        if current_player:
            combo = parse_combo(line)
            if combo:
                current_combos.append(combo)

    # Don't forget the last placement
    if current_player and current_combos:
        placements.append(Placement(
            place=current_place,
            player_name=current_player,
            player_wbo_id=None,
            combos=current_combos[:3]
        ))

    if not placements:
        return None

    return Tournament(
        wbo_post_id=post_id,
        name=tournament_name,
        date=tournament_date,
        city=city,
        country="Germany",
        wbo_url=f"https://www.instagram.com/p/{post.shortcode}/",
        placements=placements
    )


# =============================================================================
# Database Integration
# =============================================================================

def get_processed_de_ids(conn) -> set[str]:
    """Get all post IDs for German tournaments we've already processed."""
    result = conn.execute(
        "SELECT wbo_post_id FROM tournaments WHERE wbo_post_id LIKE ?",
        [f"{DE_SOURCE_PREFIX}%"]
    ).fetchall()
    return {row[0] for row in result}


def insert_de_tournament(conn, tournament: Tournament) -> Optional[int]:
    """Insert a German tournament and its placements."""
    if not tournament.date:
        print(f"  Skipping {tournament.name}: no date")
        return None

    if not tournament.placements:
        print(f"  Skipping {tournament.name}: no placements")
        return None

    # Check if already processed (by post ID)
    existing = conn.execute(
        "SELECT id FROM tournaments WHERE wbo_post_id = ?",
        [tournament.wbo_post_id]
    ).fetchone()

    if existing:
        return None  # Skip, already processed

    # Insert tournament with EU region
    result = conn.execute("""
        INSERT INTO tournaments (wbo_post_id, name, date, city, state, country, region, format, ranked, wbo_thread_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
    """, [
        tournament.wbo_post_id,
        tournament.name,
        tournament.date.strftime('%Y-%m-%d'),
        tournament.city,
        tournament.state,
        tournament.country,
        "EU",  # Use EU region for German tournaments
        tournament.format,
        tournament.ranked,
        tournament.wbo_url
    ])

    tournament_id = result.fetchone()[0]

    # Insert placements
    for placement in tournament.placements:
        if not placement.combos:
            continue

        combos = placement.combos[:3]

        try:
            conn.execute("""
                INSERT INTO placements (
                    tournament_id, place, player_name, player_wbo_id,
                    blade_1, ratchet_1, bit_1, assist_1, lock_chip_1,
                    blade_2, ratchet_2, bit_2, assist_2, lock_chip_2,
                    blade_3, ratchet_3, bit_3, assist_3, lock_chip_3
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                tournament_id,
                placement.place,
                placement.player_name,
                placement.player_wbo_id,
                combos[0].blade if len(combos) > 0 else None,
                combos[0].ratchet if len(combos) > 0 else None,
                combos[0].bit if len(combos) > 0 else None,
                combos[0].assist if len(combos) > 0 else None,
                combos[0].lock_chip if len(combos) > 0 else None,
                combos[1].blade if len(combos) > 1 else None,
                combos[1].ratchet if len(combos) > 1 else None,
                combos[1].bit if len(combos) > 1 else None,
                combos[1].assist if len(combos) > 1 else None,
                combos[1].lock_chip if len(combos) > 1 else None,
                combos[2].blade if len(combos) > 2 else None,
                combos[2].ratchet if len(combos) > 2 else None,
                combos[2].bit if len(combos) > 2 else None,
                combos[2].assist if len(combos) > 2 else None,
                combos[2].lock_chip if len(combos) > 2 else None,
            ])
        except Exception as e:
            print(f"Error inserting placement for {placement.player_name}: {e}")

    return tournament_id


# =============================================================================
# Main Scraping Functions
# =============================================================================

def scrape_german_tournaments(max_posts: Optional[int] = None, delay: float = 1.0):
    """
    Scrape German tournament data from BLG Instagram.

    Args:
        max_posts: Maximum number of posts to process (None for all)
        delay: Delay between requests in seconds
    """
    conn = get_connection()
    init_schema(conn)

    processed_ids = get_processed_de_ids(conn)
    print(f"Already processed {len(processed_ids)} German tournaments")

    # Initialize Instaloader
    L = instaloader.Instaloader()

    print(f"Fetching posts from @{INSTAGRAM_USERNAME}...")

    try:
        profile = instaloader.Profile.from_username(L.context, INSTAGRAM_USERNAME)
        print(f"Profile: {profile.full_name} ({profile.mediacount} posts)")
    except Exception as e:
        print(f"Error fetching profile: {e}")
        return

    tournaments_added = 0
    tournaments_skipped = 0
    posts_processed = 0

    posts = profile.get_posts()

    for post in tqdm(posts, desc="Processing posts", total=profile.mediacount if not max_posts else max_posts):
        if max_posts and posts_processed >= max_posts:
            break

        posts_processed += 1

        # Quick skip if already processed
        post_id = f"{DE_SOURCE_PREFIX}{post.shortcode}"
        if post_id in processed_ids:
            tournaments_skipped += 1
            continue

        try:
            tournament = parse_instagram_post(post)

            if tournament:
                result = insert_de_tournament(conn, tournament)
                if result:
                    tournaments_added += 1
                    processed_ids.add(tournament.wbo_post_id)
                    print(f"  Added: {tournament.name} ({tournament.date.strftime('%Y-%m-%d') if tournament.date else 'no date'})")
                else:
                    tournaments_skipped += 1
            else:
                # Not a tournament post (e.g., announcement, promo)
                pass

            time.sleep(delay)

        except Exception as e:
            print(f"Error processing post {post.shortcode}: {e}")
            tournaments_skipped += 1

        # Commit periodically
        if posts_processed % 20 == 0:
            conn.commit()

    # Final commit
    conn.commit()

    # Normalize data
    print("Normalizing data...")
    fixed_count = normalize_data(conn)
    if fixed_count > 0:
        print(f"Fixed {fixed_count} typos/inconsistencies")
        conn.commit()

    conn.close()
    print(f"\nDone! Added {tournaments_added} tournaments, skipped {tournaments_skipped}")


def test_combo_parsing():
    """Test German combo parsing."""
    test_cases = [
        "T. Rex 1-70B",
        "Wolf Blast F4-50H",
        "Soar Phoenix 1-60RA",
        "Shark Scale 1-60J",
        "Hover Wyvern 9-60K",
        "Wizard Rod 5-70H",
        "Sterling Wolf 7-60FB",
        "Emperor Blast H3-60LR",
        "Clock Mirage 7-55LO",
        "Cobalt Dragoon 5-60E",
    ]

    print("Testing German combo parsing:")
    for combo_str in test_cases:
        result = parse_combo(combo_str)
        if result:
            lock_chip_str = f" [{result.lock_chip}]" if result.lock_chip else ""
            print(f"  {combo_str}")
            print(f"    -> {result.blade}{lock_chip_str} {result.ratchet} {result.bit}")
        else:
            print(f"  {combo_str} -> FAILED TO PARSE")


def show_stats():
    """Show German tournament statistics."""
    conn = get_connection()

    de_tournaments = conn.execute(
        "SELECT COUNT(*) FROM tournaments WHERE wbo_post_id LIKE ?",
        [f"{DE_SOURCE_PREFIX}%"]
    ).fetchone()[0]

    de_placements = conn.execute("""
        SELECT COUNT(*) FROM placements p
        JOIN tournaments t ON p.tournament_id = t.id
        WHERE t.wbo_post_id LIKE ?
    """, [f"{DE_SOURCE_PREFIX}%"]).fetchone()[0]

    all_tournaments = conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0]

    print(f"\n=== GERMAN TOURNAMENT STATS ===")
    print(f"German tournaments: {de_tournaments}")
    print(f"German placements: {de_placements}")
    print(f"Total tournaments (all sources): {all_tournaments}")

    if de_tournaments > 0:
        print(f"\n=== RECENT GERMAN TOURNAMENTS ===")
        for row in conn.execute("""
            SELECT name, date, city
            FROM tournaments
            WHERE wbo_post_id LIKE ?
            ORDER BY date DESC
            LIMIT 5
        """, [f"{DE_SOURCE_PREFIX}%"]).fetchall():
            print(f"  {row[1]}: {row[0]} ({row[2] or 'Germany'})")

    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "test":
            test_combo_parsing()
        elif cmd == "stats":
            show_stats()
        elif cmd.isdigit():
            # Scrape N posts
            scrape_german_tournaments(max_posts=int(cmd))
        else:
            print("Usage:")
            print("  python de_scraper.py test    - Test combo parsing")
            print("  python de_scraper.py stats   - Show German tournament stats")
            print("  python de_scraper.py N       - Scrape N posts")
            print("  python de_scraper.py         - Scrape all posts")
    else:
        # Default: scrape all
        scrape_german_tournaments()
