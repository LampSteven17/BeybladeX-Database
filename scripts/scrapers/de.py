"""
DE Scraper - German tournament data from Blader League Germany Instagram.

Refactored from de_scraper.py to use the BaseScraper interface.
"""

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from tqdm import tqdm

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from base_scraper import BaseScraper, Combo, Placement, Tournament
from db import parse_cx_blade

# Try to import instaloader
try:
    import instaloader
    INSTALOADER_AVAILABLE = True
except ImportError:
    INSTALOADER_AVAILABLE = False


# =============================================================================
# Configuration
# =============================================================================

INSTAGRAM_USERNAME = "bladerleaguegermany"
DE_SOURCE_PREFIX = "blg_"


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
    """Parse a German combo string into a Combo object."""
    combo_str = combo_str.strip()
    if not combo_str:
        return None

    # Pattern: [Blade Name] [Ratchet][Bit]
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
    lock_chip, blade = parse_cx_blade(blade)

    return Combo(
        blade=blade,
        ratchet=ratchet,
        bit=bit,
        lock_chip=lock_chip
    )


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date from tournament name/caption."""
    date_str = date_str.strip()

    # German format: DD.MM.YYYY
    german_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
    if german_match:
        try:
            return datetime(
                int(german_match.group(3)),
                int(german_match.group(2)),
                int(german_match.group(1))
            )
        except ValueError:
            pass

    # ISO format: YYYY-MM-DD
    iso_match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
    if iso_match:
        try:
            return datetime(
                int(iso_match.group(1)),
                int(iso_match.group(2)),
                int(iso_match.group(3))
            )
        except ValueError:
            pass

    return None


def extract_city_from_name(name: str) -> Optional[str]:
    """Extract city name from tournament name."""
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
# DE Scraper Class
# =============================================================================

class DEScraper(BaseScraper):
    """Scraper for German tournament data from BLG Instagram."""

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    @property
    def source_name(self) -> str:
        return "Germany"

    @property
    def source_prefix(self) -> str:
        return DE_SOURCE_PREFIX

    @property
    def default_region(self) -> Optional[str]:
        return "EU"

    def clear_source_data(self, conn) -> int:
        """Clear DE data (entries with blg_ prefix)."""
        count = conn.execute(
            "SELECT COUNT(*) FROM tournaments WHERE wbo_post_id LIKE ?",
            [f"{DE_SOURCE_PREFIX}%"]
        ).fetchone()[0]

        conn.execute("""
            DELETE FROM placements WHERE tournament_id IN (
                SELECT id FROM tournaments WHERE wbo_post_id LIKE ?
            )
        """, [f"{DE_SOURCE_PREFIX}%"])

        conn.execute(
            "DELETE FROM tournaments WHERE wbo_post_id LIKE ?",
            [f"{DE_SOURCE_PREFIX}%"]
        )

        return count

    def scrape(self, conn, verbose: bool = False) -> tuple[int, int]:
        """Scrape German tournament data from Instagram."""
        if not INSTALOADER_AVAILABLE:
            print("ERROR: instaloader not installed. Run: pip install instaloader")
            return 0, 0

        processed_ids = self.get_processed_ids(conn)
        if verbose:
            print(f"Already processed {len(processed_ids)} German tournaments")

        # Initialize Instaloader
        L = instaloader.Instaloader()

        if verbose:
            print(f"Fetching posts from @{INSTAGRAM_USERNAME}...")

        try:
            profile = instaloader.Profile.from_username(L.context, INSTAGRAM_USERNAME)
            if verbose:
                print(f"Profile: {profile.full_name} ({profile.mediacount} posts)")
        except Exception as e:
            print(f"Error fetching Instagram profile: {e}")
            return 0, 0

        tournaments_added = 0
        tournaments_skipped = 0
        posts_processed = 0

        posts = profile.get_posts()
        iterator = tqdm(posts, desc="DE posts", total=profile.mediacount) if verbose else posts

        for post in iterator:
            posts_processed += 1

            # Quick skip if already processed
            post_id = f"{DE_SOURCE_PREFIX}{post.shortcode}"
            if post_id in processed_ids:
                tournaments_skipped += 1
                continue

            try:
                tournament = self._parse_instagram_post(post)

                if tournament:
                    result = self.insert_tournament(conn, tournament)
                    if result:
                        tournaments_added += 1
                        processed_ids.add(tournament.wbo_post_id)
                        if verbose:
                            tqdm.write(f"  Added: {tournament.name}")
                    else:
                        tournaments_skipped += 1

                time.sleep(self.delay)

            except Exception as e:
                if verbose:
                    tqdm.write(f"Error processing post {post.shortcode}: {e}")
                tournaments_skipped += 1

            # Commit periodically
            if posts_processed % 20 == 0:
                conn.commit()

        conn.commit()
        return tournaments_added, tournaments_skipped

    def _parse_instagram_post(self, post) -> Optional[Tournament]:
        """Parse an Instagram post into a Tournament object."""
        caption = post.caption
        if not caption:
            return None

        # Only process "Winning Combos" posts
        if "Winning Combos" not in caption and "winning combos" not in caption.lower():
            return None

        lines = [line.strip() for line in caption.split('\n') if line.strip()]
        if not lines:
            return None

        # Parse first line for tournament name
        first_line = lines[0]
        name_match = re.match(
            r'^Winning Combos\s*[–-]\s*(?:BEYBLADE X\s*)?(.+)$',
            first_line,
            re.IGNORECASE
        )

        if name_match:
            tournament_name = name_match.group(1).strip()
        else:
            tournament_name = first_line

        # Try to extract date
        tournament_date = parse_date(tournament_name)
        if not tournament_date:
            tournament_date = post.date_local

        # Clean tournament name
        tournament_name = re.sub(r'\s*\d{1,2}\.\d{1,2}\.\d{4}\s*', ' ', tournament_name).strip()

        # Extract city
        city = extract_city_from_name(tournament_name)

        # Generate ID
        post_id = f"{DE_SOURCE_PREFIX}{post.shortcode}"

        # Parse placements
        placements = []
        current_player = None
        current_place = None
        current_combos = []

        place_patterns = [
            (r'🥇\s*1\.?\s*Platz\s*\|?\s*(.+)', 1),
            (r'🥈\s*2\.?\s*Platz\s*\|?\s*(.+)', 2),
            (r'🥉\s*3\.?\s*Platz\s*\|?\s*(.+)', 3),
            (r'1\.?\s*Platz\s*\|?\s*(.+)', 1),
            (r'2\.?\s*Platz\s*\|?\s*(.+)', 2),
            (r'3\.?\s*Platz\s*\|?\s*(.+)', 3),
        ]

        for line in lines[1:]:
            matched_place = False
            for pattern, place in place_patterns:
                match = re.match(pattern, line, re.IGNORECASE)
                if match:
                    if current_player and current_combos:
                        placements.append(Placement(
                            place=current_place,
                            player_name=current_player,
                            player_wbo_id=None,
                            combos=current_combos[:3]
                        ))

                    current_place = place
                    current_player = match.group(1).strip()
                    current_combos = []
                    matched_place = True
                    break

            if matched_place:
                continue

            if line.startswith('#') or line.startswith('Lizenz') or line.startswith('Beyblade Database'):
                continue

            if current_player:
                combo = parse_combo(line)
                if combo:
                    current_combos.append(combo)

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
