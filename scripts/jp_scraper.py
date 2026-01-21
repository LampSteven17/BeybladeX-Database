"""
Japanese Tournament Scraper for okuyama3093.com

Scrapes Beyblade X tournament results from:
https://okuyama3093.com/beybladex-tournamentresult-matome/

Features:
- Requests-based scraping (primary) with Playwright fallback
- Japanese to English part name translation
- Deduplication with existing WBO data
- Support for G1, Championship, and other tournament types
"""

import re
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from db import get_connection, init_schema, normalize_data, parse_cx_blade
from translations import (
    translate_blade,
    translate_bit,
    translate_lock_chip,
    translate_assist,
    is_japanese,
    BLADE_TRANSLATIONS,
    BIT_TRANSLATIONS,
)

# Try to import playwright, provide helpful message if not installed
try:
    from playwright.sync_api import sync_playwright, Page, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Note: Playwright not available. Using requests-based scraping.")


# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://okuyama3093.com/beybladex-tournamentresult-matome/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Source identifier prefix for Japanese tournaments
JP_SOURCE_PREFIX = "okuyama_"


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
    wbo_post_id: str  # Using this field for source tracking (with okuyama_ prefix)
    name: str
    date: Optional[datetime]
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = "Japan"
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
}


def expand_bit(bit: str) -> str:
    """Expand bit abbreviations to full names."""
    bit = bit.strip()
    # First check Japanese translations
    if is_japanese(bit):
        return translate_bit(bit)
    # Then check English abbreviations
    return BIT_ABBREVIATIONS.get(bit, bit)


# =============================================================================
# Combo Parsing
# =============================================================================

def parse_jp_combo(combo_str: str) -> Optional[Combo]:
    """
    Parse a Japanese combo string into a Combo object.

    Handles formats like:
    - "ドランソード 3-60F" (Japanese blade, ratchet+bit)
    - "Dran Sword 3-60F" (Already translated)
    - "ペガサスブラスト 4-80B" (CX blade)
    - "ウィザードロッド ジャギー 5-60N" (With assist)
    """
    combo_str = combo_str.strip()
    if not combo_str:
        return None

    # Pattern 1: [Blade] [Assist?] [Ratchet][Bit]
    # Try with assist first
    match_with_assist = re.match(
        r'^(.+?)\s+([ァ-ヶー]+|[A-Z][a-z]+)\s+(\d{1,2}-\d{2,3})([A-Za-zァ-ヶー]+)$',
        combo_str
    )
    if match_with_assist:
        blade_jp = match_with_assist.group(1).strip()
        assist_jp = match_with_assist.group(2).strip()
        ratchet = match_with_assist.group(3)
        bit_jp = match_with_assist.group(4).strip()

        # Check if the "assist" is actually part of the blade name
        # by seeing if blade+assist forms a known blade
        combined = blade_jp + assist_jp
        if combined in BLADE_TRANSLATIONS or not is_japanese(assist_jp):
            # It's not an assist, fall through to pattern without assist
            pass
        else:
            blade = translate_blade(blade_jp)
            assist = translate_assist(assist_jp)
            bit = expand_bit(bit_jp)

            # Parse CX blade for lock chip
            lock_chip, blade = parse_cx_blade(blade)

            return Combo(
                blade=blade,
                ratchet=ratchet,
                bit=bit,
                assist=assist if assist != assist_jp else None,  # Only include if translated
                lock_chip=lock_chip
            )

    # Pattern 2: [Blade] [Ratchet][Bit] (no assist)
    match_simple = re.match(
        r'^(.+?)\s+(\d{1,2}-\d{2,3})([A-Za-zァ-ヶー]+)$',
        combo_str
    )
    if match_simple:
        blade_jp = match_simple.group(1).strip()
        ratchet = match_simple.group(2)
        bit_jp = match_simple.group(3).strip()

        blade = translate_blade(blade_jp)
        bit = expand_bit(bit_jp)

        # Parse CX blade for lock chip
        lock_chip, blade = parse_cx_blade(blade)

        return Combo(
            blade=blade,
            ratchet=ratchet,
            bit=bit,
            lock_chip=lock_chip
        )

    # Pattern 3: [Blade] [Ratchet] [Bit] (space between ratchet and bit)
    match_spaced = re.match(
        r'^(.+?)\s+(\d{1,2}-\d{2,3})\s+([A-Za-zァ-ヶー]+)$',
        combo_str
    )
    if match_spaced:
        blade_jp = match_spaced.group(1).strip()
        ratchet = match_spaced.group(2)
        bit_jp = match_spaced.group(3).strip()

        blade = translate_blade(blade_jp)
        bit = expand_bit(bit_jp)

        lock_chip, blade = parse_cx_blade(blade)

        return Combo(
            blade=blade,
            ratchet=ratchet,
            bit=bit,
            lock_chip=lock_chip
        )

    return None


# =============================================================================
# Date Parsing
# =============================================================================

def parse_jp_date(date_str: str) -> Optional[datetime]:
    """
    Parse Japanese date formats.

    Handles:
    - "2024年12月15日" (Japanese format)
    - "2024-12-15" (ISO format)
    - "2024/12/15"
    - "12/15/2024"
    - "December 15, 2024"
    """
    date_str = date_str.strip()

    # ISO format: YYYY-MM-DD (common in datetime attributes)
    iso_match = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})$', date_str)
    if iso_match:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        day = int(iso_match.group(3))
        return datetime(year, month, day)

    # Japanese format: YYYY年MM月DD日
    jp_match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
    if jp_match:
        year = int(jp_match.group(1))
        month = int(jp_match.group(2))
        day = int(jp_match.group(3))
        return datetime(year, month, day)

    # Slash format: YYYY/MM/DD or MM/DD/YYYY
    slash_match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', date_str)
    if slash_match:
        year = int(slash_match.group(1))
        month = int(slash_match.group(2))
        day = int(slash_match.group(3))
        return datetime(year, month, day)

    # Try other common formats
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None


# =============================================================================
# Requests-based Scraping (Primary method - no browser needed)
# =============================================================================

class RequestsScraper:
    """Scraper using requests + BeautifulSoup (no browser required)."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en;q=0.9",
        })

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def fetch_page(self, url: str) -> str:
        """Fetch a page and return its HTML content."""
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.text

    def get_tournament_links(self) -> list[dict[str, str]]:
        """Get all tournament article links from the main page."""
        html = self.fetch_page(BASE_URL)
        soup = BeautifulSoup(html, 'lxml')

        tournaments = []

        # Find all links that look like tournament result pages
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if not href:
                continue

            # Must be a direct okuyama3093.com link (not social media shares)
            if not href.startswith('https://okuyama3093.com/'):
                continue

            # Filter to tournament result pages
            if any(keyword in href.lower() for keyword in [
                'result', 'championship', 'xtremecup', 'g1result'
            ]):
                # Skip non-result pages
                if any(skip in href for skip in ['bladelist', 'ratchetlist', 'bitlist', 'weight', 'matome']):
                    continue

                title = link.get_text().strip() or href.split('/')[-2]
                if href not in [t['url'] for t in tournaments]:
                    tournaments.append({
                        "url": href,
                        "title": title,
                        "date": None
                    })

        return tournaments

    def parse_tournament_page(self, url: str) -> Optional[Tournament]:
        """Parse a single tournament page and extract results."""
        try:
            html = self.fetch_page(url)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

        soup = BeautifulSoup(html, 'lxml')

        # Extract tournament name from title
        title_elem = soup.find('h1') or soup.find('title')
        tournament_name = title_elem.get_text().strip() if title_elem else "Unknown Tournament"
        # Clean up title
        tournament_name = re.sub(r'\s*[|｜]\s*.*$', '', tournament_name)
        tournament_name = re.sub(r'【.*?】', '', tournament_name).strip()

        # Extract date - try multiple methods
        tournament_date = None

        # Method 1: Look for datetime attributes in HTML
        time_elem = soup.find('time', attrs={'datetime': True})
        if time_elem:
            datetime_attr = time_elem.get('datetime', '')
            if datetime_attr:
                tournament_date = parse_jp_date(datetime_attr)

        # Method 2: Look for Japanese date format in text
        if not tournament_date:
            content = soup.get_text()
            # Look for Japanese date format: YYYY年MM月DD日
            jp_date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', content)
            if jp_date_match:
                year = int(jp_date_match.group(1))
                month = int(jp_date_match.group(2))
                day = int(jp_date_match.group(3))
                tournament_date = datetime(year, month, day)

        # Method 3: Extract year from URL and use first day of year as fallback
        if not tournament_date:
            year_match = re.search(r'(202[0-9])', url)
            if year_match:
                year = int(year_match.group(1))
                tournament_date = datetime(year, 1, 1)

        # Generate unique ID from URL
        url_slug = url.rstrip('/').split('/')[-1]
        post_id = f"{JP_SOURCE_PREFIX}{url_slug}"

        tournament = Tournament(
            wbo_post_id=post_id,
            name=tournament_name,
            date=tournament_date,
            country="Japan",
            wbo_url=url,
        )

        # Parse placements from the page content
        placements = self._parse_placements_from_soup(soup)
        tournament.placements = placements

        return tournament

    def _parse_placements_from_soup(self, soup: BeautifulSoup) -> list[Placement]:
        """Parse placement data from BeautifulSoup object.

        Handles two formats:
        1. Table-based match results (championship finals)
        2. Text-based G1 regional results (winner/runner-up lists)
        """
        # Find all match tables
        tables = soup.find_all('table')

        # If no tables found, try text-based G1 format
        if not tables:
            return self._parse_g1_text_format(soup)

        # Dictionary to track each player's combos
        player_combos: dict[str, list[Combo]] = {}

        for table in tables:
            rows = table.find_all('tr')
            if not rows:
                continue

            # First row is header with player names
            header = rows[0]
            cells = header.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            # Extract player names from header (format: "PlayerName使用ベイ" or "PlayerName\n使用ベイ")
            player1_text = cells[0].get_text().strip()
            player2_text = cells[1].get_text().strip()

            # Extract player name (before "使用ベイ")
            player1_match = re.match(r'^(.+?)(?:使用ベイ|$)', player1_text, re.DOTALL)
            player2_match = re.match(r'^(.+?)(?:使用ベイ|$)', player2_text, re.DOTALL)

            if not player1_match or not player2_match:
                continue

            player1_name = player1_match.group(1).strip()
            player2_name = player2_match.group(1).strip()

            # Skip if names look like header labels
            if not player1_name or not player2_name:
                continue
            if '勝ち方' in player1_text or 'ポイント' in player1_text:
                continue

            # Remove common suffixes like "さん"
            player1_name = re.sub(r'さん$', '', player1_name)
            player2_name = re.sub(r'さん$', '', player2_name)

            # Initialize combo lists for players
            if player1_name not in player_combos:
                player_combos[player1_name] = []
            if player2_name not in player_combos:
                player_combos[player2_name] = []

            # Process match rows (skip header)
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue

                # Each cell contains blade + ratchet combo
                for i, (cell, player_name) in enumerate([(cells[0], player1_name), (cells[1], player2_name)]):
                    cell_text = cell.get_text().strip()

                    # Clean up cell text (remove bold markers, underscores)
                    cell_text = re.sub(r'[_\*]', '', cell_text)

                    # Try to parse combo - can be "BladeName\nX-XXBit" or "BladeNameX-XXBit" or "BladeName X-XX Bit"
                    # Pattern: Japanese/English blade name followed by ratchet-bit (with optional spaces)
                    combo_match = re.match(r'^(.+?)(\d{1,2}-\d{2,3})\s*([A-Za-z]*)$', cell_text.replace('\n', ''))
                    if combo_match:
                        blade_jp = combo_match.group(1).strip()
                        ratchet = combo_match.group(2)
                        bit_jp = combo_match.group(3) or ""

                        blade = translate_blade(blade_jp)
                        bit = expand_bit(bit_jp) if bit_jp else ""

                        combo = Combo(
                            blade=blade,
                            ratchet=ratchet,
                            bit=bit
                        )

                        # Add to player's combo list if not duplicate
                        existing = [(c.blade, c.ratchet, c.bit) for c in player_combos[player_name]]
                        if (combo.blade, combo.ratchet, combo.bit) not in existing:
                            player_combos[player_name].append(combo)

        # Convert to placements
        # Since we can't determine exact ranking from match data, assign sequential places
        # starting from 1. The data still captures which combos were used.
        placements = []
        for i, (player_name, combos) in enumerate(player_combos.items(), start=1):
            if combos:
                placements.append(Placement(
                    place=i,  # Sequential placement for uniqueness
                    player_name=player_name,
                    player_wbo_id=None,
                    combos=combos[:3]  # Limit to 3 combos
                ))

        return placements

    def _parse_g1_text_format(self, soup: BeautifulSoup) -> list[Placement]:
        """Parse G1 regional tournament results from text-based format.

        Handles format like:
        【優勝者：PlayerName選手の3on3デッキ】
        エアロペガサス 7-60 R
        フェニックスウイング 9-60 LR
        ウィザードロッド 3-60 E
        【準優勝者：PlayerName選手の3on3デッキ】
        ...
        """
        placements = []
        place_counter = 0

        # Get all text content
        content = soup.find('div', class_='entry-content') or soup.find('article') or soup
        if not content:
            return []

        text = content.get_text()
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        current_player = None
        current_place = None
        current_combos = []
        current_region = None

        # Patterns for player headers
        winner_pattern = re.compile(r'【優勝者[：:](.+?)(?:選手)?(?:の3on3デッキ)?】')
        runner_up_pattern = re.compile(r'【準優勝者[：:](.+?)(?:選手)?(?:の3on3デッキ)?】')

        # Region patterns (G1 locations)
        region_pattern = re.compile(r'(大阪|仙台|福岡|広島|東京|札幌|名古屋|神戸)')

        for line in lines:
            # Check for region marker
            region_match = region_pattern.search(line)
            if region_match and ('G1' in line or '予選' in line or '大会結果' in line):
                current_region = region_match.group(1)

            # Check for winner
            winner_match = winner_pattern.search(line)
            if winner_match:
                # Save previous player if exists
                if current_player and current_combos:
                    place_counter += 1
                    player_name = current_player
                    if current_region:
                        player_name = f"{current_player} ({current_region})"
                    placements.append(Placement(
                        place=place_counter,
                        player_name=player_name,
                        player_wbo_id=None,
                        combos=current_combos[:3]
                    ))

                current_player = winner_match.group(1).strip()
                current_place = 1
                current_combos = []
                continue

            # Check for runner-up
            runner_match = runner_up_pattern.search(line)
            if runner_match:
                # Save previous player if exists
                if current_player and current_combos:
                    place_counter += 1
                    player_name = current_player
                    if current_region:
                        player_name = f"{current_player} ({current_region})"
                    placements.append(Placement(
                        place=place_counter,
                        player_name=player_name,
                        player_wbo_id=None,
                        combos=current_combos[:3]
                    ))

                current_player = runner_match.group(1).strip()
                current_place = 2
                current_combos = []
                continue

            # If we have a current player, try to parse combo
            if current_player:
                combo = parse_jp_combo(line)
                if combo:
                    current_combos.append(combo)

        # Don't forget the last player
        if current_player and current_combos:
            place_counter += 1
            player_name = current_player
            if current_region:
                player_name = f"{current_player} ({current_region})"
            placements.append(Placement(
                place=place_counter,
                player_name=player_name,
                player_wbo_id=None,
                combos=current_combos[:3]
            ))

        return placements


# =============================================================================
# Playwright-based Scraping (Fallback for JavaScript-heavy pages)
# =============================================================================

class JapaneseScraper:
    """Scraper for Japanese tournament data using Playwright."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self._playwright = None

    def __enter__(self):
        if not PLAYWRIGHT_AVAILABLE:
            raise RuntimeError("Playwright not installed. Run: pip install playwright && playwright install chromium")
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(headless=self.headless)
        self.page = self.browser.new_page(user_agent=USER_AGENT)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            self.browser.close()
        if self._playwright:
            self._playwright.stop()

    def fetch_page(self, url: str, wait_selector: str = "body") -> str:
        """Fetch a page and return its HTML content."""
        self.page.goto(url, wait_until="networkidle")
        self.page.wait_for_selector(wait_selector, timeout=30000)
        return self.page.content()

    def get_tournament_links(self) -> list[dict[str, str]]:
        """
        Get all tournament article links from the main page.

        Returns list of dicts with 'url', 'title', and 'date' keys.
        """
        self.page.goto(BASE_URL, wait_until="networkidle")

        # Wait for content to load
        time.sleep(2)

        # Find all article links - adjust selectors based on actual site structure
        tournaments = []

        # Try to find tournament links in the page
        # The exact selectors will depend on the site's HTML structure
        links = self.page.query_selector_all("article a, .entry-title a, h2 a, h3 a")

        for link in links:
            href = link.get_attribute("href")
            title = link.inner_text().strip()

            if href and title:
                # Filter to only tournament result pages
                if any(keyword in title.lower() or keyword in href.lower()
                       for keyword in ["大会", "選手権", "tournament", "g1", "結果", "result"]):
                    tournaments.append({
                        "url": href,
                        "title": title,
                        "date": None  # Will be extracted from page
                    })

        return tournaments

    def parse_tournament_page(self, url: str) -> Optional[Tournament]:
        """
        Parse a single tournament page and extract results.

        This method attempts to parse various formats of Japanese tournament
        result pages. The exact parsing logic may need adjustment based on
        the actual page structure.
        """
        try:
            html = self.fetch_page(url)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

        # Extract tournament info from page content
        content = self.page.inner_text("body")
        lines = [line.strip() for line in content.split("\n") if line.strip()]

        # Try to extract tournament name from title
        title_elem = self.page.query_selector("h1, .entry-title, article h1")
        tournament_name = title_elem.inner_text().strip() if title_elem else "Unknown Tournament"

        # Extract date from content
        tournament_date = None
        for line in lines[:20]:  # Check first 20 lines for date
            date = parse_jp_date(line)
            if date:
                tournament_date = date
                break

        # Generate unique ID from URL
        url_hash = re.sub(r'[^\w]', '_', url.split("/")[-2] if url.endswith("/") else url.split("/")[-1])
        post_id = f"{JP_SOURCE_PREFIX}{url_hash}"

        tournament = Tournament(
            wbo_post_id=post_id,
            name=tournament_name,
            date=tournament_date,
            country="Japan",
            wbo_url=url,
        )

        # Parse placements
        placements = self._parse_placements(lines)
        tournament.placements = placements

        return tournament

    def _parse_placements(self, lines: list[str]) -> list[Placement]:
        """
        Parse placement data from page content.

        Looks for patterns like:
        - "1位: [Player Name]"
        - "優勝: [Player Name]"
        - "[Combo1], [Combo2], [Combo3]"
        """
        placements = []
        current_place = None
        current_player = None
        current_combos = []

        # Placement indicators
        place_patterns = [
            (r'^1位\s*[:：]?\s*(.+)$', 1),
            (r'^2位\s*[:：]?\s*(.+)$', 2),
            (r'^3位\s*[:：]?\s*(.+)$', 3),
            (r'^優勝\s*[:：]?\s*(.+)$', 1),
            (r'^準優勝\s*[:：]?\s*(.+)$', 2),
            (r'^1st\s*(?:Place)?\s*[:：]?\s*(.+)$', 1),
            (r'^2nd\s*(?:Place)?\s*[:：]?\s*(.+)$', 2),
            (r'^3rd\s*(?:Place)?\s*[:：]?\s*(.+)$', 3),
        ]

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check for placement line
            matched_place = False
            for pattern, place in place_patterns:
                match = re.match(pattern, line, re.IGNORECASE)
                if match:
                    # Save previous placement if exists
                    if current_place is not None and current_player and current_combos:
                        placements.append(Placement(
                            place=current_place,
                            player_name=current_player,
                            player_wbo_id=None,
                            combos=current_combos
                        ))

                    current_place = place
                    # Player name might be on same line or next line
                    remainder = match.group(1).strip() if match.group(1) else ""
                    if remainder and not re.search(r'\d-\d{2}', remainder):
                        current_player = remainder
                    else:
                        current_player = None
                    current_combos = []
                    matched_place = True
                    break

            if matched_place:
                continue

            # If we're in a placement section
            if current_place is not None:
                # Check if this looks like a player name
                if current_player is None and not re.search(r'\d-\d{2}', line):
                    if len(line) < 50 and not any(c in line for c in [':', '：', '、', ',']):
                        current_player = line
                        continue

                # Try to parse as combo
                combo = parse_jp_combo(line)
                if combo:
                    current_combos.append(combo)
                    continue

                # Check for comma-separated combos
                if '、' in line or ',' in line:
                    parts = re.split(r'[、,]', line)
                    for part in parts:
                        combo = parse_jp_combo(part.strip())
                        if combo:
                            current_combos.append(combo)

        # Don't forget the last placement
        if current_place is not None and current_player and current_combos:
            placements.append(Placement(
                place=current_place,
                player_name=current_player,
                player_wbo_id=None,
                combos=current_combos
            ))

        return placements


# =============================================================================
# Database Integration
# =============================================================================

def get_processed_jp_ids(conn) -> set[str]:
    """Get all post IDs for Japanese tournaments we've already processed."""
    result = conn.execute(
        "SELECT wbo_post_id FROM tournaments WHERE wbo_post_id LIKE ?",
        [f"{JP_SOURCE_PREFIX}%"]
    ).fetchall()
    return {row[0] for row in result}


def tournament_exists_by_name_date(conn, name: str, date: datetime) -> bool:
    """
    Check if a tournament with similar name and date already exists.

    Used to detect duplicates between WBO and Japanese sources.
    """
    if not date:
        return False

    # Check for exact or similar name match on same date
    date_str = date.strftime('%Y-%m-%d')

    # Try exact match first
    result = conn.execute(
        "SELECT COUNT(*) FROM tournaments WHERE date = ? AND name = ?",
        [date_str, name]
    ).fetchone()

    if result[0] > 0:
        return True

    # Try fuzzy match - same date and name contains key tournament words
    keywords = ["g1", "championship", "選手権", "大会"]
    for keyword in keywords:
        if keyword.lower() in name.lower():
            result = conn.execute(
                "SELECT COUNT(*) FROM tournaments WHERE date = ? AND LOWER(name) LIKE ?",
                [date_str, f"%{keyword}%"]
            ).fetchone()
            if result[0] > 0:
                return True

    return False


def insert_jp_tournament(conn, tournament: Tournament) -> Optional[int]:
    """Insert a Japanese tournament and its placements."""
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

    # Check for duplicates from WBO source
    if tournament_exists_by_name_date(conn, tournament.name, tournament.date):
        print(f"  Skipping {tournament.name}: duplicate detected from WBO source")
        return None

    # Insert tournament with JAPAN region
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
        "JAPAN",  # Use JAPAN region for Japanese source
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

def scrape_japanese_tournaments(max_tournaments: Optional[int] = None, delay: float = 2.0, use_playwright: bool = False):
    """
    Scrape Japanese tournament data from okuyama3093.com.

    Args:
        max_tournaments: Maximum number of tournaments to scrape (None for all)
        delay: Delay between requests in seconds
        use_playwright: If True, use Playwright (requires browser); else use requests
    """
    conn = get_connection()
    init_schema(conn)

    processed_ids = get_processed_jp_ids(conn)
    print(f"Already processed {len(processed_ids)} Japanese tournaments")

    tournaments_added = 0
    tournaments_skipped = 0

    # Choose scraper based on preference and availability
    if use_playwright:
        if not PLAYWRIGHT_AVAILABLE:
            print("Error: Playwright not available. Using requests-based scraping instead.")
            use_playwright = False

    scraper_name = "Playwright" if use_playwright else "requests"
    print(f"Using {scraper_name}-based scraping...")

    # Create scraper instance
    if use_playwright:
        scraper_instance = JapaneseScraper(headless=True)
    else:
        scraper_instance = RequestsScraper()

    with scraper_instance as scraper:
        print("Fetching tournament list...")
        tournament_links = scraper.get_tournament_links()

        if max_tournaments:
            tournament_links = tournament_links[:max_tournaments]

        print(f"Found {len(tournament_links)} tournament pages")

        for i, link_info in enumerate(tqdm(tournament_links, desc="Tournaments")):
            url = link_info["url"]
            title = link_info["title"]

            # Quick skip if already processed
            url_hash = re.sub(r'[^\w]', '_', url.split("/")[-2] if url.endswith("/") else url.split("/")[-1])
            post_id = f"{JP_SOURCE_PREFIX}{url_hash}"

            if post_id in processed_ids:
                tournaments_skipped += 1
                continue

            try:
                time.sleep(delay)
                tournament = scraper.parse_tournament_page(url)

                if tournament:
                    result = insert_jp_tournament(conn, tournament)
                    if result:
                        tournaments_added += 1
                        processed_ids.add(tournament.wbo_post_id)
                        print(f"  Added: {tournament.name} ({tournament.date})")
                    else:
                        tournaments_skipped += 1
                else:
                    tournaments_skipped += 1

            except Exception as e:
                print(f"Error processing {url}: {e}")
                tournaments_skipped += 1

            # Commit periodically
            if (i + 1) % 10 == 0:
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
    """Test Japanese combo parsing."""
    test_cases = [
        "ドランソード 3-60F",
        "ヘルズサイズ 4-80B",
        "ペガサスブラスト 5-60N",
        "ウィザードロッド ジャギー 3-60HN",
        "Dran Sword 3-60F",  # Already English
        "シルバーウルフ 9-60LF",
    ]

    print("Testing Japanese combo parsing:")
    for combo_str in test_cases:
        result = parse_jp_combo(combo_str)
        if result:
            lock_chip_str = f" [{result.lock_chip}]" if result.lock_chip else ""
            assist_str = f" + {result.assist}" if result.assist else ""
            print(f"  {combo_str}")
            print(f"    -> {result.blade}{lock_chip_str}{assist_str} {result.ratchet} {result.bit}")
        else:
            print(f"  {combo_str} -> FAILED TO PARSE")


def show_stats():
    """Show Japanese tournament statistics."""
    conn = get_connection()

    jp_tournaments = conn.execute(
        "SELECT COUNT(*) FROM tournaments WHERE wbo_post_id LIKE ?",
        [f"{JP_SOURCE_PREFIX}%"]
    ).fetchone()[0]

    jp_placements = conn.execute("""
        SELECT COUNT(*) FROM placements p
        JOIN tournaments t ON p.tournament_id = t.id
        WHERE t.wbo_post_id LIKE ?
    """, [f"{JP_SOURCE_PREFIX}%"]).fetchone()[0]

    all_tournaments = conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0]

    print(f"\n=== JAPANESE TOURNAMENT STATS ===")
    print(f"Japanese tournaments: {jp_tournaments}")
    print(f"Japanese placements: {jp_placements}")
    print(f"Total tournaments (all sources): {all_tournaments}")

    if jp_tournaments > 0:
        print(f"\n=== RECENT JAPANESE TOURNAMENTS ===")
        for row in conn.execute("""
            SELECT name, date, city
            FROM tournaments
            WHERE wbo_post_id LIKE ?
            ORDER BY date DESC
            LIMIT 5
        """, [f"{JP_SOURCE_PREFIX}%"]).fetchall():
            print(f"  {row[1]}: {row[0]} ({row[2] or 'Japan'})")

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
            # Scrape N tournaments
            scrape_japanese_tournaments(max_tournaments=int(cmd))
        else:
            print("Usage:")
            print("  python jp_scraper.py test    - Test combo parsing")
            print("  python jp_scraper.py stats   - Show Japanese tournament stats")
            print("  python jp_scraper.py N       - Scrape N tournaments")
            print("  python jp_scraper.py         - Scrape all tournaments")
    else:
        # Default: scrape all
        scrape_japanese_tournaments()
