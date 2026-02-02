"""
JP Scraper - Japanese tournament data from okuyama3093.com.

Refactored from jp_scraper.py to use the BaseScraper interface.
"""

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from base_scraper import BaseScraper, Combo, Placement, Tournament
from db import parse_cx_blade
from translations import (
    translate_blade,
    translate_bit,
    translate_assist,
    is_japanese,
    BLADE_TRANSLATIONS,
)


# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://okuyama3093.com/beybladex-tournamentresult-matome/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
JP_SOURCE_PREFIX = "okuyama_"


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
    if is_japanese(bit):
        return translate_bit(bit)
    return BIT_ABBREVIATIONS.get(bit, bit)


# =============================================================================
# Combo Parsing
# =============================================================================

def parse_jp_combo(combo_str: str) -> Optional[Combo]:
    """Parse a Japanese combo string into a Combo object."""
    combo_str = combo_str.strip()
    if not combo_str:
        return None

    # Pattern with assist
    match_with_assist = re.match(
        r'^(.+?)\s+([ァ-ヶー]+|[A-Z][a-z]+)\s+(\d{1,2}-\d{2,3})([A-Za-zァ-ヶー]+)$',
        combo_str
    )
    if match_with_assist:
        blade_jp = match_with_assist.group(1).strip()
        assist_jp = match_with_assist.group(2).strip()
        ratchet = match_with_assist.group(3)
        bit_jp = match_with_assist.group(4).strip()

        combined = blade_jp + assist_jp
        if combined in BLADE_TRANSLATIONS or not is_japanese(assist_jp):
            pass  # Fall through to pattern without assist
        else:
            blade = translate_blade(blade_jp)
            assist = translate_assist(assist_jp)
            bit = expand_bit(bit_jp)
            lock_chip, blade = parse_cx_blade(blade)

            return Combo(
                blade=blade,
                ratchet=ratchet,
                bit=bit,
                assist=assist if assist != assist_jp else None,
                lock_chip=lock_chip
            )

    # Pattern without assist
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
        lock_chip, blade = parse_cx_blade(blade)

        return Combo(
            blade=blade,
            ratchet=ratchet,
            bit=bit,
            lock_chip=lock_chip
        )

    # Pattern with space between ratchet and bit
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


def parse_jp_date(date_str: str) -> Optional[datetime]:
    """Parse Japanese date formats."""
    date_str = date_str.strip()

    # ISO format
    iso_match = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})$', date_str)
    if iso_match:
        return datetime(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)))

    # Japanese format
    jp_match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
    if jp_match:
        return datetime(int(jp_match.group(1)), int(jp_match.group(2)), int(jp_match.group(3)))

    # Slash format
    slash_match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', date_str)
    if slash_match:
        return datetime(int(slash_match.group(1)), int(slash_match.group(2)), int(slash_match.group(3)))

    return None


# =============================================================================
# JP Scraper Class
# =============================================================================

class JPScraper(BaseScraper):
    """Scraper for Japanese tournament data from okuyama3093.com."""

    def __init__(self, delay: float = 2.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en;q=0.9",
        })

    @property
    def source_name(self) -> str:
        return "Japan"

    @property
    def source_prefix(self) -> str:
        return JP_SOURCE_PREFIX

    @property
    def default_region(self) -> Optional[str]:
        return "JAPAN"

    def clear_source_data(self, conn) -> int:
        """Clear JP data (entries with okuyama_ prefix)."""
        count = conn.execute(
            "SELECT COUNT(*) FROM tournaments WHERE wbo_post_id LIKE ?",
            [f"{JP_SOURCE_PREFIX}%"]
        ).fetchone()[0]

        conn.execute("""
            DELETE FROM placements WHERE tournament_id IN (
                SELECT id FROM tournaments WHERE wbo_post_id LIKE ?
            )
        """, [f"{JP_SOURCE_PREFIX}%"])

        conn.execute(
            "DELETE FROM tournaments WHERE wbo_post_id LIKE ?",
            [f"{JP_SOURCE_PREFIX}%"]
        )

        return count

    def scrape(self, conn, verbose: bool = False) -> tuple[int, int]:
        """Scrape Japanese tournament data."""
        processed_ids = self.get_processed_ids(conn)
        if verbose:
            print(f"Already processed {len(processed_ids)} Japanese tournaments")

        tournaments_added = 0
        tournaments_skipped = 0

        if verbose:
            print("Fetching tournament list from okuyama3093.com...")

        tournament_links = self._get_tournament_links()

        if verbose:
            print(f"Found {len(tournament_links)} tournament pages")

        iterator = tqdm(tournament_links, desc="JP tournaments") if verbose else tournament_links

        for i, link_info in enumerate(iterator):
            url = link_info["url"]

            # Generate post ID
            url_slug = url.rstrip('/').split('/')[-1]
            post_id = f"{JP_SOURCE_PREFIX}{url_slug}"

            if post_id in processed_ids:
                tournaments_skipped += 1
                continue

            try:
                time.sleep(self.delay)
                tournament = self._parse_tournament_page(url)

                if tournament:
                    result = self.insert_tournament(conn, tournament)
                    if result:
                        tournaments_added += 1
                        processed_ids.add(tournament.wbo_post_id)
                        if verbose:
                            tqdm.write(f"  Added: {tournament.name}")
                    else:
                        tournaments_skipped += 1
                else:
                    tournaments_skipped += 1

            except Exception as e:
                if verbose:
                    tqdm.write(f"Error processing {url}: {e}")
                tournaments_skipped += 1

            # Commit periodically
            if (i + 1) % 10 == 0:
                conn.commit()

        conn.commit()
        return tournaments_added, tournaments_skipped

    def _get_tournament_links(self) -> list[dict[str, str]]:
        """Get tournament links from main page."""
        try:
            response = self.session.get(BASE_URL, timeout=30)
            response.raise_for_status()
        except Exception as e:
            print(f"Error fetching main page: {e}")
            return []

        soup = BeautifulSoup(response.text, 'lxml')
        tournaments = []

        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if not href or not href.startswith('https://okuyama3093.com/'):
                continue

            if any(keyword in href.lower() for keyword in ['result', 'championship', 'xtremecup', 'g1result']):
                if any(skip in href for skip in ['bladelist', 'ratchetlist', 'bitlist', 'weight', 'matome']):
                    continue

                title = link.get_text().strip() or href.split('/')[-2]
                if href not in [t['url'] for t in tournaments]:
                    tournaments.append({"url": href, "title": title})

        return tournaments

    def _parse_tournament_page(self, url: str) -> Optional[Tournament]:
        """Parse a tournament page."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            return None

        soup = BeautifulSoup(response.text, 'lxml')

        # Extract name
        title_elem = soup.find('h1') or soup.find('title')
        name = title_elem.get_text().strip() if title_elem else "Unknown Tournament"
        name = re.sub(r'\s*[|｜]\s*.*$', '', name)
        name = re.sub(r'【.*?】', '', name).strip()

        # Extract date
        tournament_date = None
        time_elem = soup.find('time', attrs={'datetime': True})
        if time_elem:
            tournament_date = parse_jp_date(time_elem.get('datetime', ''))

        if not tournament_date:
            content = soup.get_text()
            jp_date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', content)
            if jp_date_match:
                tournament_date = datetime(
                    int(jp_date_match.group(1)),
                    int(jp_date_match.group(2)),
                    int(jp_date_match.group(3))
                )

        if not tournament_date:
            year_match = re.search(r'(202[0-9])', url)
            if year_match:
                tournament_date = datetime(int(year_match.group(1)), 1, 1)

        # Generate ID
        url_slug = url.rstrip('/').split('/')[-1]
        post_id = f"{JP_SOURCE_PREFIX}{url_slug}"

        # Parse placements
        placements = self._parse_placements(soup)

        return Tournament(
            wbo_post_id=post_id,
            name=name,
            date=tournament_date,
            country="Japan",
            wbo_url=url,
            placements=placements,
        )

    def _parse_placements(self, soup: BeautifulSoup) -> list[Placement]:
        """Parse placements from soup."""
        tables = soup.find_all('table')
        if not tables:
            return self._parse_g1_format(soup)

        player_combos: dict[str, list[Combo]] = {}

        for table in tables:
            rows = table.find_all('tr')
            if not rows:
                continue

            header = rows[0]
            cells = header.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            player1_text = cells[0].get_text().strip()
            player2_text = cells[1].get_text().strip()

            player1_match = re.match(r'^(.+?)(?:使用ベイ|$)', player1_text, re.DOTALL)
            player2_match = re.match(r'^(.+?)(?:使用ベイ|$)', player2_text, re.DOTALL)

            if not player1_match or not player2_match:
                continue

            player1_name = re.sub(r'さん$', '', player1_match.group(1).strip())
            player2_name = re.sub(r'さん$', '', player2_match.group(1).strip())

            if not player1_name or not player2_name:
                continue
            if '勝ち方' in player1_text or 'ポイント' in player1_text:
                continue

            if player1_name not in player_combos:
                player_combos[player1_name] = []
            if player2_name not in player_combos:
                player_combos[player2_name] = []

            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue

                for cell, player_name in [(cells[0], player1_name), (cells[1], player2_name)]:
                    cell_text = re.sub(r'[_\*]', '', cell.get_text().strip())
                    combo_match = re.match(r'^(.+?)(\d{1,2}-\d{2,3})\s*([A-Za-z]*)$', cell_text.replace('\n', ''))
                    if combo_match:
                        blade = translate_blade(combo_match.group(1).strip())
                        ratchet = combo_match.group(2)
                        bit = expand_bit(combo_match.group(3) or "")
                        # Parse CX blade to extract lock chip
                        lock_chip, blade = parse_cx_blade(blade)

                        combo = Combo(blade=blade, ratchet=ratchet, bit=bit, lock_chip=lock_chip)
                        existing = [(c.blade, c.ratchet, c.bit) for c in player_combos[player_name]]
                        if (combo.blade, combo.ratchet, combo.bit) not in existing:
                            player_combos[player_name].append(combo)

        placements = []
        for i, (player_name, combos) in enumerate(player_combos.items(), start=1):
            if combos:
                placements.append(Placement(
                    place=i,
                    player_name=player_name,
                    player_wbo_id=None,
                    combos=combos[:3]
                ))

        return placements

    def _parse_g1_format(self, soup: BeautifulSoup) -> list[Placement]:
        """Parse G1 text format."""
        placements = []
        place_counter = 0

        content = soup.find('div', class_='entry-content') or soup.find('article') or soup
        if not content:
            return []

        text = content.get_text()
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        current_player = None
        current_combos = []
        current_region = None

        winner_pattern = re.compile(r'【優勝者[：:](.+?)(?:選手)?(?:の3on3デッキ)?】')
        runner_up_pattern = re.compile(r'【準優勝者[：:](.+?)(?:選手)?(?:の3on3デッキ)?】')
        region_pattern = re.compile(r'(大阪|仙台|福岡|広島|東京|札幌|名古屋|神戸)')

        for line in lines:
            region_match = region_pattern.search(line)
            if region_match and ('G1' in line or '予選' in line or '大会結果' in line):
                current_region = region_match.group(1)

            winner_match = winner_pattern.search(line)
            if winner_match:
                if current_player and current_combos:
                    place_counter += 1
                    player_name = f"{current_player} ({current_region})" if current_region else current_player
                    placements.append(Placement(
                        place=place_counter,
                        player_name=player_name,
                        player_wbo_id=None,
                        combos=current_combos[:3]
                    ))
                current_player = winner_match.group(1).strip()
                current_combos = []
                continue

            runner_match = runner_up_pattern.search(line)
            if runner_match:
                if current_player and current_combos:
                    place_counter += 1
                    player_name = f"{current_player} ({current_region})" if current_region else current_player
                    placements.append(Placement(
                        place=place_counter,
                        player_name=player_name,
                        player_wbo_id=None,
                        combos=current_combos[:3]
                    ))
                current_player = runner_match.group(1).strip()
                current_combos = []
                continue

            if current_player:
                combo = parse_jp_combo(line)
                if combo:
                    current_combos.append(combo)

        if current_player and current_combos:
            place_counter += 1
            player_name = f"{current_player} ({current_region})" if current_region else current_player
            placements.append(Placement(
                place=place_counter,
                player_name=player_name,
                player_wbo_id=None,
                combos=current_combos[:3]
            ))

        return placements
