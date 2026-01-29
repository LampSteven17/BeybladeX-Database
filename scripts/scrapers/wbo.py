"""
WBO Scraper - World Beyblade Organization forum data.

Reads downloaded HTML pages from data/wbo_pages.json and uses
the full parsing logic from scraper.py.
"""

import json
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from tqdm import tqdm

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from base_scraper import BaseScraper, Combo, Placement, Tournament
# Import parse_post from the existing scraper module for full parsing logic
from scraper import parse_post as wbo_parse_post


class WBOScraper(BaseScraper):
    """
    Scraper for WBO forum data.

    Reads pre-downloaded HTML pages from data/wbo_pages.json
    and uses the full parsing logic from scraper.py.
    """

    def __init__(self, data_path: Optional[Path] = None):
        """
        Initialize WBO scraper.

        Args:
            data_path: Path to wbo_pages.json (defaults to data/wbo_pages.json)
        """
        if data_path is None:
            data_path = Path(__file__).parent.parent.parent / "data" / "wbo_pages.json"
        self.data_path = data_path

    @property
    def source_name(self) -> str:
        return "WBO"

    @property
    def source_prefix(self) -> str:
        return ""  # WBO has no prefix

    @property
    def default_region(self) -> Optional[str]:
        return None  # Infer from country

    def clear_source_data(self, conn) -> int:
        """Clear WBO data (entries without okuyama_ or blg_ prefix)."""
        # Get count before deletion
        count = conn.execute("""
            SELECT COUNT(*) FROM tournaments
            WHERE wbo_post_id IS NOT NULL
            AND wbo_post_id NOT LIKE 'okuyama_%'
            AND wbo_post_id NOT LIKE 'blg_%'
        """).fetchone()[0]

        # Delete placements first (foreign key constraint)
        conn.execute("""
            DELETE FROM placements WHERE tournament_id IN (
                SELECT id FROM tournaments
                WHERE wbo_post_id IS NOT NULL
                AND wbo_post_id NOT LIKE 'okuyama_%'
                AND wbo_post_id NOT LIKE 'blg_%'
            )
        """)

        # Delete tournaments
        conn.execute("""
            DELETE FROM tournaments
            WHERE wbo_post_id IS NOT NULL
            AND wbo_post_id NOT LIKE 'okuyama_%'
            AND wbo_post_id NOT LIKE 'blg_%'
        """)

        return count

    def scrape(self, conn, verbose: bool = False) -> tuple[int, int]:
        """
        Scrape WBO data from wbo_pages.json.

        Args:
            conn: Database connection
            verbose: If True, print detailed progress

        Returns:
            Tuple of (added_count, skipped_count)
        """
        if not self.data_path.exists():
            print(f"ERROR: WBO data file not found at {self.data_path}")
            print()
            print("To download WBO pages:")
            print("1. Run wbo_downloader.py on Windows with browser cookies")
            print("2. Save output to data/wbo_pages.json")
            return 0, 0

        if verbose:
            print(f"Loading WBO data from {self.data_path}...")

        with open(self.data_path, 'r', encoding='utf-8') as f:
            pages_data = json.load(f)

        if verbose:
            print(f"Loaded {len(pages_data)} pages")

        # Get already processed post IDs
        processed_ids = self.get_processed_ids(conn)
        if verbose:
            print(f"Already processed {len(processed_ids)} WBO posts")

        tournaments_added = 0
        tournaments_skipped = 0

        # Sort pages by number for consistent processing
        page_numbers = sorted(pages_data.keys(), key=lambda x: int(x))

        iterator = tqdm(page_numbers, desc="WBO pages") if verbose else page_numbers

        for page_num in iterator:
            page_html = pages_data[page_num]

            try:
                soup = BeautifulSoup(page_html, 'lxml')
                posts = soup.find_all('div', class_='post')

                for post in posts:
                    post_id = post.get('id', '')

                    # Quick skip if already processed
                    if post_id in processed_ids:
                        tournaments_skipped += 1
                        continue

                    # Use the full parse_post logic from scraper.py
                    tournaments = wbo_parse_post(post)

                    for tournament in tournaments:
                        if tournament.wbo_post_id in processed_ids:
                            tournaments_skipped += 1
                            continue

                        # Convert from scraper.py Tournament to base_scraper Tournament
                        converted = self._convert_tournament(tournament)

                        result = self.insert_tournament(conn, converted)
                        if result:
                            tournaments_added += 1
                            processed_ids.add(tournament.wbo_post_id)
                            if verbose:
                                tqdm.write(f"  Added: {tournament.name}")
                        else:
                            tournaments_skipped += 1

                conn.commit()

            except Exception as e:
                print(f"Error on page {page_num}: {e}")
                continue

        return tournaments_added, tournaments_skipped

    def _convert_tournament(self, src_tournament) -> Tournament:
        """Convert a scraper.py Tournament to a base_scraper Tournament."""
        placements = []
        for src_placement in src_tournament.placements:
            combos = []
            for src_combo in src_placement.combos:
                combos.append(Combo(
                    blade=src_combo.blade,
                    ratchet=src_combo.ratchet,
                    bit=src_combo.bit,
                    assist=src_combo.assist,
                    lock_chip=src_combo.lock_chip,
                    stage=src_combo.stage,
                ))
            placements.append(Placement(
                place=src_placement.place,
                player_name=src_placement.player_name,
                player_wbo_id=src_placement.player_wbo_id,
                combos=combos,
            ))

        return Tournament(
            wbo_post_id=src_tournament.wbo_post_id,
            name=src_tournament.name,
            date=src_tournament.date,
            city=src_tournament.city,
            state=src_tournament.state,
            country=src_tournament.country,
            format=src_tournament.format,
            ranked=src_tournament.ranked,
            wbo_url=src_tournament.wbo_url,
            placements=placements,
        )
