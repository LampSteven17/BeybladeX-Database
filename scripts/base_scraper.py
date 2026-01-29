"""
Base Scraper - Abstract base class and shared types for all data sources.

Provides:
- Shared dataclasses: Combo, Placement, Tournament
- Abstract BaseScraper class with common functionality
- Database integration helpers
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from db import get_connection, init_schema, normalize_data, infer_region, infer_region_from_tournament, parse_cx_blade


# =============================================================================
# Shared Data Classes
# =============================================================================

@dataclass
class Combo:
    """A single Beyblade combo (blade + ratchet + bit)."""
    blade: str
    ratchet: str
    bit: str
    assist: Optional[str] = None
    lock_chip: Optional[str] = None
    stage: Optional[str] = None  # 'first', 'final', 'both', or None


@dataclass
class Placement:
    """A player's placement in a tournament with their combos."""
    place: int
    player_name: str
    player_wbo_id: Optional[str]
    combos: list[Combo] = field(default_factory=list)


@dataclass
class Tournament:
    """A tournament with its placements."""
    wbo_post_id: str  # Unique ID with source prefix (e.g., "okuyama_xxx", "blg_xxx")
    name: str
    date: Optional[datetime]
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    format: Optional[str] = None
    ranked: Optional[bool] = None
    wbo_url: Optional[str] = None
    placements: list[Placement] = field(default_factory=list)


# =============================================================================
# Abstract Base Scraper
# =============================================================================

class BaseScraper(ABC):
    """
    Abstract base class for all data source scrapers.

    Subclasses must implement:
    - source_name: Human-readable name (e.g., "WBO", "Japan", "Germany")
    - source_prefix: ID prefix for deduplication (e.g., "", "okuyama_", "blg_")
    - default_region: Default region code (e.g., "NA", "JAPAN", "EU")
    - scrape(): Main scraping method
    - clear_source_data(): Clear this source's data from database
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable name for this data source."""
        pass

    @property
    @abstractmethod
    def source_prefix(self) -> str:
        """Prefix for wbo_post_id to identify this source's data."""
        pass

    @property
    @abstractmethod
    def default_region(self) -> Optional[str]:
        """Default region code for this source (None to infer from country)."""
        pass

    @abstractmethod
    def scrape(self, conn, verbose: bool = False) -> tuple[int, int]:
        """
        Scrape data from this source and insert into database.

        Args:
            conn: Database connection
            verbose: If True, print detailed progress

        Returns:
            Tuple of (added_count, skipped_count)
        """
        pass

    @abstractmethod
    def clear_source_data(self, conn) -> int:
        """
        Clear all data from this source in the database.

        Args:
            conn: Database connection

        Returns:
            Number of tournaments deleted
        """
        pass

    def get_processed_ids(self, conn) -> set[str]:
        """Get all wbo_post_ids for this source that are already in the database."""
        if self.source_prefix:
            result = conn.execute(
                "SELECT wbo_post_id FROM tournaments WHERE wbo_post_id LIKE ?",
                [f"{self.source_prefix}%"]
            ).fetchall()
        else:
            # WBO has no prefix - get IDs that don't match any other source prefix
            result = conn.execute("""
                SELECT wbo_post_id FROM tournaments
                WHERE wbo_post_id IS NOT NULL
                AND wbo_post_id NOT LIKE 'okuyama_%'
                AND wbo_post_id NOT LIKE 'blg_%'
            """).fetchall()
        return {row[0] for row in result}

    def insert_tournament(self, conn, tournament: Tournament) -> Optional[int]:
        """
        Insert a tournament and its placements into the database.

        Args:
            conn: Database connection
            tournament: Tournament to insert

        Returns:
            Tournament ID if inserted, None if skipped
        """
        if not tournament.date:
            return None

        if not tournament.placements:
            return None

        # Check if already processed
        existing = conn.execute(
            "SELECT id FROM tournaments WHERE wbo_post_id = ?",
            [tournament.wbo_post_id]
        ).fetchone()

        if existing:
            return None  # Skip, already processed

        # Determine region - check multiple fields for location hints
        region = self.default_region
        if region is None:
            region = infer_region_from_tournament(
                tournament.name,
                tournament.city,
                tournament.state,
                tournament.country
            )

        # For WBO (no prefix), default to NA if region couldn't be inferred
        # since WBO is predominantly North American
        if region is None and not self.source_prefix:
            region = "NA"

        # Insert tournament
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
            region,
            tournament.format,
            tournament.ranked,
            tournament.wbo_url
        ])

        tournament_id = result.fetchone()[0]

        # Insert placements
        for placement in tournament.placements:
            if not placement.combos:
                continue

            combos = placement.combos[:3]  # Max 3 combos

            try:
                conn.execute("""
                    INSERT INTO placements (
                        tournament_id, place, player_name, player_wbo_id,
                        blade_1, ratchet_1, bit_1, assist_1, lock_chip_1, stage_1,
                        blade_2, ratchet_2, bit_2, assist_2, lock_chip_2, stage_2,
                        blade_3, ratchet_3, bit_3, assist_3, lock_chip_3, stage_3
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ])
            except Exception as e:
                print(f"Error inserting placement for {placement.player_name}: {e}")

        return tournament_id

    def get_stats(self, conn) -> dict:
        """Get statistics for this source's data."""
        if self.source_prefix:
            prefix_pattern = f"{self.source_prefix}%"
            tournaments = conn.execute(
                "SELECT COUNT(*) FROM tournaments WHERE wbo_post_id LIKE ?",
                [prefix_pattern]
            ).fetchone()[0]

            placements = conn.execute("""
                SELECT COUNT(*) FROM placements p
                JOIN tournaments t ON p.tournament_id = t.id
                WHERE t.wbo_post_id LIKE ?
            """, [prefix_pattern]).fetchone()[0]
        else:
            # WBO (no prefix)
            tournaments = conn.execute("""
                SELECT COUNT(*) FROM tournaments
                WHERE wbo_post_id IS NOT NULL
                AND wbo_post_id NOT LIKE 'okuyama_%'
                AND wbo_post_id NOT LIKE 'blg_%'
            """).fetchone()[0]

            placements = conn.execute("""
                SELECT COUNT(*) FROM placements p
                JOIN tournaments t ON p.tournament_id = t.id
                WHERE t.wbo_post_id IS NOT NULL
                AND t.wbo_post_id NOT LIKE 'okuyama_%'
                AND t.wbo_post_id NOT LIKE 'blg_%'
            """).fetchone()[0]

        return {
            "source": self.source_name,
            "tournaments": tournaments,
            "placements": placements,
        }
