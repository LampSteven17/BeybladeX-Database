"""
Database connection and schema management for BeybladeX Database.

Single source of truth: site/public/data/beyblade.duckdb
(Used by both scrapers and the website)
"""

import duckdb
import fcntl
import os
from contextlib import contextmanager
from pathlib import Path

# Database path - single source of truth, used directly by the website
DB_PATH = Path(__file__).parent.parent / "site" / "public" / "data" / "beyblade.duckdb"

# Lock file for coordinating concurrent access
LOCK_PATH = DB_PATH.parent / ".beyblade.lock"


class DatabaseLockError(Exception):
    """Raised when the database lock cannot be acquired."""
    pass


@contextmanager
def database_lock(timeout: float = 0):
    """Context manager for acquiring an exclusive lock on the database.

    This prevents concurrent write operations from multiple processes
    (e.g., cron job and API-triggered scrapes running simultaneously).

    Args:
        timeout: How long to wait for the lock (0 = non-blocking, fail immediately)

    Raises:
        DatabaseLockError: If the lock cannot be acquired

    Usage:
        with database_lock():
            conn = get_connection()
            # ... do database operations ...
            conn.close()
    """
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_RDWR)

    try:
        # Try to acquire exclusive lock (non-blocking)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            os.close(lock_fd)
            raise DatabaseLockError(
                "Could not acquire database lock. Another scrape may be in progress. "
                "Wait for it to complete or check for stale lock files."
            )

        yield

    finally:
        # Release lock and close file descriptor
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except (IOError, OSError):
            pass
        os.close(lock_fd)


def is_database_locked() -> bool:
    """Check if the database is currently locked by another process.

    Returns:
        True if locked, False if available
    """
    if not LOCK_PATH.exists():
        return False

    try:
        lock_fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # If we got here, lock is available - release it
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            return False
        except (IOError, OSError):
            return True
        finally:
            os.close(lock_fd)
    except (IOError, OSError):
        return False


# =============================================================================
# CX Blade Components - Lock Chip + Main Blade parsing
# =============================================================================

# CX blades are composed of: Lock Chip + Main Blade (+ Assist Blade)
# Format: "[Lock Chip] [Main Blade]" e.g., "Pegasus Blast" = Pegasus lock chip + Blast main blade
# This dict maps full CX blade names to (lock_chip, main_blade) tuples
CX_BLADE_COMPONENTS: dict[str, tuple[str, str]] = {
    # CX-01 and variants using Brave main blade
    "Dran Brave": ("Dran", "Brave"),
    "Emperor Brave": ("Emperor", "Brave"),
    # CX-02 and variants using Arc main blade
    "Wizard Arc": ("Wizard", "Arc"),
    # CX-03 and variants using Dark main blade
    "Perseus Dark": ("Perseus", "Dark"),
    # CX-05 and variants using Reaper main blade
    "Hells Reaper": ("Hells", "Reaper"),
    # CX-06 and variants using Brush main blade
    "Fox Brush": ("Fox", "Brush"),
    # CX-07 and variants using Blast main blade
    "Pegasus Blast": ("Pegasus", "Blast"),
    "Cerberus Blast": ("Cerberus", "Blast"),
    "Hells Blast": ("Hells", "Blast"),
    "Emperor Blast": ("Emperor", "Blast"),
    "Wolf Blast": ("Wolf", "Blast"),
    "Dran Blast": ("Dran", "Blast"),
    "Valkyrie Blast": ("Valkyrie", "Blast"),
    "Valkyrie Blast W": ("Valkyrie", "Blast"),
    "Valkyrie Blast S": ("Valkyrie", "Blast"),
    "Sol Blast": ("Sol", "Blast"),
    "Perseus Blast W": ("Perseus", "Blast"),
    "Kraken Blast": ("Kraken", "Blast"),
    # CX-09 and variants using Eclipse main blade
    "Sol Eclipse": ("Sol", "Eclipse"),
    "Emperor Eclipse": ("Emperor", "Eclipse"),
    "Hells Eclipse": ("Hells", "Eclipse"),
    # CX-10 and variants using Hunt main blade
    "Wolf Hunt": ("Wolf", "Hunt"),
    "Emperor Hunt": ("Emperor", "Hunt"),
    "Perseus Hunt": ("Perseus", "Hunt"),
    # CX-11 and variants using Might main blade
    "Emperor Might": ("Emperor", "Might"),
    "Cerberus Might": ("Cerberus", "Might"),
    "Dran Might": ("Dran", "Might"),
    "Whale Might": ("Whale", "Might"),
    # CX-12 and variants using Flare main blade
    "Phoenix Flare": ("Phoenix", "Flare"),
    "Pegasus Flame": ("Pegasus", "Flare"),  # Flame is likely typo for Flare
    # Random Booster CX blades
    "Valkyrie Volt": ("Valkyrie", "Volt"),
    "Valkyrie Volt A": ("Valkyrie", "Volt"),
    # NOTE: Dragoon Storm and Driger Slash are BX blades (classic remakes), NOT CX
}


# CX main blade names that REQUIRE a lock chip prefix
# If we see just "Blast" without "Pegasus Blast", that's invalid/incomplete data
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

# Known CX lock chips - used for fuzzy matching when parsing blade names
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

# Lowercase versions for case-insensitive matching
_CX_MAIN_BLADES_LOWER = {b.lower(): b for b in CX_MAIN_BLADES}
_CX_LOCK_CHIPS_LOWER = {c.lower(): c for c in CX_LOCK_CHIPS}


def parse_cx_blade(blade_name: str) -> tuple[str | None, str]:
    """
    Parse a CX blade name into (lock_chip, main_blade).
    Returns (None, blade_name) if not a known CX blade.

    Handles various formats:
    - Exact match: "Pegasus Blast" -> ("Pegasus", "Blast")
    - Reversed order: "Blast Pegasus" -> ("Pegasus", "Blast")
    - Concatenated: "PegasusBlast" -> ("Pegasus", "Blast")
    - With suffixes: "Valkyrie Blast W" -> ("Valkyrie", "Blast")
    - Case insensitive: "pegasus blast" -> ("Pegasus", "Blast")

    If blade_name is a bare CX main blade (e.g., "Blast" without lock chip),
    this indicates incomplete data - returns (None, blade_name) but callers
    should check if blade_name is in CX_MAIN_BLADES to detect this case.
    """
    # 1. Check exact match first (fastest path)
    if blade_name in CX_BLADE_COMPONENTS:
        return CX_BLADE_COMPONENTS[blade_name]

    # 2. Normalize: strip whitespace, remove common suffixes
    normalized = blade_name.strip()

    # Remove trailing single-letter suffixes (W, S, A, J, etc.) used for variants
    # e.g., "Valkyrie Blast W" -> "Valkyrie Blast", "Sol Blast J" -> "Sol Blast"
    if len(normalized) > 2 and normalized[-2] == " " and normalized[-1] in "WSAFHTJ":
        suffix_stripped = normalized[:-2]
        if suffix_stripped in CX_BLADE_COMPONENTS:
            return CX_BLADE_COMPONENTS[suffix_stripped]
        normalized = suffix_stripped

    # 3. Try case-insensitive exact match
    normalized_lower = normalized.lower()
    for full_name, components in CX_BLADE_COMPONENTS.items():
        if full_name.lower() == normalized_lower:
            return components

    # 4. Split by space and try to identify lock chip + main blade
    parts = normalized.split()
    if len(parts) >= 2:
        # Try normal order: "Pegasus Blast"
        first_lower = parts[0].lower()
        second_lower = parts[1].lower()

        if (
            first_lower in _CX_LOCK_CHIPS_LOWER
            and second_lower in _CX_MAIN_BLADES_LOWER
        ):
            return (
                _CX_LOCK_CHIPS_LOWER[first_lower],
                _CX_MAIN_BLADES_LOWER[second_lower],
            )

        # Try reversed order: "Blast Pegasus"
        if (
            first_lower in _CX_MAIN_BLADES_LOWER
            and second_lower in _CX_LOCK_CHIPS_LOWER
        ):
            return (
                _CX_LOCK_CHIPS_LOWER[second_lower],
                _CX_MAIN_BLADES_LOWER[first_lower],
            )

    # 5. Try to find concatenated patterns (no space): "PegasusBlast"
    # Check if blade_name contains both a lock chip and main blade concatenated
    name_lower = normalized_lower.replace(" ", "")  # Remove any spaces

    for lock_chip_lower, lock_chip in _CX_LOCK_CHIPS_LOWER.items():
        for main_blade_lower, main_blade in _CX_MAIN_BLADES_LOWER.items():
            # Check "LockChipMainBlade" pattern
            concat_pattern = lock_chip_lower + main_blade_lower
            if name_lower == concat_pattern or name_lower.startswith(concat_pattern):
                return (lock_chip, main_blade)
            # Check "MainBladeLockChip" pattern (reversed)
            concat_pattern_rev = main_blade_lower + lock_chip_lower
            if name_lower == concat_pattern_rev or name_lower.startswith(
                concat_pattern_rev
            ):
                return (lock_chip, main_blade)

    # 6. No match found
    return (None, blade_name)


def is_incomplete_cx_blade(blade_name: str, lock_chip: str | None) -> bool:
    """
    Check if a blade is a CX main blade missing its lock chip.
    This indicates incomplete/invalid data that should be flagged.
    """
    return blade_name in CX_MAIN_BLADES and lock_chip is None


def is_invalid_two_main_blades(blade_name: str) -> bool:
    """
    Check if a blade name is an invalid combination of two CX main blades.
    e.g., "Might Blast" is invalid because both are CX main blades.

    Returns True if the blade name contains two CX main blade names,
    which is an invalid/impossible combination.
    """
    parts = blade_name.split()
    if len(parts) != 2:
        return False

    # Check if both parts are CX main blades
    first_lower = parts[0].lower()
    second_lower = parts[1].lower()

    first_is_main = first_lower in _CX_MAIN_BLADES_LOWER
    second_is_main = second_lower in _CX_MAIN_BLADES_LOWER

    # Invalid if BOTH parts are main blades (not lock chip + main blade)
    return first_is_main and second_is_main


def validate_and_fix_blade(blade_name: str) -> str:
    """
    Validate a blade name and return a corrected version if needed.

    Checks for:
    1. Invalid two-main-blade combinations (e.g., "Might Blast")
    2. Known typos in BLADE_NORMALIZATIONS

    Returns the corrected blade name, or original if valid.
    """
    # Import here to avoid circular dependency
    from db import BLADE_NORMALIZATIONS

    # First check normalizations (includes invalid combo fixes)
    if blade_name in BLADE_NORMALIZATIONS:
        return BLADE_NORMALIZATIONS[blade_name]

    # Check for invalid two-main-blade combinations not in normalizations
    if is_invalid_two_main_blades(blade_name):
        parts = blade_name.split()
        first_lower = parts[0].lower()
        # Return just the first main blade with a guess at the lock chip
        # Default to common lock chips for each main blade
        main_blade = _CX_MAIN_BLADES_LOWER.get(first_lower, parts[0])
        default_lock_chips = {
            "Blast": "Pegasus",
            "Might": "Emperor",
            "Brave": "Dran",
            "Arc": "Wizard",
            "Reaper": "Hells",
            "Brush": "Fox",
            "Eclipse": "Sol",
            "Hunt": "Wolf",
            "Flare": "Phoenix",
            "Volt": "Valkyrie",
        }
        lock_chip = default_lock_chips.get(main_blade, "")
        if lock_chip:
            return f"{lock_chip} {main_blade}"
        return main_blade

    return blade_name


# =============================================================================
# Blade Series Classification
# =============================================================================

# Series definitions based on product codes:
# - BX = Basic Line (standard BX-XX releases)
# - UX = Unique Line (metal distributed to perimeter, UX-XX releases)
# - CX = Custom Line (disassembles into Lock Chip + Main Blade + Assist Blade)
#
# Sources: beyblade.fandom.com, beybxdb.com, worldbeyblade.org
BLADE_SERIES = {
    # ==========================================================================
    # BX Series (Basic Line) - Standard releases
    # ==========================================================================
    "Dran Sword": "BX",  # BX-01
    "Hells Scythe": "BX",  # BX-02
    "Wizard Arrow": "BX",  # BX-03
    "Knight Shield": "BX",  # BX-04
    "Knight Lance": "BX",  # BX-13
    "Leon Claw": "BX",  # BX-15
    "Shark Edge": "BX",  # BX-14 Random Booster
    "Viper Tail": "BX",  # BX-14 Random Booster
    "Dran Dagger": "BX",  # BX-14 Random Booster
    "Rhino Horn": "BX",  # BX-19
    "Phoenix Wing": "BX",  # BX-23
    "Hells Chain": "BX",  # BX-24 Random Booster
    "Unicorn Sting": "BX",  # BX-26
    "Black Shell": "BX",  # BX-24 Random Booster
    "Tyranno Beat": "BX",  # BX-24 Random Booster
    "Weiss Tiger": "BX",  # BX-33
    "Cobalt Dragoon": "BX",  # BX-34
    "Cobalt Drake": "BX",  # BX-31 Random Booster
    "Crimson Garuda": "BX",  # BX-38
    "Talon Ptera": "BX",  # BX-35 Random Booster
    "Roar Tyranno": "BX",  # BX-35 Random Booster
    "Sphinx Cowl": "BX",  # BX-35 Random Booster
    "Wyvern Gale": "BX",  # BX-35 Random Booster
    "Shelter Drake": "BX",  # BX-39
    "Tricera Press": "BX",  # BX-44
    "Samurai Calibur": "BX",  # BX-45
    "Bear Scratch": "BX",  # BX-48 Random Booster
    "Xeno Xcalibur": "BX",  # BXG-13
    "Chain Incendio": "BX",  # BX Random Booster
    "Scythe Incendio": "BX",  # BX Random Booster
    "Steel Samurai": "BX",  # BX
    "Optimus Primal": "BX",  # BX (Collab)
    "Bite Croc": "BX",  # BX (Hasbro exclusive)
    "Knife Shinobi": "BX",  # BX (Hasbro exclusive)
    "Venom": "BX",  # BX
    "Keel Shark": "BX",  # BX (Hasbro name for Shark Edge)
    "Whale Wave": "BX",  # BX
    "Gill Shark": "BX",  # BX (in CX-11 deck set but blade is BX)
    "Driger Slash": "BX",  # BX remake of classic Driger
    "Dragoon Storm": "BX",  # BX remake of classic Dragoon
    # ==========================================================================
    # UX Series (Unique Line) - More metal to perimeter, plastic interior hooks
    # ==========================================================================
    "Dran Buster": "UX",  # UX-01
    "Hells Hammer": "UX",  # UX-02
    "Wizard Rod": "UX",  # UX-03
    # "Soar Phoenix" removed - same blade as "Phoenix Wing" (UX-04 Entry Set)
    "Leon Crest": "UX",  # UX-06
    "Knight Mail": "UX",  # UX-07
    "Silver Wolf": "UX",  # UX-08
    "Samurai Saber": "UX",  # UX-09
    "Phoenix Feather": "UX",  # UX-10
    "Impact Drake": "UX",  # UX-11
    "Tusk Mammoth": "UX",  # UX-12 Random Booster
    "Phoenix Rudder": "UX",  # UX-12 Random Booster
    "Ghost Circle": "UX",  # UX-12 Random Booster
    "Golem Rock": "UX",  # UX-13
    "Scorpio Spear": "UX",  # UX-14
    "Shinobi Shadow": "UX",  # UX-15 Random Booster
    "Clock Mirage": "UX",  # UX-16
    "Meteor Dragoon": "UX",  # UX-17
    "Mummy Curse": "UX",  # UX-18 Random Booster
    "Dranzer Spiral": "UX",  # UX-12 Random Booster
    "Shark Scale": "UX",  # UX-15 Shark Scale Deck Set
    "Hover Wyvern": "UX",  # UX
    "Aero Pegasus": "UX",  # UX
    "Wand Wizard": "UX",  # UX Starter Pack
    # ==========================================================================
    # CX Series (Custom Line) - Main Blade names (lock chip stored separately)
    # After parsing: "Pegasus Blast" -> lock_chip="Pegasus", blade="Blast"
    # ==========================================================================
    # Main blade types (what gets stored in blade column after parsing)
    "Brave": "CX",  # CX-01 main blade
    "Arc": "CX",  # CX-02 main blade
    "Dark": "CX",  # CX-03 main blade
    "Reaper": "CX",  # CX-05 main blade
    "Brush": "CX",  # CX-06 main blade
    "Blast": "CX",  # CX-07 main blade
    "Eclipse": "CX",  # CX-09 main blade
    "Hunt": "CX",  # CX-10 main blade
    "Might": "CX",  # CX-11 main blade
    "Flare": "CX",  # CX-12 main blade
    "Volt": "CX",  # CX Random Booster main blade
    "Storm": "CX",  # CX Random Booster main blade
    "Emperor": "CX",  # CX main blade
    # Also keep full names for backwards compatibility with existing data
    "Dran Brave": "CX",
    "Wizard Arc": "CX",
    "Perseus Dark": "CX",
    "Hells Reaper": "CX",
    "Fox Brush": "CX",
    "Pegasus Blast": "CX",
    "Sol Eclipse": "CX",
    "Wolf Hunt": "CX",
    "Emperor Might": "CX",
    "Phoenix Flare": "CX",
    "Valkyrie Volt": "CX",
    "Emperor Brave": "CX",
    "Cerberus Blast": "CX",
    "Hells Blast": "CX",
}


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a connection to the database.

    Args:
        read_only: If True, open in read-only mode (allows concurrent reads).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def init_schema(conn: duckdb.DuckDBPyConnection = None) -> None:
    """Initialize the database schema."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    # Create sequences for auto-increment
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_parts START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_tournaments START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_placements START 1")

    # Parts reference table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS parts (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_parts'),
            name VARCHAR NOT NULL UNIQUE,
            type VARCHAR NOT NULL,
            spin_direction VARCHAR,
            series VARCHAR,
            release_date DATE,
            notes VARCHAR
        )
    """)

    # Tournaments table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tournaments (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_tournaments'),
            wbo_post_id VARCHAR UNIQUE,
            name VARCHAR NOT NULL,
            date DATE NOT NULL,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            region VARCHAR,
            format VARCHAR,
            ranked BOOLEAN,
            participant_count INTEGER,
            wbo_thread_url VARCHAR,
            challonge_url VARCHAR,
            scraped_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # Placements table (top 3 from each tournament)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS placements (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_placements'),
            tournament_id INTEGER REFERENCES tournaments(id),
            place INTEGER NOT NULL,
            player_name VARCHAR NOT NULL,
            player_wbo_id VARCHAR,
            blade_1 VARCHAR NOT NULL,
            ratchet_1 VARCHAR NOT NULL,
            bit_1 VARCHAR NOT NULL,
            assist_1 VARCHAR,
            lock_chip_1 VARCHAR,
            stage_1 VARCHAR,
            blade_2 VARCHAR,
            ratchet_2 VARCHAR,
            bit_2 VARCHAR,
            assist_2 VARCHAR,
            lock_chip_2 VARCHAR,
            stage_2 VARCHAR,
            blade_3 VARCHAR,
            ratchet_3 VARCHAR,
            bit_3 VARCHAR,
            assist_3 VARCHAR,
            lock_chip_3 VARCHAR,
            stage_3 VARCHAR,
            UNIQUE(tournament_id, place)
        )
    """)

    # View: Flatten all combos (includes stage for weighted scoring)
    conn.execute("""
        CREATE OR REPLACE VIEW combo_usage AS
        SELECT
            p.tournament_id,
            t.date as tournament_date,
            t.region,
            p.place,
            p.player_name,
            p.blade_1 as blade,
            p.ratchet_1 as ratchet,
            p.bit_1 as bit,
            p.assist_1 as assist,
            p.lock_chip_1 as lock_chip,
            p.stage_1 as stage
        FROM placements p
        JOIN tournaments t ON p.tournament_id = t.id
        UNION ALL
        SELECT
            p.tournament_id,
            t.date,
            t.region,
            p.place,
            p.player_name,
            p.blade_2,
            p.ratchet_2,
            p.bit_2,
            p.assist_2,
            p.lock_chip_2,
            p.stage_2
        FROM placements p
        JOIN tournaments t ON p.tournament_id = t.id
        WHERE p.blade_2 IS NOT NULL
        UNION ALL
        SELECT
            p.tournament_id,
            t.date,
            t.region,
            p.place,
            p.player_name,
            p.blade_3,
            p.ratchet_3,
            p.bit_3,
            p.assist_3,
            p.lock_chip_3,
            p.stage_3
        FROM placements p
        JOIN tournaments t ON p.tournament_id = t.id
        WHERE p.blade_3 IS NOT NULL
    """)

    # View: Part statistics
    conn.execute("""
        CREATE OR REPLACE VIEW part_stats AS
        SELECT
            part_name,
            part_type,
            COUNT(*) as total_placements,
            SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as first_place_count,
            SUM(CASE WHEN place = 2 THEN 1 ELSE 0 END) as second_place_count,
            SUM(CASE WHEN place = 3 THEN 1 ELSE 0 END) as third_place_count,
            ROUND(SUM(CASE WHEN place = 1 THEN 1.0 ELSE 0 END) / COUNT(*), 3) as win_rate
        FROM (
            SELECT place, blade as part_name, 'blade' as part_type FROM combo_usage
            UNION ALL
            SELECT place, ratchet, 'ratchet' FROM combo_usage
            UNION ALL
            SELECT place, bit, 'bit' FROM combo_usage
            UNION ALL
            SELECT place, assist, 'assist' FROM combo_usage WHERE assist IS NOT NULL
            UNION ALL
            SELECT place, lock_chip, 'lock_chip' FROM combo_usage WHERE lock_chip IS NOT NULL
        ) parts
        GROUP BY part_name, part_type
        ORDER BY total_placements DESC
    """)

    # View: Full combo statistics (includes lock_chip for CX blades)
    conn.execute("""
        CREATE OR REPLACE VIEW combo_stats AS
        SELECT
            CASE
                WHEN lock_chip IS NOT NULL THEN lock_chip || ' ' || blade || ' ' || ratchet || bit
                ELSE blade || ' ' || ratchet || bit
            END as combo,
            blade,
            ratchet,
            bit,
            lock_chip,
            COUNT(*) as total_placements,
            SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as first_place_count,
            ROUND(SUM(CASE WHEN place = 1 THEN 1.0 ELSE 0 END) / COUNT(*), 3) as win_rate
        FROM combo_usage
        GROUP BY blade, ratchet, bit, lock_chip
        ORDER BY total_placements DESC
    """)

    if should_close:
        conn.close()


# =============================================================================
# Region Mapping
# =============================================================================
# Maps country names to standardized region codes

REGION_MAPPING = {
    # North America
    "usa": "NA",
    "us": "NA",
    "united states": "NA",
    "canada": "NA",
    "mexico": "NA",
    # Europe
    "uk": "EU",
    "united kingdom": "EU",
    "england": "EU",
    "france": "EU",
    "germany": "EU",
    "spain": "EU",
    "italy": "EU",
    "netherlands": "EU",
    "belgium": "EU",
    "poland": "EU",
    "sweden": "EU",
    "norway": "EU",
    "finland": "EU",
    "denmark": "EU",
    "ireland": "EU",
    "portugal": "EU",
    "austria": "EU",
    "switzerland": "EU",
    "scotland": "EU",
    "wales": "EU",
    "greece": "EU",
    "czech": "EU",
    "hungary": "EU",
    "romania": "EU",
    # Japan (separate from general Asia)
    "japan": "JAPAN",
    "日本": "JAPAN",
    # Asia (excluding Japan)
    "korea": "ASIA",
    "south korea": "ASIA",
    "china": "ASIA",
    "taiwan": "ASIA",
    "hong kong": "ASIA",
    "singapore": "ASIA",
    "malaysia": "ASIA",
    "philippines": "ASIA",
    "indonesia": "ASIA",
    "thailand": "ASIA",
    "vietnam": "ASIA",
    "india": "ASIA",
    # Oceania
    "australia": "OCEANIA",
    "new zealand": "OCEANIA",
    # South America
    "brazil": "SA",
    "argentina": "SA",
    "chile": "SA",
    "colombia": "SA",
    "peru": "SA",
}

# US States (full names and abbreviations)
US_STATES = {
    "alabama", "al", "alaska", "ak", "arizona", "az", "arkansas", "ar",
    "california", "ca", "colorado", "co", "connecticut", "ct", "delaware", "de",
    "florida", "fl", "georgia", "ga", "hawaii", "hi", "idaho", "id",
    "illinois", "il", "indiana", "in", "iowa", "ia", "kansas", "ks",
    "kentucky", "ky", "louisiana", "la", "maine", "me", "maryland", "md",
    "massachusetts", "ma", "michigan", "mi", "minnesota", "mn", "mississippi", "ms",
    "missouri", "mo", "montana", "mt", "nebraska", "ne", "nevada", "nv",
    "new hampshire", "nh", "new jersey", "nj", "new mexico", "nm", "new york", "ny",
    "north carolina", "nc", "north dakota", "nd", "ohio", "oh", "oklahoma", "ok",
    "oregon", "or", "pennsylvania", "pa", "rhode island", "ri", "south carolina", "sc",
    "south dakota", "sd", "tennessee", "tn", "texas", "tx", "utah", "ut",
    "vermont", "vt", "virginia", "va", "washington", "wa", "west virginia", "wv",
    "wisconsin", "wi", "wyoming", "wy", "district of columbia", "dc",
}

# Canadian Provinces
CANADIAN_PROVINCES = {
    "ontario", "on", "quebec", "qc", "british columbia", "bc", "alberta", "ab",
    "manitoba", "mb", "saskatchewan", "sk", "nova scotia", "ns", "new brunswick", "nb",
    "newfoundland", "nl", "prince edward island", "pei", "pe",
    "northwest territories", "nt", "nunavut", "nu", "yukon", "yt",
}

# Major cities that clearly indicate region
CITY_REGIONS = {
    # US cities
    "los angeles": "NA", "new york": "NA", "chicago": "NA", "houston": "NA",
    "phoenix": "NA", "philadelphia": "NA", "san antonio": "NA", "san diego": "NA",
    "dallas": "NA", "san jose": "NA", "austin": "NA", "jacksonville": "NA",
    "san francisco": "NA", "seattle": "NA", "denver": "NA", "boston": "NA",
    "las vegas": "NA", "portland": "NA", "detroit": "NA", "atlanta": "NA",
    "miami": "NA", "orlando": "NA", "tampa": "NA", "sacramento": "NA",
    "tempe": "NA", "tucson": "NA", "mesa": "NA", "fresno": "NA",
    # Canadian cities
    "toronto": "NA", "vancouver": "NA", "montreal": "NA", "calgary": "NA",
    "edmonton": "NA", "ottawa": "NA", "winnipeg": "NA",
    # European cities
    "london": "EU", "paris": "EU", "berlin": "EU", "madrid": "EU",
    "rome": "EU", "amsterdam": "EU", "brussels": "EU", "vienna": "EU",
    "munich": "EU", "barcelona": "EU", "milan": "EU", "manchester": "EU",
    # Asian cities
    "tokyo": "JAPAN", "osaka": "JAPAN", "kyoto": "JAPAN", "nagoya": "JAPAN",
    "seoul": "ASIA", "beijing": "ASIA", "shanghai": "ASIA", "hong kong": "ASIA",
    "singapore": "ASIA", "taipei": "ASIA", "bangkok": "ASIA",
    # Oceania
    "sydney": "OCEANIA", "melbourne": "OCEANIA", "brisbane": "OCEANIA",
    "auckland": "OCEANIA", "wellington": "OCEANIA",
}


def infer_region(text: str | None) -> str | None:
    """
    Infer region from location text (country, state, city, or tournament name).

    Checks in order:
    1. Country names
    2. US state names/abbreviations
    3. Canadian province names/abbreviations
    4. Major city names
    """
    if not text:
        return None

    text_lower = text.lower().strip()

    # Direct country match
    if text_lower in REGION_MAPPING:
        return REGION_MAPPING[text_lower]

    # Check if text contains a US state
    for state in US_STATES:
        # Match whole word to avoid false positives (e.g., "in" in "winning")
        if state in text_lower.split() or text_lower == state:
            return "NA"
        # Also check for state at word boundaries
        import re
        if re.search(rf'\b{re.escape(state)}\b', text_lower):
            return "NA"

    # Check if text contains a Canadian province
    for province in CANADIAN_PROVINCES:
        import re
        if re.search(rf'\b{re.escape(province)}\b', text_lower):
            return "NA"

    # Check for city names
    for city, region in CITY_REGIONS.items():
        if city in text_lower:
            return region

    # Check for country names anywhere in text
    for country, region in REGION_MAPPING.items():
        if len(country) > 2 and country in text_lower:  # Skip 2-letter codes to avoid false positives
            return region

    return None


def infer_region_from_tournament(name: str | None, city: str | None,
                                  state: str | None, country: str | None) -> str | None:
    """
    Infer region from multiple tournament fields.
    Checks country, state, city, then tournament name.
    """
    # Try each field in order of reliability
    for field in [country, state, city, name]:
        if field:
            region = infer_region(field)
            if region:
                return region
    return None


# =============================================================================
# Data Normalization - Fix common typos and inconsistencies
# =============================================================================

# Mapping of known typos/variants to canonical names
BLADE_NORMALIZATIONS = {
    # Typos - Aero Pegasus
    "Aero Pegaus": "Aero Pegasus",
    "Aero Pegesus": "Aero Pegasus",
    "Aerp Pegasus": "Aero Pegasus",
    "[ ] Aero Pegasus": "Aero Pegasus",
    # Typos - Cobalt Dragoon
    "Coablt Dragoon": "Cobalt Dragoon",
    "Cobal Dragoon": "Cobalt Dragoon",
    "Cobalt Dragon": "Cobalt Dragoon",
    "Colbat Dragoon": "Cobalt Dragoon",
    "Side: Cobalt Dragoon": "Cobalt Dragoon",
    "[ ] Cobalt Dragoon": "Cobalt Dragoon",
    # Typos - Hover Wyvern
    "Hovern Wyvern": "Hover Wyvern",
    "Hover": "Hover Wyvern",
    # Typos - Roar Tyranno
    "Roar Tyrnano": "Roar Tyranno",
    "Roar Tryanno": "Roar Tyranno",
    "Tyrano Roar": "Roar Tyranno",
    # Typos - Tyranno Beat
    "Tyranno Beat,": "Tyranno Beat",
    "Tyrano Beat": "Tyranno Beat",
    "Tyranno": "Tyranno Beat",
    "T. Rex": "Tyranno Beat",
    "Beat Tyranno": "Tyranno Beat",
    # Typos - Samurai
    "Samauri Saber": "Samurai Saber",
    "Samurai Caliber": "Samurai Calibur",
    "Samurai": "Samurai Saber",
    # Typos - Wizard Rod
    "Wizardr Rod": "Wizard Rod",
    "Wizarz Rod": "Wizard Rod",
    "Wizard Rod,": "Wizard Rod",
    "Side Board) Wizard Rod": "Wizard Rod",
    "[ ] Wizard Rod": "Wizard Rod",
    "Wand Wizard": "Wizard Rod",  # German name variant
    # Typos - Silver Wolf
    "Sliver Wolf": "Silver Wolf",
    "Sterling Wolf": "Silver Wolf",
    "Side: Silver Wolf": "Silver Wolf",
    # Typos - Shark Scale
    "Shake Scale": "Shark Scale",
    # Typos - Whale Wave / Tide Whale (same blade)
    "Wave Wave": "Whale Wave",
    "Tide Whale": "Whale Wave",
    # Typos - Golem Rock
    "Rock Golem": "Golem Rock",
    "[ ] Golem Rock": "Golem Rock",
    # Typos - Phoenix Wing
    "Soar Phoenix": "Phoenix Wing",  # Old name -> correct name
    "Phoenix2ing": "Phoenix Wing",
    "phoenix": "Phoenix Wing",
    "Phoenix": "Phoenix Wing",
    "[ ] Phoenix Wing": "Phoenix Wing",
    # Typos - Hells Scythe
    "[ ] Hells Scythe": "Hells Scythe",
    # Typos - Other
    "Wizard Arrow.": "Wizard Arrow",
    "Arrow Wizard": "Wizard Arrow",
    "uster": "Dran Buster",
    "Buster Dran": "Dran Buster",
    "Silver Samurai": "Steel Samurai",
    "Fox Blast": "Fox Brush",
    "Brush Fox": "Fox Brush",
    # Swapped names - Dran variants
    "Sword Dran": "Dran Sword",
    "Dagger Dran": "Dran Dagger",
    # Swapped names - Samurai
    "Saber Samurai": "Samurai Saber",
    # Swapped names - Other
    "Horn Rhino": "Rhino Horn",
    "Shadow Shinobi": "Shinobi Shadow",
    # Soar Phoenix is old name for Phoenix Wing
    "•  Soar Phoenix": "Phoenix Wing",
    # Bullet point prefixes
    "•  Shark Scale": "Shark Scale",
    "•  Silver Wolf": "Silver Wolf",
    "•  Golem Rock": "Golem Rock",
    "•  Wand Wizard": "Wizard Rod",
    "•  Samurai Calibur": "Samurai Calibur",
    # Player name prefixes (parsing errors)
    "Jigoku: Wand Wizard": "Wizard Rod",
    "Vio: Wand Wizard": "Wizard Rod",
    "AkhiEly: Wand Wizard": "Wizard Rod",
    "MozartEmredeus: Shark Scale": "Shark Scale",
    "stealth: Shark Scale": "Shark Scale",
    "Suárez: Tide Whale": "Whale Wave",
    "Drice: Pegasus Blast": "Pegasus Blast",
    "Zero: Buster Dran": "Dran Buster",
    "Der Grillmeister: Hover Wyvern": "Hover Wyvern",
    # Black Shell variant
    "Obsidian Shell": "Black Shell",
    # ==========================================================================
    # INVALID CX COMBOS - Two CX main blades combined (data entry errors)
    # These are invalid because you can't combine two main blades
    # Map to best guess based on context, or mark for deletion
    # ==========================================================================
    # "Might Blast" is invalid - both are CX main blades
    # Best guess: player meant Emperor Might (most common Might combo)
    "Might Blast": "Emperor Might",
    "Blast Might": "Emperor Might",
    # Other potential invalid combinations to catch
    "Brave Blast": "Dran Brave",
    "Blast Brave": "Dran Brave",
    "Arc Blast": "Wizard Arc",
    "Blast Arc": "Wizard Arc",
    "Eclipse Blast": "Sol Eclipse",
    "Blast Eclipse": "Sol Eclipse",
    "Hunt Blast": "Wolf Hunt",
    "Blast Hunt": "Wolf Hunt",
    "Reaper Blast": "Hells Reaper",
    "Blast Reaper": "Hells Reaper",
    "Flare Blast": "Phoenix Flare",
    "Blast Flare": "Phoenix Flare",
    "Volt Blast": "Valkyrie Volt",
    "Blast Volt": "Valkyrie Volt",
    # Typos - Knight Mail
    "Mail Knight": "Knight Mail",
    # Parsing errors
    "|| KnightLance": "Knight Lance",
    "|| KnightShield": "Knight Shield",
    "|| Knight Lance": "Knight Lance",
    "|| Knight Shield": "Knight Shield",
    # Special format entries - Ace Pokébey prefix
    "Ace Pokébey: Impact Drake": "Impact Drake",
    "Ace Pokébey: Rock Golem": "Golem Rock",
    "Ace Pokébey: Samurai Calibur": "Samurai Calibur",
    # Japanese names that may slip through translation
    "ドランソード": "Dran Sword",
    "ヘルズサイズ": "Hells Scythe",
    "ウィザードアロー": "Wizard Arrow",
    "ナイトシールド": "Knight Shield",
    "ナイトランス": "Knight Lance",
    "ドランバスター": "Dran Buster",
    "ヘルズハンマー": "Hells Hammer",
    "ウィザードロッド": "Wizard Rod",
    "シルバーウルフ": "Silver Wolf",
    "サムライセイバー": "Samurai Saber",
    "ペガサスブラスト": "Pegasus Blast",
    "エアロペガサス": "Aero Pegasus",
    "コバルトドラグーン": "Cobalt Dragoon",
    "メテオドラグーン": "Meteor Dragoon",
    "ゴーレムロック": "Golem Rock",
    "インパクトドレイク": "Impact Drake",
    "シャークスケイル": "Shark Scale",
    "ホバーワイバーン": "Hover Wyvern",
    "フォックスブラストW": "Brush",  # Fox Brush -> Brush (CX main blade)
    # CX blades - normalize to just the main blade name
    # The lock chip is stored separately via parse_cx_blade()
    # Blast main blade variants
    "Emperor Blast": "Blast",
    "Wolf Blast": "Blast",
    "Dran Blast": "Blast",
    "Pegasus Blast": "Blast",
    "Hells Blast": "Blast",
    "Cerberus Blast": "Blast",
    "Valkyrie Blast": "Blast",
    "Valkyrie Blast W": "Blast",
    "Valkyrie Blast S": "Blast",
    "Sol Blast": "Blast",
    "Perseus Blast W": "Blast",
    "Kraken Blast": "Blast",
    "ドランブラストW": "Blast",
    "ペガサスブラストW": "Blast",
    "ソルブラストJ": "Blast",
    "Pegasus Blast (Ultra Instinct)": "Blast",
    "Pegasus (Ultra Instinct) Blast": "Blast",
    # Hunt main blade variants
    "Emperor Hunt": "Hunt",
    "Wolf Hunt": "Hunt",
    "Perseus Hunt": "Hunt",
    # Eclipse main blade variants
    "Emperor Eclipse": "Eclipse",
    "Sol Eclipse": "Eclipse",
    "Hells Eclipse": "Eclipse",
    # Might main blade variants
    "Emperor Might": "Might",
    "Cerberus Might": "Might",
    "Dran Might": "Might",
    "Whale Might": "Might",
    # Brave main blade variants
    "Dran Brave": "Brave",
    "Emperor Brave": "Brave",
    # Volt main blade variants
    "Valkyrie Volt": "Volt",
    "Valkyrie Volt A": "Volt",
    # Flare main blade variants
    "Phoenix Flare": "Flare",
    "Pegasus Flame": "Flare",
    # Arc main blade variants
    "Wizard Arc": "Arc",
    # Dark main blade variants
    "Perseus Dark": "Dark",
    # Reaper main blade variants
    "Hells Reaper": "Reaper",
    # Brush main blade variants
    "Fox Brush": "Brush",
    # BX classic remakes - keep full name (NOT CX blades)
    "Dragoon Storm": "Dragoon Storm",
    "Driger Slash": "Driger Slash",
    # Standalone CX main blades - just keep as-is
    "Blast": "Blast",
    "Hunt": "Hunt",
    "Might": "Might",
    "Brave": "Brave",
    "Arc": "Arc",
    "Brush": "Brush",
    "Eclipse": "Eclipse",
    "Volt": "Volt",
    "Flare": "Flare",
    "Dark": "Dark",
    "Reaper": "Reaper",
    # Note: Storm and Slash are NOT CX main blades
    # Dragoon Storm and Driger Slash are BX classic remakes
    # Emperor alone probably meant a CX blade
    "Emperor": "Blast",
    # =========================================================================
    # CamelCase blades - add spaces (from WBO scraper output)
    # =========================================================================
    "WizardRod": "Wizard Rod",
    "PhoenixWing": "Phoenix Wing",
    "CobaltDragoon": "Cobalt Dragoon",
    "AeroPegasus": "Aero Pegasus",
    "SharkScale": "Shark Scale",
    "TyrannoBeat": "Tyranno Beat",
    "HoverWyvern": "Hover Wyvern",
    "SilverWolf": "Silver Wolf",
    "KnightMail": "Knight Mail",
    "DranBuster": "Dran Buster",
    "HellsScythe": "Hells Scythe",
    "ImpactDrake": "Impact Drake",
    "SharkEdge": "Shark Edge",
    "ScorpioSpear": "Scorpio Spear",
    "DranSword": "Dran Sword",
    "WhaleWave": "Whale Wave",
    "GolemRock": "Golem Rock",
    "LeonClaw": "Leon Claw",
    "WyvernGale": "Wyvern Gale",
    "LeonCrest": "Leon Crest",
    "ShinobiShadow": "Shinobi Shadow",
    "SamuraiCalibur": "Samurai Calibur",
    "ViperTail": "Viper Tail",
    "PhoenixFeather": "Phoenix Feather",
    "DrigerSlash": "Driger Slash",
    "DragoonStorm": "Dragoon Storm",
    "RhinoHorn": "Rhino Horn",
    "WizardArrow": "Wizard Arrow",
    "KnightShield": "Knight Shield",
    "KnightLance": "Knight Lance",
    "HellsChain": "Hells Chain",
    "HellsHammer": "Hells Hammer",
    "UnicornSting": "Unicorn Sting",
    "BlackShell": "Black Shell",
    "DranDagger": "Dran Dagger",
    "WeissTiger": "Weiss Tiger",
    "CobaltDrake": "Cobalt Drake",
    "CrimsonGaruda": "Crimson Garuda",
    "TalonPtera": "Talon Ptera",
    "RoarTyranno": "Roar Tyranno",
    "SphinxCowl": "Sphinx Cowl",
    "ShelterDrake": "Shelter Drake",
    "TrySpress": "Tricera Press",
    "TriceraPress": "Tricera Press",
    "BearScratch": "Bear Scratch",
    "XenoXcalibur": "Xeno Xcalibur",
    "SteelSamurai": "Steel Samurai",
    "BurnWyvern": "Burn Wyvern",
    "TuskMammoth": "Tusk Mammoth",
    "DranzerSpiral": "Dranzer Spiral",
    "MeteorDragoon": "Meteor Dragoon",
    "SamuraiSaber": "Samurai Saber",
    "GhostCircle": "Ghost Circle",
    "BiteCroc": "Bite Croc",
    "KeelShark": "Keel Shark",
    "KnifeShinobi": "Knife Shinobi",
    "ChainIncendio": "Chain Incendio",
    "ClockMirage": "Clock Mirage",
    "GillShark": "Gill Shark",
    "MummyCurse": "Mummy Curse",
    "ScytheIncendio": "Scythe Incendio",
    "StingUnicorn": "Sting Unicorn",
    "WandWizard": "Wizard Rod",
    "ProminencePhoenix": "Prominence Phoenix",
    "PhoenixRudder": "Phoenix Rudder",
}

# Bit abbreviation normalizations (unexpanded abbreviations -> full names)
BIT_NORMALIZATIONS = {
    # Single letter abbreviations that weren't expanded
    "U": "Unite",
    "L": "Level",
    "E": "Elevate",
    "G": "Glide",
    "Q": "Quake",
    "K": "Kick",
    "V": "Vanguard",
    # Two letter that might slip through
    "Lv": "Level",
    "El": "Elevate",
    "Un": "Unite",
    "Br": "Brake",
    "Bd": "Bound",
    "Gl": "Glide",
    "WB": "Wall Ball",
    "UN": "Under Needle",
    "RA": "Rubber Accel",
    "FB": "Free Ball",
    "UF": "Upper Flat",
    "GF": "Gear Flat",
    "GB": "Gear Ball",
    "GN": "Gear Needle",
    "GP": "Gear Point",
    "HN": "High Needle",
    "LF": "Low Flat",
    "LR": "Low Rush",
    "LN": "Low Needle",
    "MN": "Metal Needle",
    "HT": "High Taper",
    "HA": "High Accel",
    "DB": "Disc Ball",
    # Inconsistent naming
    "Hex": "Hexa",
    # CamelCase bits - add spaces
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

# Ratchet normalizations (typos and invalid values)
RATCHET_NORMALIZATIONS = {
    "5-50": "4-50",
}


def normalize_data(conn: duckdb.DuckDBPyConnection = None) -> int:
    """
    Normalize data by fixing known typos and inconsistencies.

    Call this after inserting new data to ensure consistency.
    Returns the total number of records fixed.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    total_fixed = 0

    # Fix blade normalizations
    for wrong, correct in BLADE_NORMALIZATIONS.items():
        # Count and fix blade_1
        count1 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_1 = ?", [wrong]
        ).fetchone()[0]
        if count1 > 0:
            conn.execute(
                "UPDATE placements SET blade_1 = ? WHERE blade_1 = ?", [correct, wrong]
            )

        # Count and fix blade_2
        count2 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_2 = ?", [wrong]
        ).fetchone()[0]
        if count2 > 0:
            conn.execute(
                "UPDATE placements SET blade_2 = ? WHERE blade_2 = ?", [correct, wrong]
            )

        # Count and fix blade_3
        count3 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_3 = ?", [wrong]
        ).fetchone()[0]
        if count3 > 0:
            conn.execute(
                "UPDATE placements SET blade_3 = ? WHERE blade_3 = ?", [correct, wrong]
            )

        total_fixed += count1 + count2 + count3

    # Fix bit normalizations
    for wrong, correct in BIT_NORMALIZATIONS.items():
        # Count and fix bit_1
        count1 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE bit_1 = ?", [wrong]
        ).fetchone()[0]
        if count1 > 0:
            conn.execute(
                "UPDATE placements SET bit_1 = ? WHERE bit_1 = ?", [correct, wrong]
            )

        # Count and fix bit_2
        count2 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE bit_2 = ?", [wrong]
        ).fetchone()[0]
        if count2 > 0:
            conn.execute(
                "UPDATE placements SET bit_2 = ? WHERE bit_2 = ?", [correct, wrong]
            )

        # Count and fix bit_3
        count3 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE bit_3 = ?", [wrong]
        ).fetchone()[0]
        if count3 > 0:
            conn.execute(
                "UPDATE placements SET bit_3 = ? WHERE bit_3 = ?", [correct, wrong]
            )

        total_fixed += count1 + count2 + count3

    # Fix ratchet normalizations
    for wrong, correct in RATCHET_NORMALIZATIONS.items():
        for col in ["ratchet_1", "ratchet_2", "ratchet_3"]:
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {col} = ?", [wrong]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {col} = ? WHERE {col} = ?", [correct, wrong]
                )
                total_fixed += count

    # Fix assist normalizations (use same blade normalizations)
    for wrong, correct in BLADE_NORMALIZATIONS.items():
        for col in ["assist_1", "assist_2", "assist_3"]:
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {col} = ?", [wrong]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {col} = ? WHERE {col} = ?", [correct, wrong]
                )
                total_fixed += count

    # Extract lock chips from CX blade names and update lock_chip columns
    for cx_name, (lock_chip, main_blade) in CX_BLADE_COMPONENTS.items():
        for i in [1, 2, 3]:
            blade_col = f"blade_{i}"
            lock_col = f"lock_chip_{i}"
            # Update blade to main blade and set lock chip where lock_chip is NULL
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {blade_col} = ? AND {lock_col} IS NULL",
                [cx_name],
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {blade_col} = ?, {lock_col} = ? WHERE {blade_col} = ? AND {lock_col} IS NULL",
                    [main_blade, lock_chip, cx_name],
                )
                total_fixed += count

    # Clean up invalid lock chips - main blades should NOT be lock chips
    MAIN_BLADES = {
        "Blast",
        "Brave",
        "Arc",
        "Dark",
        "Reaper",
        "Brush",
        "Eclipse",
        "Hunt",
        "Might",
        "Flare",
        "Volt",
        "Storm",
        "Slash",
    }
    for i in [1, 2, 3]:
        lock_col = f"lock_chip_{i}"
        for blade in MAIN_BLADES:
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {lock_col} = ?", [blade]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {lock_col} = NULL WHERE {lock_col} = ?",
                    [blade],
                )
                total_fixed += count

    if should_close:
        conn.close()

    return total_fixed


def reset_database() -> None:
    """Drop all tables and recreate schema. Use with caution."""
    conn = get_connection()
    conn.execute("DROP VIEW IF EXISTS combo_stats")
    conn.execute("DROP VIEW IF EXISTS part_stats")
    conn.execute("DROP VIEW IF EXISTS combo_usage")
    conn.execute("DROP TABLE IF EXISTS placements")
    conn.execute("DROP TABLE IF EXISTS tournaments")
    conn.execute("DROP TABLE IF EXISTS parts")
    init_schema(conn)
    conn.close()


if __name__ == "__main__":
    print(f"Initializing database at {DB_PATH}")
    init_schema()
    print("Schema initialized successfully.")

    # Show tables
    conn = get_connection()
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"\nTables: {[t[0] for t in tables]}")
    conn.close()
