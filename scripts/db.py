"""
Database connection and schema management for BeybladeX Database.

Single source of truth: site/public/data/beyblade.duckdb
(Used by both scrapers and the website)
"""

import duckdb
import fcntl
import os
import re
from contextlib import contextmanager
from pathlib import Path

# Database path - single source of truth, used directly by the website
DB_PATH = Path(__file__).parent.parent / "site" / "public" / "data" / "beyblade.duckdb"

# Battles database - separate file for personal gameplay data.
# Never touched by git stash/deploy — isolated from tournament data.
BATTLES_DB_PATH = Path(__file__).parent.parent / "data" / "battles.duckdb"

# Backup path for JSON exports of battle data
BATTLES_BACKUP_PATH = Path(__file__).parent.parent / "data" / "backups" / "battles_backup.json"

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
#
# All valid CX lock chips, main blades, and full combo names come from the
# parts_catalog table (populated by scripts/scrapers/fandom.py from the wiki).
# Use _cat() inside parsing functions to get the catalog at runtime so the
# parser auto-picks-up new wiki releases without code edits.

def _cat():
    """Lazy import to avoid circular dependency at module load."""
    from catalog import PartsCatalog
    return PartsCatalog.get()


def parse_cx_blade(blade_name: str) -> tuple[str | None, str]:
    """
    Parse a CX blade name into (lock_chip, main_blade).
    Returns (None, blade_name) if not a known CX blade.

    All lock chips and main blades come from the wiki-derived parts catalog,
    so any new release auto-parses without code edits. Handled formats:
    - "Pegasus Blast"   -> ("Pegasus", "Blast")
    - "Blast Pegasus"   -> ("Pegasus", "Blast")
    - "PegasusBlast"    -> ("Pegasus", "Blast")
    - "Valkyrie Blast W"-> ("Valkyrie", "Blast")  (trailing variant suffix)
    - case insensitive
    """
    cat = _cat()
    chips_lower = cat.lock_chips_lower
    mains_lower = cat.main_blades_lower
    metals_lower = {b.lower(): b for b in cat.metal_blades}

    # 1. Exact match against the computed CX full-name set
    if blade_name in cat.cx_full_names:
        parts = blade_name.split()
        if len(parts) == 2 and parts[0] in cat.lock_chips:
            if parts[1] in cat.main_blades or parts[1] in cat.metal_blades:
                return (parts[0], parts[1])
        if blade_name in cat.metal_blades:
            return (None, blade_name)

    # 2. Strip trailing variant suffix like " W" or " S"
    normalized = blade_name.strip()
    if len(normalized) > 2 and normalized[-2] == " " and normalized[-1] in "WSAFHTJ":
        suffix_stripped = normalized[:-2]
        if suffix_stripped in cat.cx_full_names:
            parts = suffix_stripped.split()
            if len(parts) == 2 and parts[0] in cat.lock_chips:
                return (parts[0], parts[1])
        normalized = suffix_stripped

    normalized_lower = normalized.lower()

    # 3. Two-word form: "Lock Main" or "Main Lock"
    parts = normalized.split()
    if len(parts) >= 2:
        first_lower = parts[0].lower()
        second_lower = parts[1].lower()
        if first_lower in chips_lower and (second_lower in mains_lower or second_lower in metals_lower):
            return (
                chips_lower[first_lower],
                mains_lower.get(second_lower) or metals_lower[second_lower],
            )
        if (first_lower in mains_lower or first_lower in metals_lower) and second_lower in chips_lower:
            return (
                chips_lower[second_lower],
                mains_lower.get(first_lower) or metals_lower[first_lower],
            )

    # 4. Concatenated form: "PegasusBlast"
    name_no_space = normalized_lower.replace(" ", "")
    for chip_lower, chip in chips_lower.items():
        for main_lower, main in mains_lower.items():
            if name_no_space == chip_lower + main_lower or name_no_space.startswith(chip_lower + main_lower):
                return (chip, main)
            if name_no_space == main_lower + chip_lower or name_no_space.startswith(main_lower + chip_lower):
                return (chip, main)

    return (None, blade_name)


def is_incomplete_cx_blade(blade_name: str, lock_chip: str | None) -> bool:
    """A bare CX main blade (e.g., "Blast" without "Pegasus") is incomplete."""
    return blade_name in _cat().main_blades and lock_chip is None


def is_invalid_two_main_blades(blade_name: str) -> bool:
    """Two CX main blade words like "Might Blast" is impossible — flag it."""
    parts = blade_name.split()
    if len(parts) != 2:
        return False
    mains_lower = _cat().main_blades_lower
    return parts[0].lower() in mains_lower and parts[1].lower() in mains_lower


# Default lock-chip guesses used by validate_and_fix_blade() to repair
# broken two-main-blade entries. Not part-release-sensitive.
_DEFAULT_LOCK_CHIP_BY_MAIN = {
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


def validate_and_fix_blade(blade_name: str) -> str:
    """Repair broken blade names: typos and invalid two-main-blade combos."""
    from db import BLADE_NORMALIZATIONS  # defined later in this module

    if blade_name in BLADE_NORMALIZATIONS:
        return BLADE_NORMALIZATIONS[blade_name]

    if is_invalid_two_main_blades(blade_name):
        parts = blade_name.split()
        main_blade = _cat().main_blades_lower.get(parts[0].lower(), parts[0])
        lock_chip = _DEFAULT_LOCK_CHIP_BY_MAIN.get(main_blade, "")
        return f"{lock_chip} {main_blade}".strip()

    return blade_name




def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a connection to the tournament database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def get_battles_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a connection to the battles database (personal gameplay data).

    This is a separate file from the tournament DB so it's never affected
    by git stash/deploy cycles or tournament rescrapes. Your battle data
    is safe even if the tournament DB gets wiped and rebuilt.
    """
    BATTLES_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(BATTLES_DB_PATH), read_only=read_only)


def init_battles_schema(conn: duckdb.DuckDBPyConnection = None) -> None:
    """Initialize the battles database schema (separate from tournament DB)."""
    should_close = conn is None
    if conn is None:
        conn = get_battles_connection()

    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_battles START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_stamina START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS battles (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_battles'),
            combo_id VARCHAR NOT NULL,
            opponent_id VARCHAR NOT NULL,
            stadium VARCHAR,
            finish_type VARCHAR NOT NULL,
            result VARCHAR NOT NULL,
            points INTEGER,
            session_id VARCHAR,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp,
            -- Structured combo components (mirrors placements table)
            blade VARCHAR,
            lock_chip VARCHAR,
            over_blade VARCHAR,
            assist VARCHAR,
            ratchet VARCHAR,
            bit VARCHAR,
            -- Structured opponent components
            opp_blade VARCHAR,
            opp_lock_chip VARCHAR,
            opp_over_blade VARCHAR,
            opp_assist VARCHAR,
            opp_ratchet VARCHAR,
            opp_bit VARCHAR
        )
    """)

    # Idempotent migration for existing tables
    for col in ['blade', 'lock_chip', 'over_blade', 'assist', 'ratchet', 'bit',
                'opp_blade', 'opp_lock_chip', 'opp_over_blade', 'opp_assist', 'opp_ratchet', 'opp_bit']:
        try:
            conn.execute(f"ALTER TABLE battles ADD COLUMN {col} VARCHAR")
        except Exception:
            pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stamina_trials (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_stamina'),
            combo_id VARCHAR NOT NULL,
            spin_time_ms INTEGER NOT NULL,
            trial_number INTEGER,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    if should_close:
        conn.close()


def backup_battles_to_json() -> str:
    """Export all battles + stamina data to a JSON file for version control.

    Called manually via the "Commit" button on the tracker page.
    The JSON file is human-readable, diffable, and safe to commit to git.
    Returns the path of the backup file.
    """
    import json

    conn = get_battles_connection(read_only=True)
    try:
        battles = conn.execute("""
            SELECT id, combo_id, opponent_id, stadium, finish_type,
                   result, points, session_id, notes,
                   created_at::VARCHAR as created_at,
                   blade, lock_chip, over_blade, assist, ratchet, bit,
                   opp_blade, opp_lock_chip, opp_over_blade, opp_assist, opp_ratchet, opp_bit
            FROM battles ORDER BY id
        """).fetchall()

        stamina = conn.execute("""
            SELECT id, combo_id, spin_time_ms, trial_number, notes,
                   created_at::VARCHAR as created_at
            FROM stamina_trials ORDER BY id
        """).fetchall()
    finally:
        conn.close()

    data = {
        "exported_at": __import__("datetime").datetime.now().isoformat(),
        "battles": [
            {
                "id": r[0], "combo_id": r[1], "opponent_id": r[2],
                "stadium": r[3], "finish_type": r[4], "result": r[5],
                "points": r[6], "session_id": r[7], "notes": r[8],
                "created_at": r[9],
                "blade": r[10], "lock_chip": r[11], "over_blade": r[12],
                "assist": r[13], "ratchet": r[14], "bit": r[15],
                "opp_blade": r[16], "opp_lock_chip": r[17], "opp_over_blade": r[18],
                "opp_assist": r[19], "opp_ratchet": r[20], "opp_bit": r[21],
            }
            for r in battles
        ],
        "stamina_trials": [
            {
                "id": r[0], "combo_id": r[1], "spin_time_ms": r[2],
                "trial_number": r[3], "notes": r[4], "created_at": r[5],
            }
            for r in stamina
        ],
    }

    BATTLES_BACKUP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BATTLES_BACKUP_PATH, "w") as f:
        json.dump(data, f, indent=2)

    return str(BATTLES_BACKUP_PATH)


def restore_battles_from_json(path: str = None) -> tuple[int, int]:
    """Restore battles + stamina from a JSON backup. Clears existing data first.

    Returns (battles_count, stamina_count).
    """
    import json

    path = path or str(BATTLES_BACKUP_PATH)
    with open(path) as f:
        data = json.load(f)

    conn = get_battles_connection()
    init_battles_schema(conn)

    conn.execute("DELETE FROM battles")
    conn.execute("DELETE FROM stamina_trials")

    for b in data.get("battles", []):
        conn.execute("""
            INSERT INTO battles (combo_id, opponent_id, stadium, finish_type,
                                 result, points, session_id, notes,
                                 blade, lock_chip, over_blade, assist, ratchet, bit,
                                 opp_blade, opp_lock_chip, opp_over_blade, opp_assist, opp_ratchet, opp_bit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [b["combo_id"], b["opponent_id"], b.get("stadium"),
              b["finish_type"], b["result"], b.get("points"),
              b.get("session_id"), b.get("notes"),
              b.get("blade"), b.get("lock_chip"), b.get("over_blade"),
              b.get("assist"), b.get("ratchet"), b.get("bit"),
              b.get("opp_blade"), b.get("opp_lock_chip"), b.get("opp_over_blade"),
              b.get("opp_assist"), b.get("opp_ratchet"), b.get("opp_bit")])

    for s in data.get("stamina_trials", []):
        conn.execute("""
            INSERT INTO stamina_trials (combo_id, spin_time_ms, trial_number, notes)
            VALUES (?, ?, ?, ?)
        """, [s["combo_id"], s["spin_time_ms"], s.get("trial_number"), s.get("notes")])

    conn.commit()
    conn.close()

    return len(data.get("battles", [])), len(data.get("stamina_trials", []))


def init_schema(conn: duckdb.DuckDBPyConnection = None) -> None:
    """Initialize the database schema."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    # Create sequences for auto-increment
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_tournaments START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_placements START 1")

    # Parts catalog table - source of truth for valid parts
    # Populated by the Fandom wiki scraper from Category:* pages.
    # Drift detection adds rows with status='pending' for unknown parts seen in tournaments.
    # Composite PK so the same name can exist as multiple part_types
    # (e.g., "Wolf" the lock chip, "Wolf" the blade).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS parts_catalog (
            name VARCHAR NOT NULL,
            part_type VARCHAR NOT NULL,
            system VARCHAR,
            canonical_name VARCHAR,
            aliases VARCHAR,
            wiki_url VARCHAR,
            status VARCHAR DEFAULT 'accepted',
            source VARCHAR,
            metal BOOLEAN DEFAULT FALSE,
            sample_combo VARCHAR,
            occurrence_count INTEGER DEFAULT 0,
            discovered_at TIMESTAMP DEFAULT current_timestamp,
            accepted_at TIMESTAMP,
            suggested_canonical VARCHAR,
            suggestion_reason VARCHAR,
            suggestion_confidence DOUBLE,
            suggested_at TIMESTAMP,
            PRIMARY KEY (name, part_type)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parts_catalog_type_status ON parts_catalog(part_type, status)")

    # Idempotent migration: add suggestion columns to pre-existing tables
    # (from an earlier deploy without these columns).
    for col, typ in [
        ("suggested_canonical", "VARCHAR"),
        ("suggestion_reason", "VARCHAR"),
        ("suggestion_confidence", "DOUBLE"),
        ("suggested_at", "TIMESTAMP"),
    ]:
        try:
            conn.execute(f"ALTER TABLE parts_catalog ADD COLUMN {col} {typ}")
        except Exception:
            pass  # column already exists

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
            bit_1 VARCHAR,
            assist_1 VARCHAR,
            lock_chip_1 VARCHAR,
            over_blade_1 VARCHAR,
            stage_1 VARCHAR,
            blade_2 VARCHAR,
            ratchet_2 VARCHAR,
            bit_2 VARCHAR,
            assist_2 VARCHAR,
            lock_chip_2 VARCHAR,
            over_blade_2 VARCHAR,
            stage_2 VARCHAR,
            blade_3 VARCHAR,
            ratchet_3 VARCHAR,
            bit_3 VARCHAR,
            assist_3 VARCHAR,
            lock_chip_3 VARCHAR,
            over_blade_3 VARCHAR,
            stage_3 VARCHAR,
            UNIQUE(tournament_id, place)
        )
    """)

    # Part attributes table (official Takara Tomy stats from Fandom wiki)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS part_attributes (
            name VARCHAR PRIMARY KEY,
            part_type VARCHAR NOT NULL,
            system VARCHAR,
            attack INTEGER,
            defense INTEGER,
            stamina INTEGER,
            dash INTEGER,
            burst_resistance INTEGER,
            weight DECIMAL(5,1),
            spin_direction VARCHAR,
            scraped_at TIMESTAMP DEFAULT current_timestamp
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
            p.over_blade_1 as over_blade,
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
            p.over_blade_2,
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
            p.over_blade_3,
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
            UNION ALL
            SELECT place, over_blade, 'over_blade' FROM combo_usage WHERE over_blade IS NOT NULL
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
    "Aero Pegauss": "Aero Pegasus",
    "Aerp Pegasus": "Aero Pegasus",
    "[ ] Aero Pegasus": "Aero Pegasus",
    # Typos - Cobalt Dragoon
    "Coablt Dragoon": "Cobalt Dragoon",
    "Cobal Dragoon": "Cobalt Dragoon",
    "Cobalt Dragon": "Cobalt Dragoon",
    "Cobalt Fragoon": "Cobalt Dragoon",
    "Cobalt Goon": "Cobalt Dragoon",
    "Colbat Dragoon": "Cobalt Dragoon",
    "Colbalt Dragoon": "Cobalt Dragoon",
    "Obalt Dragoon": "Cobalt Dragoon",
    "cobalt": "Cobalt Dragoon",
    "Side: Cobalt Dragoon": "Cobalt Dragoon",
    "[ ] Cobalt Dragoon": "Cobalt Dragoon",
    # Typos - Cobalt Drake
    "Colbalt Drake": "Cobalt Drake",
    "Obalt Drake": "Cobalt Drake",
    # Typos - Hover Wyvern
    "Hovern Wyvern": "Hover Wyvern",
    "Hoover Wyvern": "Hover Wyvern",
    "Hover Wyven": "Hover Wyvern",
    "Hover Wyvren": "Hover Wyvern",
    "Hover": "Hover Wyvern",
    # Typos - Roar Tyranno
    "Roar Tyrnano": "Roar Tyranno",
    "Roar Tryanno": "Roar Tyranno",
    "Roar Tyrano": "Roar Tyranno",
    "Tyrano Roar": "Roar Tyranno",
    # Typos - Tyranno Beat
    "Tyranno Beat,": "Tyranno Beat",
    "Tyrano Beat": "Tyranno Beat",
    "Tryanno Beat": "Tyranno Beat",
    "Tyrannno Beat": "Tyranno Beat",
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
    "Wizard Rob": "Wizard Rod",
    "Wizzard Rod": "Wizard Rod",
    "Izard Rod": "Wizard Rod",
    "Side Board) Wizard Rod": "Wizard Rod",
    "[ ] Wizard Rod": "Wizard Rod",
    "Wand Wizard": "Wizard Rod",  # German name variant
    # Typos - Silver Wolf
    "Sliver Wolf": "Silver Wolf",
    "Sillver Wolf": "Silver Wolf",
    "Ilver Wolf": "Silver Wolf",
    "Sterling Wolf": "Silver Wolf",
    "Side: Silver Wolf": "Silver Wolf",
    # Typos - Shark Scale
    "Shake Scale": "Shark Scale",
    "Scark Scale": "Shark Scale",
    # Typos - Whale Wave / Tide Whale (same blade)
    "Wave Wave": "Whale Wave",
    "Tide Whale": "Whale Wave",
    # Typos - Golem Rock
    "Rock Golem": "Golem Rock",
    "[ ] Golem Rock": "Golem Rock",
    # Typos - Phoenix Wing
    "Soar Phoenix": "Phoenix Wing",  # Old name -> correct name
    "Phoenix2ing": "Phoenix Wing",
    "Pheonix Wing": "Phoenix Wing",
    "Phoneix Wing": "Phoenix Wing",
    "Phoenixx Wing": "Phoenix Wing",
    "Hoenix Wing": "Phoenix Wing",
    "Phoenix Ing": "Phoenix Wing",
    "phoenix": "Phoenix Wing",
    "Phoenix": "Phoenix Wing",
    "[ ] Phoenix Wing": "Phoenix Wing",
    # Typos - Hells Scythe
    "Hells Sythe": "Hells Scythe",
    "[ ] Hells Scythe": "Hells Scythe",
    # Typos - Hells Chain
    "Hellshain": "Hells Chain",
    # Typos - Dran Buster
    "Dranzbuster": "Dran Buster",
    # Typos - Scorpio Spear
    "Scorpion Spear": "Scorpio Spear",
    "Scorpion Sting": "Scorpio Spear",
    "Rpio Spear": "Scorpio Spear",
    # Typos - Eclipse
    "Sol Ecllipse": "Eclipse",
    # Swapped names
    "Sting Unicorn": "Unicorn Sting",
    "Goat Tackle": "Tackle Goat",
    # Hasbro alternate names
    "Croc Crunch": "Bite Croc",
    "Dragon Buster": "Dran Buster",
    "Draciel Shield": "Knight Shield",  # Hasbro classic remake name
    "Pearl Tiger": "Weiss Tiger",
    "Scarlet Garuda": "Crimson Garuda",
    "Ptera Swing": "Talon Ptera",
    "Tricera Spiky": "Tricera Press",
    "Savage Bear": "Bear Scratch",
    "Helm Knight": "Knight Mail",  # Hasbro UX name
    "Hammer Incendio": "Hells Hammer",  # Incendio = Hasbro for Hells
    "Tide Whale": "Whale Wave",  # Hasbro name
    "Scale Shark": "Shark Scale",  # word order typo
    "Optimus Prime": "Optimus Primal",
    # L-Drago variants → normalize to L-Drago (modes don't matter for rankings)
    "Lightning L-Drago": "L-Drago",
    "Lighting L-Drago": "L-Drago",
    "Lightning L-Drago (Upper)": "L-Drago",
    "Lightning L-Drago(Upper)": "L-Drago",
    "L-Drago (Upper)": "L-Drago",
    "L-Drago (Lower)": "L-Drago",
    # Standalone Dragoon → Dragoon Storm
    "Dragoon": "Dragoon Storm",
    # Storm Pegasus variant
    "Storm Pegasus": "Storm Pegasis",
    # Scythe alone → Hells Scythe
    "Scythe": "Hells Scythe",
    # Wizard alone → Wizard Rod (most common)
    "Wizard": "Wizard Rod",
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
    # Prefix garbage - combo labels
    "(Both) Shark Edge": "Shark Edge",
    "(Both) Wizard Rod": "Wizard Rod",
    "Combo 1 - Wizard Rod": "Wizard Rod",
    "Combo 1 – Hover Wyvern": "Hover Wyvern",
    "Combo 2 - Cobalt Dragoon": "Cobalt Dragoon",
    "Combo 2 - Scorpio Spear": "Scorpio Spear",
    "Combo 2 – Fox Blast": "Brush",
    "Combo 3 - Shark Scale": "Shark Scale",
    "Combo 3 – Shark Scale": "Shark Scale",
    # Prefix garbage - number prefixes
    "1- Phoenix Wing": "Phoenix Wing",
    "1- Tyranno Beat": "Tyranno Beat",
    "1- Wizard Rod": "Wizard Rod",
    "2- Aero Pegasus": "Aero Pegasus",
    "2- Phoenix Wing": "Phoenix Wing",
    "3- Cobalt Dragoon": "Cobalt Dragoon",
    "3- Silver Wolf": "Silver Wolf",
    "3- Wizard Rod": "Wizard Rod",
    # Prefix garbage - special characters
    ". Monz Steel Samurai": "Steel Samurai",
    "]Golem Rock": "Golem Rock",
    "]Phoenix Rudder": "Phoenix Rudder",
    "]Shark Edge": "Shark Edge",
    "[Size=Medium]Phoenix Wing": "Phoenix Wing",
    "|| Hells Chain": "Hells Chain",
    "🦅 Phoenix Wing": "Phoenix Wing",
    "🦈 Shark Scale": "Shark Scale",
    "🦖Tyranno Beat": "Tyranno Beat",
    # Prefix garbage - @ mentions
    "@Hunter Xhuntress Whale Wave": "Whale Wave",
    "@Raatch Tyranno Beat": "Tyranno Beat",
    "@Tyranno Beat": "Tyranno Beat",
    # Prefix garbage - player names
    "A-Pac Cobalt Dragoon": "Cobalt Dragoon",
    "Beezo Phoenix Wing": "Phoenix Wing",
    "Beyblader Nick Wizard Rod": "Wizard Rod",
    "Cyrus25 1St Wizard Rod": "Wizard Rod",
    "Delimaker00 Silver Wolf": "Silver Wolf",
    "Doompend1Ng Wizard Rod": "Wizard Rod",
    "Felipe X - Wizard Rod": "Wizard Rod",
    "Hylian Alchemist Tyranno Beat": "Tyranno Beat",
    "Jeto-Jaguar Cobalt Dragoon": "Cobalt Dragoon",
    "Joint 3Rd. Big Strike Hells Scythe": "Hells Scythe",
    "Jont 3Rd. Le Xshaken Dran Sword": "Dran Sword",
    "Just A - Aero Pegasus": "Aero Pegasus",
    "Just A - Hells Chain": "Hells Chain",
    "Liliz Wizard Rod": "Wizard Rod",
    "The Rogue Blader Cobalt Dragoon": "Cobalt Dragoon",
    # Prefix garbage - color labels
    "Blue Blade - Dran Buster": "Dran Buster",
    "Blue Blade - Hells Chain": "Hells Chain",
    "Red Blade - Phoenix Wing": "Phoenix Wing",
    "Red Blade - Tyranno Beat": "Tyranno Beat",
    "Red Blade - Unicorn Sting": "Unicorn Sting",
    "White Blade - Weiss Tiger": "Weiss Tiger",
    # Multi-combo dumps (extract first blade)
    "Cobalt Dragoon 1-60P + Pegasus Blast": "Cobalt Dragoon",
    "Hells Chain 5-70Fb + Dran Buster": "Hells Chain",
    "Shark Scale 1-70 Hexa     /    Shark Scale": "Shark Scale",
    "Tyranno Beat 9-70R, Tyranno Beat": "Tyranno Beat",
    "Wizard Rod 1-60 Free Ball     /     Wizard Rod": "Wizard Rod",
    # Long multi-combo dumps (all combos in one blade field)
    "Bite Croc 3-60R                                                              Impact Drake 1-60Lr                                                   Tyranno Beat": "Bite Croc",
    "Knight Mail 9-60P                                                            Wizard Rod 3-60Fb                                                                           Knight Mail": "Knight Mail",
    "Knight Mail 9-60Un                                                         Wizard Rod 9-60Un                                                               Tyranno Beat": "Knight Mail",
    "Optimus Primal 1-60Lr                                                    Wizard Rod 9-60H                                                       Wizard Rod": "Optimus Primal",
    "Phoenix Wing 3-60R                                                        Silver Wolf 9-70H                                                                  Wizard Rod": "Phoenix Wing",
    "Phoenix Wing 3-60R                                                        Tyranno Beat 7-80R                                                                          Cobalt Dragoon": "Phoenix Wing",
    "Shark Edge 4-60Lf                                                         Wizard Rod 9-60B                                                        Phoenix Wing": "Shark Edge",
    "Tyranno Beat 1-60Lr                                                       Leon Crest 0-80Lr                                                                 Keel Shark": "Tyranno Beat",
    "Tyranno Beat 1-60Lr                                                       Phoenix Wing 1-60Lr                                                                        Aero Pegasus": "Tyranno Beat",
    # Japanese names
    "フェニックスウィング": "Phoenix Wing",
    "ペルセウスブラストW": "Blast",
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
    "Fort Blast": "Fort",  # Two CX main blades - keep first
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
    "Pegaus Blast": "Blast",
    "Emporer Blast": "Blast",
    "Valk Blast": "Blast",
    "Hell'S Blast": "Blast",
    "Blast Assult": "Blast",
    "Valkyrie Blast Wheel": "Blast",  # Wheel is assist, handled separately
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
    # Flame main blade variants
    "Pegasus Flame": "Flame",
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
    "BurnWyvern": "Hover Wyvern",
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
    "StingUnicorn": "Unicorn Sting",
    "WandWizard": "Wizard Rod",
    "ProminencePhoenix": "Phoenix Wing",
    "PhoenixRudder": "Phoenix Rudder",
}

# Bit abbreviation normalizations (unexpanded abbreviations -> full names)
BIT_NORMALIZATIONS = {
    # ALL single letter abbreviations
    "A": "Accel",
    "B": "Ball",
    "C": "Cyclone",
    "D": "Dot",
    "E": "Elevate",
    "F": "Flat",
    "G": "Glide",
    "H": "Hexa",
    "J": "Jolt",
    "K": "Kick",
    "L": "Level",
    "M": "Merge",
    "N": "Needle",
    "O": "Orb",
    "P": "Point",
    "Q": "Quake",
    "R": "Rush",
    "S": "Spike",
    "T": "Taper",
    "U": "Unite",
    "V": "Vanguard",
    "W": "Wedge",
    "Z": "Zap",
    # Two letter abbreviations
    "Lv": "Level",
    "El": "Elevate",
    "Un": "Unite",
    "Br": "Brake",
    "Bd": "Bound",
    "Gl": "Glide",
    "LO": "Low Orb",
    "GR": "Gear Rush",
    "BS": "Bound Spike",
    "TK": "Trans Kick",
    "TP": "Trans Point",
    "WW": "Wall Wedge",
    "WB": "Wall Ball",
    "UN": "Under Needle",
    "RA": "Rubber Accel",
    "FB": "Free Ball",
    "UF": "Under Flat",
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
    # Inconsistent naming / casing
    "Hex": "Hexa",
    "Disk Ball": "Disc Ball",
    "Low flat": "Low Flat",
    "Low rush": "Low Rush",
    "Under needle": "Under Needle",
    "Freeball": "Free Ball",
    "Loworb": "Low Orb",
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
    "UnderFlat": "Under Flat",
    "RushAccel": "Rubber Accel",
    "BoundSpike": "Bound Spike",
    "TransKick": "Trans Kick",
    "TransPoint": "Trans Point",
    "WallWedge": "Wall Wedge",
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

    # Bootstrap the PartsCatalog singleton from the *current* writable conn
    # so subsequent _cat() calls don't try to open a second connection
    # (DuckDB only permits one writable connection per file).
    from catalog import PartsCatalog
    PartsCatalog._instance = PartsCatalog.load(conn)

    total_fixed = 0

    # Pre-cleaning: strip non-breaking spaces (\xa0) and stage annotations from all text columns
    # Some scrapers produce dirty data like 'B \xa0 \xa0 First and Finals Stage'
    stage_annotations = [
        "First and Finals Stage",
        "First Stage Only",
        "Finals Only",
        "Final Stage",
        "First Stage",
        "Both Stages",
    ]
    for col in ["bit_1", "bit_2", "bit_3", "blade_1", "blade_2", "blade_3",
                "ratchet_1", "ratchet_2", "ratchet_3", "assist_1", "assist_2", "assist_3"]:
        # Replace non-breaking spaces with regular spaces
        count = conn.execute(
            f"SELECT COUNT(*) FROM placements WHERE {col} LIKE '%' || chr(160) || '%'"
        ).fetchone()[0]
        if count > 0:
            conn.execute(
                f"UPDATE placements SET {col} = REPLACE({col}, chr(160), ' ') WHERE {col} LIKE '%' || chr(160) || '%'"
            )
            total_fixed += count

    # Strip stage annotations that got baked into column values
    for col in ["bit_1", "bit_2", "bit_3"]:
        for annotation in stage_annotations:
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {col} LIKE ?", [f"%{annotation}%"]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {col} = TRIM(REPLACE({col}, ?, '')) WHERE {col} LIKE ?",
                    [annotation, f"%{annotation}%"]
                )
                total_fixed += count

    # Final trim pass to clean up leftover whitespace
    for col in ["bit_1", "bit_2", "bit_3", "blade_1", "blade_2", "blade_3",
                "ratchet_1", "ratchet_2", "ratchet_3"]:
        conn.execute(f"UPDATE placements SET {col} = TRIM({col}) WHERE {col} != TRIM({col})")

    # Extract lock chips from CX blade names FIRST (before normalizations)
    # This must run before BLADE_NORMALIZATIONS to preserve lock chip info.
    # Iterate over every Lock+Main combination derived from the catalog.
    _cx_components = {}
    _cat_local = _cat()
    for chip in _cat_local.lock_chips:
        for main in _cat_local.main_blades:
            _cx_components[f"{chip} {main}"] = (chip, main)
    for cx_name, (lock_chip, main_blade) in _cx_components.items():
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

    # Special fixups for entries where assist blade was baked into blade name
    _assist_fixups = {
        # (blade_value, lock_chip_value) -> (new_blade, new_assist)
        # "Valkyrie Blast Wheel" already has lock_chip="Valkyrie", blade needs fixing
        "Valkyrie Blast Wheel": ("Blast", "Wheel"),
    }
    for blade_val, (new_blade, new_assist) in _assist_fixups.items():
        for i in [1, 2, 3]:
            blade_col = f"blade_{i}"
            assist_col = f"assist_{i}"
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {blade_col} = ?", [blade_val]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {blade_col} = ?, {assist_col} = COALESCE({assist_col}, ?) WHERE {blade_col} = ?",
                    [new_blade, new_assist, blade_val],
                )
                total_fixed += count

    # For "Umbra Hornet Assault": CX extraction set blade="Hornet", lock_chip="Umbra"
    # but "Assault" assist was lost. Fix assist for entries with Umbra lock chip + Hornet blade
    for i in [1, 2, 3]:
        blade_col = f"blade_{i}"
        lock_col = f"lock_chip_{i}"
        assist_col = f"assist_{i}"
        count = conn.execute(
            f"SELECT COUNT(*) FROM placements WHERE {blade_col} = 'Hornet' AND {lock_col} = 'Umbra' AND {assist_col} IS NULL"
        ).fetchone()[0]
        if count > 0:
            conn.execute(
                f"UPDATE placements SET {assist_col} = 'Assault' WHERE {blade_col} = 'Hornet' AND {lock_col} = 'Umbra' AND {assist_col} IS NULL"
            )
            total_fixed += count

    # Fix blade normalizations
    for wrong, correct in BLADE_NORMALIZATIONS.items():
        for col in ["blade_1", "blade_2", "blade_3"]:
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {col} = ?", [wrong]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {col} = ? WHERE {col} = ?", [correct, wrong]
                )
                total_fixed += count

    # Fix bit normalizations
    for wrong, correct in BIT_NORMALIZATIONS.items():
        for col in ["bit_1", "bit_2", "bit_3"]:
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {col} = ?", [wrong]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {col} = ? WHERE {col} = ?", [correct, wrong]
                )
                total_fixed += count

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

    # Clean up invalid lock chips - main blades should NOT be lock chips
    for i in [1, 2, 3]:
        lock_col = f"lock_chip_{i}"
        for blade in _cat_local.main_blades:
            count = conn.execute(
                f"SELECT COUNT(*) FROM placements WHERE {lock_col} = ?", [blade]
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    f"UPDATE placements SET {lock_col} = NULL WHERE {lock_col} = ?",
                    [blade],
                )
                total_fixed += count

    # Drift detection: scan placements for any tokens not in the wiki catalog
    # and mark them as 'pending' for admin review on /admin/new-parts
    try:
        from catalog import detect_drift
        drift_summary = detect_drift(conn)
        if drift_summary:
            print(f"  Drift detected: {drift_summary}")
    except Exception as e:
        print(f"  Warning: drift detection failed: {e}")

    # Smart drift resolver: ask RAGFlow to suggest canonical names for the
    # pending parts so the admin page can show one-click accept buttons.
    # No-op if RAGFlow isn't configured.
    try:
        from catalog import resolve_pending_with_ragflow
        suggested = resolve_pending_with_ragflow(conn)
        if suggested:
            print(f"  RAGFlow suggested canonical names for {suggested} pending parts")
    except Exception as e:
        print(f"  Warning: RAGFlow drift resolver failed: {e}")

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
    conn.execute("DROP TABLE IF EXISTS part_attributes")
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
