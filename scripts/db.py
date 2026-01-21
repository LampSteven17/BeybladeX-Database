"""
Database connection and schema management for BeybladeX Database.

Single source of truth: data/beyblade.duckdb
"""

import duckdb
from pathlib import Path

# Database path - single source of truth
DB_PATH = Path(__file__).parent.parent / "data" / "beyblade.duckdb"


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
    # CX-09 and variants using Eclipse main blade
    "Sol Eclipse": ("Sol", "Eclipse"),
    # CX-10 and variants using Hunt main blade
    "Wolf Hunt": ("Wolf", "Hunt"),
    # CX-11 and variants using Might main blade
    "Emperor Might": ("Emperor", "Might"),
    # CX-12 and variants using Flare main blade
    "Phoenix Flare": ("Phoenix", "Flare"),
    # Random Booster CX blades
    "Valkyrie Volt": ("Valkyrie", "Volt"),
}


def parse_cx_blade(blade_name: str) -> tuple[str | None, str]:
    """
    Parse a CX blade name into (lock_chip, main_blade).
    Returns (None, blade_name) if not a known CX blade.
    """
    if blade_name in CX_BLADE_COMPONENTS:
        return CX_BLADE_COMPONENTS[blade_name]
    return (None, blade_name)


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
    "Dran Sword": "BX",        # BX-01
    "Hells Scythe": "BX",      # BX-02
    "Wizard Arrow": "BX",      # BX-03
    "Knight Shield": "BX",     # BX-04
    "Knight Lance": "BX",      # BX-13
    "Leon Claw": "BX",         # BX-15
    "Shark Edge": "BX",        # BX-14 Random Booster
    "Viper Tail": "BX",        # BX-14 Random Booster
    "Dran Dagger": "BX",       # BX-14 Random Booster
    "Rhino Horn": "BX",        # BX-19
    "Phoenix Wing": "BX",      # BX-23
    "Hells Chain": "BX",       # BX-24 Random Booster
    "Unicorn Sting": "BX",     # BX-26
    "Black Shell": "BX",       # BX-24 Random Booster
    "Tyranno Beat": "BX",      # BX-24 Random Booster
    "Weiss Tiger": "BX",       # BX-33
    "Cobalt Dragoon": "BX",    # BX-34
    "Cobalt Drake": "BX",      # BX-31 Random Booster
    "Crimson Garuda": "BX",    # BX-38
    "Talon Ptera": "BX",       # BX-35 Random Booster
    "Roar Tyranno": "BX",      # BX-35 Random Booster
    "Sphinx Cowl": "BX",       # BX-35 Random Booster
    "Wyvern Gale": "BX",       # BX-35 Random Booster
    "Shelter Drake": "BX",     # BX-39
    "Tricera Press": "BX",     # BX-44
    "Samurai Calibur": "BX",   # BX-45
    "Bear Scratch": "BX",      # BX-48 Random Booster
    "Xeno Xcalibur": "BX",     # BXG-13
    "Chain Incendio": "BX",    # BX Random Booster
    "Scythe Incendio": "BX",   # BX Random Booster
    "Steel Samurai": "BX",     # BX
    "Optimus Primal": "BX",    # BX (Collab)
    "Bite Croc": "BX",         # BX (Hasbro exclusive)
    "Knife Shinobi": "BX",     # BX (Hasbro exclusive)
    "Venom": "BX",             # BX
    "Keel Shark": "BX",        # BX (Hasbro name for Shark Edge)
    "Whale Wave": "BX",        # BX
    "Gill Shark": "BX",        # BX (in CX-11 deck set but blade is BX)
    "Driger Slash": "BX",      # BX remake of classic Driger
    "Dragoon Storm": "BX",     # BX remake of classic Dragoon

    # ==========================================================================
    # UX Series (Unique Line) - More metal to perimeter, plastic interior hooks
    # ==========================================================================
    "Dran Buster": "UX",       # UX-01
    "Hells Hammer": "UX",      # UX-02
    "Wizard Rod": "UX",        # UX-03
    "Soar Phoenix": "UX",      # UX-04 Entry Set
    "Leon Crest": "UX",        # UX-06
    "Knight Mail": "UX",       # UX-07
    "Silver Wolf": "UX",       # UX-08
    "Samurai Saber": "UX",     # UX-09
    "Phoenix Feather": "UX",   # UX-10
    "Impact Drake": "UX",      # UX-11
    "Tusk Mammoth": "UX",      # UX-12 Random Booster
    "Phoenix Rudder": "UX",    # UX-12 Random Booster
    "Ghost Circle": "UX",      # UX-12 Random Booster
    "Golem Rock": "UX",        # UX-13
    "Scorpio Spear": "UX",     # UX-14
    "Shinobi Shadow": "UX",    # UX-15 Random Booster
    "Clock Mirage": "UX",      # UX-16
    "Meteor Dragoon": "UX",    # UX-17
    "Mummy Curse": "UX",       # UX-18 Random Booster
    "Dranzer Spiral": "UX",    # UX-12 Random Booster
    "Shark Scale": "UX",       # UX-15 Shark Scale Deck Set
    "Hover Wyvern": "UX",      # UX
    "Aero Pegasus": "UX",      # UX
    "Wand Wizard": "UX",       # UX Starter Pack

    # ==========================================================================
    # CX Series (Custom Line) - Main Blade names (lock chip stored separately)
    # After parsing: "Pegasus Blast" -> lock_chip="Pegasus", blade="Blast"
    # ==========================================================================
    # Main blade types (what gets stored in blade column after parsing)
    "Brave": "CX",             # CX-01 main blade
    "Arc": "CX",               # CX-02 main blade
    "Dark": "CX",              # CX-03 main blade
    "Reaper": "CX",            # CX-05 main blade
    "Brush": "CX",             # CX-06 main blade
    "Blast": "CX",             # CX-07 main blade
    "Eclipse": "CX",           # CX-09 main blade
    "Hunt": "CX",              # CX-10 main blade
    "Might": "CX",             # CX-11 main blade
    "Flare": "CX",             # CX-12 main blade
    "Volt": "CX",              # CX Random Booster main blade
    "Storm": "CX",             # CX Random Booster main blade
    "Emperor": "CX",           # CX main blade
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


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a connection to the database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


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
            blade_2 VARCHAR,
            ratchet_2 VARCHAR,
            bit_2 VARCHAR,
            assist_2 VARCHAR,
            lock_chip_2 VARCHAR,
            blade_3 VARCHAR,
            ratchet_3 VARCHAR,
            bit_3 VARCHAR,
            assist_3 VARCHAR,
            lock_chip_3 VARCHAR,
            UNIQUE(tournament_id, place)
        )
    """)

    # View: Flatten all combos
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
            p.lock_chip_1 as lock_chip
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
            p.lock_chip_2
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
            p.lock_chip_3
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

    # View: Full combo statistics
    conn.execute("""
        CREATE OR REPLACE VIEW combo_stats AS
        SELECT
            blade || ' ' || ratchet || bit as combo,
            blade,
            ratchet,
            bit,
            COUNT(*) as total_placements,
            SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as first_place_count,
            ROUND(SUM(CASE WHEN place = 1 THEN 1.0 ELSE 0 END) / COUNT(*), 3) as win_rate
        FROM combo_usage
        GROUP BY blade, ratchet, bit
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


def infer_region(country: str | None) -> str | None:
    """Infer region from country name."""
    if not country:
        return None
    return REGION_MAPPING.get(country.lower().strip())


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
    "Phoenix2ing": "Phoenix Wing",
    "phoenix": "Phoenix Wing",
    "Phoenix": "Phoenix Wing",
    "[ ] Phoenix Wing": "Phoenix Wing",
    # Typos - Hells Scythe
    "[ ] Hells Scythe": "Hells Scythe",
    # Typos - Other
    "Wizard Arrow.": "Wizard Arrow",
    "uster": "Dran Buster",
    "Silver Samurai": "Steel Samurai",
    "Fox Blast": "Fox Brush",
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
    # Inconsistent naming
    "Hex": "Hexa",
    # Common typos
    "FB": "Flat",  # Probably meant F (Flat)
}

# Ratchet normalizations (typos and invalid values)
RATCHET_NORMALIZATIONS = {
    # Invalid ratchets - 50 doesn't exist, probably meant 60
    "5-50": "5-60",
    "1-50": "1-60",
    "2-50": "2-60",
    "3-50": "3-60",
    "4-50": "4-60",
    "6-50": "6-60",
    "7-50": "7-60",
    "8-50": "8-60",
    "9-50": "9-60",
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
                "UPDATE placements SET blade_1 = ? WHERE blade_1 = ?",
                [correct, wrong]
            )

        # Count and fix blade_2
        count2 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_2 = ?", [wrong]
        ).fetchone()[0]
        if count2 > 0:
            conn.execute(
                "UPDATE placements SET blade_2 = ? WHERE blade_2 = ?",
                [correct, wrong]
            )

        # Count and fix blade_3
        count3 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_3 = ?", [wrong]
        ).fetchone()[0]
        if count3 > 0:
            conn.execute(
                "UPDATE placements SET blade_3 = ? WHERE blade_3 = ?",
                [correct, wrong]
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
                "UPDATE placements SET bit_1 = ? WHERE bit_1 = ?",
                [correct, wrong]
            )

        # Count and fix bit_2
        count2 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE bit_2 = ?", [wrong]
        ).fetchone()[0]
        if count2 > 0:
            conn.execute(
                "UPDATE placements SET bit_2 = ? WHERE bit_2 = ?",
                [correct, wrong]
            )

        # Count and fix bit_3
        count3 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE bit_3 = ?", [wrong]
        ).fetchone()[0]
        if count3 > 0:
            conn.execute(
                "UPDATE placements SET bit_3 = ? WHERE bit_3 = ?",
                [correct, wrong]
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
                    f"UPDATE placements SET {col} = ? WHERE {col} = ?",
                    [correct, wrong]
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
                    f"UPDATE placements SET {col} = ? WHERE {col} = ?",
                    [correct, wrong]
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
