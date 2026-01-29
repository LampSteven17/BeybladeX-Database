"""
Import official Beyblade X Championship data.

This script imports data from major official tournaments:
- Beyblade X World Championship 2025
- Beyblade X Asia Championship 2024

Data is stored in a JSON file and imported into the database.
Each player can have up to 3 combos in their deck.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection, init_schema, infer_region

# Championship data file
DATA_FILE = Path(__file__).parent.parent / "data" / "championships.json"

# Default championship data
DEFAULT_DATA = {
    "tournaments": [
        {
            "id": "wc2025_regular",
            "name": "Beyblade X World Championship 2025 - Regular Class",
            "date": "2025-10-12",
            "location": "Tokyo, Japan",
            "region": "JAPAN",  # Held in Japan, but players from worldwide
            "format": "3on3 Battle, First to 4 points",
            "class": "Regular (Ages 6-12)",
            "placements": [
                {
                    "place": 1,
                    "player": "Leobardo",
                    "player_region": "Mexico City, Mexico",
                    "combos": [
                        {"blade": "Aero Pegasus", "ratchet": "7-60", "bit": "Level"},
                        {"blade": "Wizard Rod", "ratchet": "1-60", "bit": "Hexa"},
                        {"blade": "Shark Scale", "ratchet": "3-60", "bit": "Low Rush"}
                    ]
                },
                {
                    "place": 2,
                    "player": "Balya",
                    "player_region": "Jakarta, Indonesia",
                    "combos": [
                        {"blade": "Cobalt Dragoon", "ratchet": "5-60", "bit": "Elevate"},
                        {"blade": "Wizard Rod", "ratchet": "1-60", "bit": "Hexa"},
                        {"blade": "Shark Scale", "ratchet": "1-70", "bit": "Low Rush"}
                    ]
                },
                {
                    "place": 3,
                    "player": "Berguiny",
                    "player_region": "Paris, France",
                    "combos": [
                        {"blade": "Hover Wyvern", "ratchet": "7-60", "bit": "Low Rush"},
                        {"blade": "Wizard Rod", "ratchet": "9-70", "bit": "Ball"},
                        {"blade": "Valkyrie Blast Wheel", "ratchet": "9-60", "bit": "Free Ball", "lock_chip": "Valkyrie"}
                    ]
                },
                {
                    "place": 4,
                    "player": "Kim Jung U",
                    "player_region": "Seoul, South Korea",
                    "combos": []  # Combos not documented
                }
            ]
        },
        {
            "id": "wc2025_open",
            "name": "Beyblade X World Championship 2025 - Open Class",
            "date": "2025-10-12",
            "location": "Tokyo, Japan",
            "region": "JAPAN",
            "format": "3on3 Battle, First to 4 points",
            "class": "Open (Ages 6+)",
            "placements": [
                {
                    "place": 1,
                    "player": "Fahreddin",
                    "player_region": "Istanbul, Turkey",
                    "combos": []  # Combos not publicly documented yet
                },
                {
                    "place": 2,
                    "player": "Kyle",
                    "player_region": "Hong Kong",
                    "combos": []
                },
                {
                    "place": 3,
                    "player": "Omanju King",
                    "player_region": "Tokyo, Japan",
                    "combos": []
                },
                {
                    "place": 4,
                    "player": "Yoo Ha Jun",
                    "player_region": "Seoul, South Korea",
                    "combos": []
                }
            ]
        },
        {
            "id": "asia2024_regular",
            "name": "Beyblade X Asia Championship 2024 - Regular Class",
            "date": "2024-12-01",
            "location": "Tokyo, Japan",
            "region": "JAPAN",
            "format": "3on3 Battle, First to 4 points",
            "class": "Regular (Ages 6-12)",
            "placements": [
                {
                    "place": 1,
                    "player": "Sousuke",
                    "player_region": "Tokyo, Japan",
                    "combos": []  # Combos not documented
                },
                {
                    "place": 2,
                    "player": "Emmanuel Marcelino",
                    "player_region": "Manila, Philippines",
                    "combos": []
                },
                {
                    "place": 3,
                    "player": "Min Seo Jo",
                    "player_region": "Seoul, South Korea",
                    "combos": []
                }
            ]
        },
        {
            "id": "asia2024_open",
            "name": "Beyblade X Asia Championship 2024 - Open Class",
            "date": "2024-12-01",
            "location": "Tokyo, Japan",
            "region": "JAPAN",
            "format": "3on3 Battle, First to 4 points",
            "class": "Open (Ages 6+)",
            "placements": [
                {
                    "place": 1,
                    "player": "Zane",
                    "player_region": "Singapore",
                    "combos": [
                        {"blade": "Wizard Rod", "ratchet": "9-60", "bit": "Free Ball"}
                    ]  # Partial combo info from controversy discussion
                },
                {
                    "place": 2,
                    "player": "Mi Hee Youn",
                    "player_region": "Seoul, South Korea",
                    "combos": []
                },
                {
                    "place": 3,
                    "player": "Jerry",
                    "player_region": "Taipei, Taiwan",
                    "combos": []
                }
            ]
        }
    ]
}


def init_data_file():
    """Create the data file with default data if it doesn't exist."""
    if not DATA_FILE.exists():
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_DATA, f, indent=2, ensure_ascii=False)
        print(f"Created {DATA_FILE} with default championship data")
        return DEFAULT_DATA
    else:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)


def clear_championship_data(conn) -> int:
    """Clear all championship data from the database."""
    # Championship tournaments have wbo_post_id starting with 'champ_'
    count = conn.execute("""
        SELECT COUNT(*) FROM tournaments
        WHERE wbo_post_id LIKE 'champ_%'
    """).fetchone()[0]

    conn.execute("""
        DELETE FROM placements WHERE tournament_id IN (
            SELECT id FROM tournaments WHERE wbo_post_id LIKE 'champ_%'
        )
    """)
    conn.execute("DELETE FROM tournaments WHERE wbo_post_id LIKE 'champ_%'")
    conn.commit()

    return count


def import_championships(conn, data: dict, verbose: bool = False) -> tuple[int, int]:
    """
    Import championship data into the database.

    Returns:
        Tuple of (tournaments_added, placements_added)
    """
    tournaments_added = 0
    placements_added = 0

    for tournament in data.get("tournaments", []):
        tournament_id_str = f"champ_{tournament['id']}"

        # Check if already exists
        existing = conn.execute(
            "SELECT id FROM tournaments WHERE wbo_post_id = ?",
            [tournament_id_str]
        ).fetchone()

        if existing:
            if verbose:
                print(f"  Skipping {tournament['name']} (already exists)")
            continue

        # Parse date
        try:
            date = datetime.strptime(tournament['date'], '%Y-%m-%d')
        except ValueError:
            print(f"  Error: Invalid date format for {tournament['name']}")
            continue

        # Insert tournament
        result = conn.execute("""
            INSERT INTO tournaments (wbo_post_id, name, date, region, format)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
        """, [
            tournament_id_str,
            tournament['name'],
            date.strftime('%Y-%m-%d'),
            tournament.get('region', 'JAPAN'),
            tournament.get('format')
        ])

        db_tournament_id = result.fetchone()[0]
        tournaments_added += 1

        if verbose:
            print(f"  Added tournament: {tournament['name']}")

        # Insert placements
        for placement in tournament.get("placements", []):
            combos = placement.get("combos", [])

            # Skip placements without any combo data
            if not combos:
                if verbose:
                    print(f"    {placement['place']}. {placement['player']} (skipped - no combo data)")
                continue

            # Pad combos to 3 entries
            while len(combos) < 3:
                combos.append({})

            try:
                conn.execute("""
                    INSERT INTO placements (
                        tournament_id, place, player_name,
                        blade_1, ratchet_1, bit_1, lock_chip_1,
                        blade_2, ratchet_2, bit_2, lock_chip_2,
                        blade_3, ratchet_3, bit_3, lock_chip_3
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    db_tournament_id,
                    placement['place'],
                    placement['player'],
                    combos[0].get('blade'),
                    combos[0].get('ratchet'),
                    combos[0].get('bit'),
                    combos[0].get('lock_chip'),
                    combos[1].get('blade'),
                    combos[1].get('ratchet'),
                    combos[1].get('bit'),
                    combos[1].get('lock_chip'),
                    combos[2].get('blade'),
                    combos[2].get('ratchet'),
                    combos[2].get('bit'),
                    combos[2].get('lock_chip'),
                ])
                placements_added += 1
                if verbose:
                    combo_strs = [
                        f"{c.get('blade')} {c.get('ratchet')} {c.get('bit')}"
                        for c in combos if c.get('blade')
                    ]
                    print(f"    {placement['place']}. {placement['player']}: {', '.join(combo_strs)}")
            except Exception as e:
                print(f"    Error inserting {placement['player']}: {e}")

        conn.commit()

    return tournaments_added, placements_added


def get_stats(conn) -> dict:
    """Get statistics for championship data."""
    tournaments = conn.execute("""
        SELECT COUNT(*) FROM tournaments WHERE wbo_post_id LIKE 'champ_%'
    """).fetchone()[0]

    placements = conn.execute("""
        SELECT COUNT(*) FROM placements p
        JOIN tournaments t ON p.tournament_id = t.id
        WHERE t.wbo_post_id LIKE 'champ_%'
    """).fetchone()[0]

    return {
        "source": "Championships",
        "tournaments": tournaments,
        "placements": placements,
    }


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Import Beyblade X Championship data")
    parser.add_argument("--clear", action="store_true", help="Clear existing championship data first")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    parser.add_argument("--stats", action="store_true", help="Show statistics only")
    parser.add_argument("--init", action="store_true", help="Initialize/reset the data file")
    args = parser.parse_args()

    conn = get_connection()
    init_schema(conn)

    if args.init:
        if DATA_FILE.exists():
            DATA_FILE.unlink()
        init_data_file()
        print("Data file initialized with default championship data")
        print(f"Edit {DATA_FILE} to add more tournaments or update combos")
        return

    if args.stats:
        stats = get_stats(conn)
        print(f"Championships: {stats['tournaments']} tournaments, {stats['placements']} placements")
        return

    # Load data
    data = init_data_file()

    if args.clear:
        cleared = clear_championship_data(conn)
        print(f"Cleared {cleared} existing championship tournaments")

    print("Importing championship data...")
    tournaments, placements = import_championships(conn, data, verbose=args.verbose)

    print(f"\nImported {tournaments} tournaments, {placements} placements")

    # Show final stats
    stats = get_stats(conn)
    print(f"Total: {stats['tournaments']} tournaments, {stats['placements']} placements")

    conn.close()


if __name__ == "__main__":
    main()
