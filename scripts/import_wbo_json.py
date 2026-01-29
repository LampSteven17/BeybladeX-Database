"""
Import WBO data from JSON file scraped by wbo_scraper_windows.py
"""

import json
import re
from pathlib import Path
from db import get_connection, init_schema, normalize_data

DATA_FILE = Path(__file__).parent.parent / "data" / "wbo_data.json"


def normalize_blade_name(blade: str) -> str:
    """Normalize blade name - add spaces to CamelCase like 'WizardRod' -> 'Wizard Rod'"""
    if not blade:
        return blade
    if " " in blade:
        return blade
    return re.sub(r"([a-z])([A-Z])", r"\1 \2", blade)


def normalize_bit_name(bit: str) -> str:
    """Normalize bit name - expand abbreviations and add spaces"""
    if not bit:
        return bit

    expansions = {
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
    }

    if bit in expansions:
        return expansions[bit]

    if " " not in bit:
        bit = re.sub(r"([a-z])([A-Z])", r"\1 \2", bit)

    return bit


def main():
    print("=" * 60)
    print("Importing WBO data from JSON")
    print("=" * 60)

    # Load JSON
    print(f"\nLoading {DATA_FILE}...")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        tournaments = json.load(f)

    print(f"Found {len(tournaments)} tournaments in JSON")

    # Connect to database
    conn = get_connection()
    init_schema(conn)

    saved = 0
    skipped = 0
    total_placements = 0

    for tournament in tournaments:
        wbo_post_id = tournament.get("wbo_post_id", "")

        # Check if already exists
        existing = conn.execute(
            "SELECT 1 FROM tournaments WHERE wbo_post_id = ?", [wbo_post_id]
        ).fetchone()

        if existing:
            skipped += 1
            continue

        # Insert tournament
        name = tournament.get("name", "Unknown Tournament")
        date = tournament.get("date") or "2024-01-01"  # Default date if missing

        conn.execute(
            """
            INSERT INTO tournaments (wbo_post_id, name, date)
            VALUES (?, ?, ?)
        """,
            [wbo_post_id, name, date],
        )

        # Get the tournament ID we just inserted
        tournament_id = conn.execute(
            "SELECT id FROM tournaments WHERE wbo_post_id = ?", [wbo_post_id]
        ).fetchone()[0]

        # Insert placements (combos are inline in placements table)
        # Track seen places to handle duplicate place numbers (multiple tournaments in one post)
        seen_places = set()
        place_offset = 0

        for placement in tournament.get("placements", []):
            place = placement.get("place", 0)
            player = placement.get("player", "Unknown")
            combos = placement.get("combos", [])

            # If we've seen this place before, it's a new "tournament" within the post
            # Offset the places to make them unique
            if place in seen_places:
                place_offset += 100  # Add offset for "sub-tournament"
                seen_places.clear()

            seen_places.add(place)
            actual_place = place + place_offset

            # Extract up to 3 combos and normalize names
            blade_1 = (
                normalize_blade_name(combos[0].get("blade", ""))
                if len(combos) > 0
                else ""
            )
            ratchet_1 = combos[0].get("ratchet", "") if len(combos) > 0 else ""
            bit_1 = (
                normalize_bit_name(combos[0].get("bit", "")) if len(combos) > 0 else ""
            )
            lock_chip_1 = combos[0].get("lock_chip") if len(combos) > 0 else None
            assist_1 = combos[0].get("assist") if len(combos) > 0 else None

            blade_2 = (
                normalize_blade_name(combos[1].get("blade"))
                if len(combos) > 1
                else None
            )
            ratchet_2 = combos[1].get("ratchet") if len(combos) > 1 else None
            bit_2 = (
                normalize_bit_name(combos[1].get("bit")) if len(combos) > 1 else None
            )
            lock_chip_2 = combos[1].get("lock_chip") if len(combos) > 1 else None
            assist_2 = combos[1].get("assist") if len(combos) > 1 else None

            blade_3 = (
                normalize_blade_name(combos[2].get("blade"))
                if len(combos) > 2
                else None
            )
            ratchet_3 = combos[2].get("ratchet") if len(combos) > 2 else None
            bit_3 = (
                normalize_bit_name(combos[2].get("bit")) if len(combos) > 2 else None
            )
            lock_chip_3 = combos[2].get("lock_chip") if len(combos) > 2 else None
            assist_3 = combos[2].get("assist") if len(combos) > 2 else None

            # Skip if no valid combo
            if not blade_1 or not ratchet_1 or not bit_1:
                continue

            conn.execute(
                """
                INSERT INTO placements (
                    tournament_id, place, player_name,
                    blade_1, ratchet_1, bit_1, lock_chip_1, assist_1,
                    blade_2, ratchet_2, bit_2, lock_chip_2, assist_2,
                    blade_3, ratchet_3, bit_3, lock_chip_3, assist_3
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    tournament_id,
                    actual_place,
                    player,
                    blade_1,
                    ratchet_1,
                    bit_1,
                    lock_chip_1,
                    blade_2,
                    ratchet_2,
                    bit_2,
                    lock_chip_2,
                    blade_3,
                    ratchet_3,
                    bit_3,
                    lock_chip_3,
                ],
            )
            total_placements += 1

        saved += 1

    # Run normalization to fix typos
    print("\nNormalizing data (fixing typos)...")
    fixed = normalize_data(conn)
    print(f"Fixed {fixed} records")

    conn.commit()
    conn.close()

    print()
    print("=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"New tournaments imported: {saved}")
    print(f"Duplicates skipped: {skipped}")
    print(f"Total placements added: {total_placements}")


if __name__ == "__main__":
    main()
