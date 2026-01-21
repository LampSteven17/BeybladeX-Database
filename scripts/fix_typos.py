"""
Fix typos and inconsistencies in the beyblade database.
"""

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "beyblade.duckdb"

# Mapping of typos to correct values
BLADE_FIXES = {
    "Aero Pegaus": "Aero Pegasus",
    "Roar Tyrnano": "Roar Tyranno",
    "Samauri Saber": "Samurai Saber",
    "Wizard Arrow.": "Wizard Arrow",
    "Wizardr Rod": "Wizard Rod",
    "phoenix": "Phoenix Wing",
    "uster": "Dran Buster",
    "|| KnightLance": "Knight Lance",
    "|| KnightShield": "Knight Shield",
    # Normalize incomplete entries
    "Samurai": "Samurai Saber",
    "Phoenix": "Phoenix Wing",
}

def fix_typos():
    conn = duckdb.connect(str(DB_PATH))

    print("Fixing blade typos...")

    for wrong, correct in BLADE_FIXES.items():
        # Count occurrences first
        count1 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_1 = ?", [wrong]
        ).fetchone()[0]
        count2 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_2 = ?", [wrong]
        ).fetchone()[0]
        count3 = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE blade_3 = ?", [wrong]
        ).fetchone()[0]

        total = count1 + count2 + count3
        if total > 0:
            # Fix blade_1
            conn.execute(
                "UPDATE placements SET blade_1 = ? WHERE blade_1 = ?",
                [correct, wrong]
            )
            # Fix blade_2
            conn.execute(
                "UPDATE placements SET blade_2 = ? WHERE blade_2 = ?",
                [correct, wrong]
            )
            # Fix blade_3
            conn.execute(
                "UPDATE placements SET blade_3 = ? WHERE blade_3 = ?",
                [correct, wrong]
            )
            print(f"  Fixed '{wrong}' -> '{correct}': {total} occurrences")

    # Verify fixes
    print("\n=== Remaining unique blades ===")
    blades = conn.execute("""
        SELECT DISTINCT blade FROM (
            SELECT blade_1 as blade FROM placements
            UNION ALL
            SELECT blade_2 FROM placements WHERE blade_2 IS NOT NULL
            UNION ALL
            SELECT blade_3 FROM placements WHERE blade_3 IS NOT NULL
        )
        ORDER BY blade
    """).fetchall()

    for b in blades:
        print(f"  {b[0]}")

    print(f"\nTotal unique blades: {len(blades)}")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    fix_typos()
