"""
Seed the parts table with canonical Beyblade X parts.

This is the reference list for normalizing scraped data.
Add new parts here as they release.
"""

from db import get_connection, init_schema

# Blades - (name, spin_direction, series, notes)
BLADES = [
    # BX Series - Right Spin
    ("Dran Sword", "right", "BX", "Starter blade"),
    ("Hell Scythe", "right", "BX", "Attack type"),
    ("Knight Shield", "right", "BX", "Defense type"),
    ("Wizard Arrow", "right", "BX", "Stamina type"),
    ("Shark Edge", "right", "BX", "Attack type"),
    ("Knight Lance", "right", "BX", "Defense type"),
    ("Hells Hammer", "right", "BX", "Attack type"),
    ("Phoenix Wing", "right", "BX", "Heavy attack, meta dominant"),
    ("Leon Claw", "right", "BX", "Balance type"),
    ("Viper Tail", "right", "BX", "Stamina type"),
    ("Tyranno Beat", "right", "BX", "Attack type"),
    ("Rhino Horn", "right", "BX", "Defense type"),
    ("Shark Slayer", "right", "BX", "Attack type"),
    ("Dran Dagger", "right", "BX", "Attack type"),
    ("Wizard Rod", "right", "BX", "Stamina/balance, versatile"),
    ("Hells Chain", "right", "BX", "Attack type"),
    ("Phoenix Rudder", "right", "BX", "Stamina type"),
    ("Black Shell", "right", "BX", "Defense type"),
    ("Prominence Phoenix", "right", "BX", "Special attack"),
    ("Steel Samurai", "right", "BX", None),
    ("Burn Wyvern", "right", "BX", None),

    # BX Series - Left Spin
    ("Cobalt Dragoon", "left", "BX", "Only left-spin, meta-warping"),
    ("Cobalt Drake", "left", "BX", "Left-spin attack"),

    # UX Series
    ("Dran Buster", "right", "UX", "UX powerhouse"),
    ("Hells Scythe", "right", "UX", "UX attack"),
    ("Tusk Mammoth", "right", "UX", None),
    ("Aero Pegasus", "right", "UX", None),
    ("Spike Cadeus", "right", "UX", None),
    ("Weiss Tiger", "right", "UX", None),
    ("Talon Ptera", "right", "UX", None),
]

# Ratchets - (name, series, notes)
# Format: [prongs]-[height] e.g., "9-60" = 9 prongs, 60 height
RATCHETS = [
    ("1-60", "BX", "Low CoG, attack alignment"),
    ("2-60", "BX", None),
    ("3-60", "BX", "Attack-oriented height"),
    ("3-70", "BX", None),
    ("3-80", "BX", None),
    ("4-60", "BX", None),
    ("4-70", "BX", None),
    ("4-80", "BX", None),
    ("5-60", "BX", "Balanced weight and stability"),
    ("5-70", "BX", "Heavy weight option"),
    ("5-80", "BX", None),
    ("6-60", "BX", None),
    ("6-80", "BX", None),
    ("9-60", "BX", "High burst resistance, stamina focus"),
    ("9-70", "BX", None),
    ("9-80", "BX", None),
    # UX Ratchets
    ("1-55", "UX", None),
    ("3-85", "UX", None),
    ("4-55", "UX", None),
    ("4-85", "UX", None),
    ("5-55", "UX", None),
    ("7-60", "UX", None),
]

# Bits - (name, series, notes)
BITS = [
    # Standard bits
    ("Ball", "BX", "Consistent stamina"),
    ("Flat", "BX", "Aggressive movement"),
    ("Point", "BX", "Aggressive early game"),
    ("Needle", "BX", "Stamina focus"),
    ("Spike", "BX", None),
    ("Taper", "BX", None),
    ("Rush", "BX", None),
    ("Hexa", "BX", None),
    ("Orb", "BX", None),
    ("Dot", "BX", None),
    ("Accel", "BX", None),
    ("Cyclone", "BX", None),

    # High performance bits
    ("High Needle", "BX", "HN - Higher stamina"),
    ("Level", "BX", "Great stamina, high burst resistance"),
    ("Elevate", "BX", "Best LAD, enables equalisation"),
    ("GearBall", "BX", "GB - Versatile"),
    ("GearFlat", "BX", "GF - Versatile attack/stamina"),
    ("GearNeedle", "BX", "GN"),
    ("GearPoint", "BX", "GP"),
    ("Low Flat", "BX", "LF - Low attack"),
    ("Low Rush", "BX", "LR - Speed-based movement"),
    ("Low Needle", "BX", "LN"),
    ("Metal Needle", "BX", "MN"),
    ("High Taper", "BX", "HT"),
    ("High Accel", "BX", "HA"),
    ("Disc Ball", "BX", "DB"),
    ("Unite", "BX", None),
    ("Brake", "BX", None),
    ("Bound", "BX", None),

    # UX Bits
    ("Spiral Needle", "UX", "SN"),
    ("High Sword", "UX", "HS"),
]


def seed_parts():
    """Insert all parts into the database."""
    conn = get_connection()
    init_schema(conn)

    # Clear existing parts
    conn.execute("DELETE FROM parts")

    # Insert blades
    for name, spin, series, notes in BLADES:
        conn.execute(
            "INSERT INTO parts (name, type, spin_direction, series, notes) VALUES (?, 'blade', ?, ?, ?)",
            [name, spin, series, notes]
        )

    # Insert ratchets
    for name, series, notes in RATCHETS:
        conn.execute(
            "INSERT INTO parts (name, type, series, notes) VALUES (?, 'ratchet', ?, ?)",
            [name, series, notes]
        )

    # Insert bits
    for name, series, notes in BITS:
        conn.execute(
            "INSERT INTO parts (name, type, series, notes) VALUES (?, 'bit', ?, ?)",
            [name, series, notes]
        )

    conn.commit()
    conn.close()

    print(f"Seeded {len(BLADES)} blades, {len(RATCHETS)} ratchets, {len(BITS)} bits")


def show_parts():
    """Display all parts in the database."""
    conn = get_connection()

    print("\n=== BLADES ===")
    blades = conn.execute(
        "SELECT name, spin_direction, series FROM parts WHERE type = 'blade' ORDER BY series, name"
    ).fetchall()
    for name, spin, series in blades:
        print(f"  {name} ({spin}, {series})")

    print("\n=== RATCHETS ===")
    ratchets = conn.execute(
        "SELECT name, series FROM parts WHERE type = 'ratchet' ORDER BY name"
    ).fetchall()
    for name, series in ratchets:
        print(f"  {name} ({series})")

    print("\n=== BITS ===")
    bits = conn.execute(
        "SELECT name, series FROM parts WHERE type = 'bit' ORDER BY name"
    ).fetchall()
    for name, series in bits:
        print(f"  {name} ({series})")

    conn.close()


if __name__ == "__main__":
    seed_parts()
    show_parts()
