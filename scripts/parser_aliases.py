"""
Parser-only aliases — the small set of name variants that are NOT on the wiki.

The wiki gives us authoritative full part names (Flat, Slash, Pegasus Blast, etc.).
Tournament organizers also use:
  - single-letter abbreviations (F=Flat, S=Slash)
  - two-letter abbreviations (HN=High Needle, GB=Gear Ball)
  - typo variants (Rrush=Rush)

These mappings are stable and tournament-format-specific (not part-release-specific),
so they live here as constants instead of in the database. Adding a NEW PART does NOT
require touching this file — only adding a new abbreviation convention does.

Loaded by scripts/catalog.py and merged into the catalog's alias maps.
"""

# Single-letter bit abbreviations.
# IMPORTANT: some letters overlap with assist abbreviations (S, B, H, M, Z, W, F, D).
# Context determines meaning: in a CX combo, a letter BEFORE the ratchet is an assist;
# AFTER the ratchet it's a bit.
BIT_LETTER_ABBREVIATIONS = {
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
    "V": "Vortex",
    "W": "Wedge",
    "Z": "Zap",
}

# Multi-letter bit abbreviations (still ambiguous-free unlike single letters).
BIT_SHORT_ABBREVIATIONS = {
    "HN": "High Needle",
    "HT": "High Taper",
    "LF": "Low Flat",
    "LR": "Low Rush",
    "LO": "Low Orb",
    "GF": "Gear Flat",
    "GB": "Gear Ball",
    "GN": "Gear Needle",
    "GP": "Gear Point",
    "GR": "Gear Rush",
    "MN": "Metal Needle",
    "DB": "Disc Ball",
    "DS": "Disc Spike",
    "FB": "Free Ball",
    "FF": "Free Flat",
    "WB": "Wall Ball",
    "WW": "Wall Wedge",
    "BS": "Bound Spike",
    "TK": "Trans Kick",
    "TP": "Trans Point",
    "UF": "Under Flat",
    "UN": "Under Needle",
    "RA": "Rubber Accel",
    "Lv": "Level",
    "El": "Elevate",
    "Un": "Unite",
    "Gl": "Glide",
    "Hex": "Hexa",
}

# Tournament data typos and weird spellings — fix on the way in.
BIT_TYPO_FIXES = {
    "Rrush": "Rush",
}

# Single-letter assist blade abbreviations (CX-only).
ASSIST_LETTER_ABBREVIATIONS = {
    "S": "Slash",
    "B": "Bumper",
    "J": "Jaggy",
    "R": "Round",
    "T": "Turn",
    "C": "Charge",
    "M": "Massive",
    "H": "Heavy",
    "Z": "Zillion",
    "W": "Wheel",
    "F": "Free",
    "D": "Dual",
    # Newer ones
    "A": "Assault",
    "E": "Erase",
    "K": "Knuckle",
    "V": "Vertical",
}

# Lock chips that aren't yet in the wiki Category but appear in some sources
# (e.g., Hasbro localizations or hidden vendor variants). Empty by default;
# fill in only when we know the wiki is missing one.
EXTRA_LOCK_CHIPS: dict[str, str] = {}

# Collab / crossover / Jurassic World skin blades that are competitively
# identical (or near-identical retools) of standard BX blades. These get
# folded into the canonical base blade so rankings and combo aggregations
# don't fragment across cosmetic variants.
#
# Source: each wiki page describes the skin as "identical to those of [Base]"
# or "retool of [Base], plastic components identical". A few (Red Hulk,
# Captain America) have retooled metal discs but identical plastic — close
# enough for tournament meta analysis.
#
# Updated manually when new collabs are announced. The fandom scraper's
# _upsert_catalog_part preserves status='rejected' rows, so these persist
# across wiki bookmarklet refreshes.
SKIN_ALIASES: dict[str, str] = {
    # Transformers collabs
    "Bumblebee": "Knight Shield",
    "Megatron": "Hells Scythe",
    "Shockwave": "Viper Tail",
    "Optimus Primal": "Shark Edge",
    # Star Wars collabs
    "Darth Vader": "Knight Lance",
    "General Grievous": "Rhino Horn",
    "Moff Gideon": "Hells Scythe",
    "Obi-Wan Kenobi": "Knight Lance",
    "Chewbacca": "Shelter Drake",
    # Marvel collabs
    "Venom": "Dran Sword",
    "Red Hulk": "Tyranno Beat",       # retool, plastics identical
    "Spider-Man": "Viper Tail",
    "Iron Man": "Knight Shield",
    "Captain America": "Dran Sword",   # retool, plastics identical
    # Jurassic World collabs
    "Mosasaurus": "Sphinx Cowl",
    "Quetzalcoatlus": "Viper Tail",
    "Spinosaurus": "Roar Tyranno",
    "Tyrannosaurus": "Roar Tyranno",
    "T-Rex": "Tyranno Beat",          # wiki: T._Rex_1-80GB
}
