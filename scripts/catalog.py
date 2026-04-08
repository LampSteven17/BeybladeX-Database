"""
PartsCatalog — single source of truth for valid Beyblade X parts.

Loads the parts_catalog table from DuckDB once per process, builds in-memory
lookup sets and alias maps, and exposes them to the parsers.

Usage:
    from catalog import PartsCatalog
    cat = PartsCatalog.get()  # singleton, lazy-loaded
    if name in cat.lock_chips: ...
    full_bit = cat.bit_aliases.get("F", "F")  # 'Flat'

After running the fandom scraper, call PartsCatalog.reload() to pick up new
wiki entries without restarting the process.

The catalog is populated by scripts/scrapers/fandom.py:populate_parts_catalog,
which discovers parts from Fandom wiki Category pages.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import parser_aliases as PA


# Where to emit the JSON catalog for the frontend
CATALOG_JSON_PATH = (
    Path(__file__).parent.parent / "site" / "public" / "data" / "parts_catalog.json"
)


@dataclass
class PartsCatalog:
    # Sets of accepted (canonical) names by part type
    lock_chips: set[str] = field(default_factory=set)
    main_blades: set[str] = field(default_factory=set)
    metal_blades: set[str] = field(default_factory=set)
    blades: set[str] = field(default_factory=set)              # BX/UX standalone blades
    assists: set[str] = field(default_factory=set)
    over_blades: set[str] = field(default_factory=set)
    bits: set[str] = field(default_factory=set)
    ratchets: set[str] = field(default_factory=set)

    # Lower-case lookup maps (case-insensitive matching)
    lock_chips_lower: dict[str, str] = field(default_factory=dict)
    main_blades_lower: dict[str, str] = field(default_factory=dict)
    blades_lower: dict[str, str] = field(default_factory=dict)
    assists_lower: dict[str, str] = field(default_factory=dict)
    over_blades_lower: dict[str, str] = field(default_factory=dict)
    bits_lower: dict[str, str] = field(default_factory=dict)

    # Alias maps (abbreviations + no-space variants -> canonical)
    bit_aliases: dict[str, str] = field(default_factory=dict)
    assist_aliases: dict[str, str] = field(default_factory=dict)
    blade_aliases: dict[str, str] = field(default_factory=dict)

    # Metadata
    metal_lock_chips: set[str] = field(default_factory=set)
    series_for_blade: dict[str, str] = field(default_factory=dict)  # blade -> 'BX'|'UX'|'CX'

    # The set of full CX combo names ("Pegasus Blast") computed lazily
    cx_full_names: set[str] = field(default_factory=set)

    # Singleton state
    _instance: Optional["PartsCatalog"] = None

    @classmethod
    def get(cls) -> "PartsCatalog":
        """Return the singleton, loading from the database on first call.

        Uses a write connection (not read-only) so it doesn't conflict with
        a writable connection that may already be open in the same process
        — DuckDB raises if you mix read-only and write modes against the
        same file.
        """
        if cls._instance is None:
            from db import get_connection  # local import to avoid cycles
            conn = get_connection()
            try:
                cls._instance = cls.load(conn)
            finally:
                conn.close()
        return cls._instance

    @classmethod
    def reload(cls) -> "PartsCatalog":
        """Force a reload from the database (call after fandom scrape)."""
        cls._instance = None
        return cls.get()

    @classmethod
    def load(cls, conn) -> "PartsCatalog":
        """Build the catalog from the parts_catalog table."""
        cat = cls()

        # Pull all accepted parts
        try:
            rows = conn.execute("""
                SELECT name, part_type, system, metal
                FROM parts_catalog
                WHERE status = 'accepted'
            """).fetchall()
        except Exception:
            # Table doesn't exist yet (init_schema not run)
            rows = []

        for name, part_type, system, metal in rows:
            if part_type == "lock_chip":
                cat.lock_chips.add(name)
                if metal:
                    cat.metal_lock_chips.add(name)
            elif part_type == "main_blade":
                cat.main_blades.add(name)
            elif part_type == "metal_blade":
                cat.metal_blades.add(name)
            elif part_type == "blade":
                cat.blades.add(name)
                if system:
                    cat.series_for_blade[name] = system
            elif part_type == "assist":
                cat.assists.add(name)
            elif part_type == "over_blade":
                cat.over_blades.add(name)
            elif part_type == "bit":
                cat.bits.add(name)
            elif part_type == "ratchet":
                cat.ratchets.add(name)

        # Build lower-case lookup maps
        cat.lock_chips_lower = {n.lower(): n for n in cat.lock_chips}
        cat.main_blades_lower = {n.lower(): n for n in cat.main_blades}
        cat.blades_lower = {n.lower(): n for n in cat.blades}
        cat.assists_lower = {n.lower(): n for n in cat.assists}
        cat.over_blades_lower = {n.lower(): n for n in cat.over_blades}
        cat.bits_lower = {n.lower(): n for n in cat.bits}

        # Compute the set of full CX combo names (LockChip + MainBlade)
        for chip in cat.lock_chips:
            for main in cat.main_blades:
                cat.cx_full_names.add(f"{chip} {main}")
            for metal in cat.metal_blades:
                cat.cx_full_names.add(f"{chip} {metal}")
        # Standalone CX metal blades also act as full blade names
        cat.cx_full_names |= cat.metal_blades
        # All CX full names get series='CX'
        for name in cat.cx_full_names:
            cat.series_for_blade.setdefault(name, "CX")

        # Build bit alias map: abbreviations + no-space + every full name maps to itself
        cat.bit_aliases = {}
        for full in cat.bits:
            cat.bit_aliases[full] = full
            no_space = full.replace(" ", "")
            if no_space != full:
                cat.bit_aliases[no_space] = full
        cat.bit_aliases.update(PA.BIT_LETTER_ABBREVIATIONS)
        cat.bit_aliases.update(PA.BIT_SHORT_ABBREVIATIONS)
        cat.bit_aliases.update(PA.BIT_TYPO_FIXES)
        # Make sure abbreviations don't override full names if collision
        for full in cat.bits:
            cat.bit_aliases[full] = full

        # Build assist alias map similarly
        cat.assist_aliases = {}
        for full in cat.assists:
            cat.assist_aliases[full] = full
            no_space = full.replace(" ", "")
            if no_space != full:
                cat.assist_aliases[no_space] = full
        cat.assist_aliases.update(PA.ASSIST_LETTER_ABBREVIATIONS)

        # Build blade alias map: full CX names + no-space variants
        cat.blade_aliases = {}
        for full in cat.blades | cat.cx_full_names:
            cat.blade_aliases[full] = full
            no_space = full.replace(" ", "")
            if no_space != full:
                cat.blade_aliases[no_space] = full

        return cat

    # ------------------------------------------------------------------
    # Drift detection: record an unknown part seen in tournament data
    # ------------------------------------------------------------------
    def is_unknown(self, name: str, part_type: str) -> bool:
        """Return True if `name` is not in the catalog for the given part_type."""
        if not name:
            return False
        sets_by_type = {
            "lock_chip": self.lock_chips,
            "main_blade": self.main_blades,
            "metal_blade": self.metal_blades,
            "blade": self.blades | self.cx_full_names,
            "assist": self.assists,
            "over_blade": self.over_blades,
            "bit": self.bits,
            "ratchet": self.ratchets,
        }
        return name not in sets_by_type.get(part_type, set())


def record_drift(conn, name: str, part_type: str, sample_combo: Optional[str] = None) -> None:
    """Record an unknown part seen in tournament data as 'pending' for review.

    Idempotent: re-recording the same part increments occurrence_count.
    """
    if not name or not part_type:
        return
    existing = conn.execute(
        "SELECT name, status FROM parts_catalog WHERE name = ? AND part_type = ?",
        [name, part_type],
    ).fetchone()
    if existing:
        # Don't overwrite an accepted entry
        if existing[1] == "accepted":
            return
        conn.execute("""
            UPDATE parts_catalog
            SET occurrence_count = COALESCE(occurrence_count, 0) + 1,
                sample_combo = COALESCE(sample_combo, ?)
            WHERE name = ? AND part_type = ? AND status = 'pending'
        """, [sample_combo, name, part_type])
    else:
        conn.execute("""
            INSERT INTO parts_catalog
                (name, part_type, status, source, sample_combo, occurrence_count)
            VALUES (?, ?, 'pending', 'drift', ?, 1)
        """, [name, part_type, sample_combo])


def detect_drift(conn) -> dict:
    """Scan the placements table and record any tokens that aren't in the
    accepted parts catalog as 'pending' entries for admin review.

    Should be called after a scrape (e.g., from normalize_data() or
    refresh_all). Idempotent.

    Returns a dict {part_type: count_of_new_pending} summarizing what was
    discovered.
    """
    # Reset stale counts so they reflect the current scan only
    conn.execute("UPDATE parts_catalog SET occurrence_count = 0 WHERE status = 'pending'")

    cat = PartsCatalog.load(conn)  # fresh load after fandom refresh

    # For each (column, part_type) pair, find tokens not in the catalog set
    column_types = [
        ("bit_1", "bit"), ("bit_2", "bit"), ("bit_3", "bit"),
        ("ratchet_1", "ratchet"), ("ratchet_2", "ratchet"), ("ratchet_3", "ratchet"),
        ("assist_1", "assist"), ("assist_2", "assist"), ("assist_3", "assist"),
        ("over_blade_1", "over_blade"), ("over_blade_2", "over_blade"), ("over_blade_3", "over_blade"),
        ("lock_chip_1", "lock_chip"), ("lock_chip_2", "lock_chip"), ("lock_chip_3", "lock_chip"),
    ]

    accepted_by_type = {
        "bit": cat.bits,
        "ratchet": cat.ratchets,
        "assist": cat.assists,
        "over_blade": cat.over_blades,
        "lock_chip": cat.lock_chips,
    }

    summary: dict[str, int] = {}
    for column, part_type in column_types:
        rows = conn.execute(f"""
            SELECT {column} AS name, COUNT(*) AS c, ANY_VALUE({column}) AS sample
            FROM placements
            WHERE {column} IS NOT NULL
            GROUP BY {column}
        """).fetchall()
        for name, count, _sample in rows:
            if not name:
                continue
            if name in accepted_by_type[part_type]:
                continue
            # New pending entry — record it (record_drift increments count)
            for _ in range(count):
                # increment by `count` in one shot to avoid N round trips:
                pass
            existing = conn.execute(
                "SELECT name, status FROM parts_catalog WHERE name = ? AND part_type = ?",
                [name, part_type],
            ).fetchone()
            if existing and existing[1] == "accepted":
                continue
            if existing:
                conn.execute("""
                    UPDATE parts_catalog
                    SET occurrence_count = COALESCE(occurrence_count, 0) + ?
                    WHERE name = ? AND part_type = ?
                """, [count, name, part_type])
            else:
                conn.execute("""
                    INSERT INTO parts_catalog
                        (name, part_type, status, source, occurrence_count)
                    VALUES (?, ?, 'pending', 'drift', ?)
                """, [name, part_type, count])
            summary[part_type] = summary.get(part_type, 0) + 1

    # Blades: scan blade_1/2/3 for unknowns. We accept any blade that is in
    # cat.blades, cat.cx_full_names, or that is a bare CX main blade (which
    # is incomplete data, not unknown). Drift is something we've never seen
    # at all.
    accepted_blades = cat.blades | cat.cx_full_names | cat.main_blades | cat.metal_blades
    for column in ["blade_1", "blade_2", "blade_3"]:
        rows = conn.execute(f"""
            SELECT {column} AS name, COUNT(*) AS c
            FROM placements
            WHERE {column} IS NOT NULL
            GROUP BY {column}
        """).fetchall()
        for name, count in rows:
            if not name or name in accepted_blades:
                continue
            existing = conn.execute(
                "SELECT name, status FROM parts_catalog WHERE name = ? AND part_type = 'blade'",
                [name],
            ).fetchone()
            if existing and existing[1] == "accepted":
                continue
            if existing:
                conn.execute("""
                    UPDATE parts_catalog
                    SET occurrence_count = COALESCE(occurrence_count, 0) + ?
                    WHERE name = ? AND part_type = 'blade'
                """, [count, name])
            else:
                conn.execute("""
                    INSERT INTO parts_catalog
                        (name, part_type, status, source, occurrence_count)
                    VALUES (?, 'blade', 'pending', 'drift', ?)
                """, [name, count])
            summary["blade"] = summary.get("blade", 0) + 1

    conn.commit()
    return summary


# ----------------------------------------------------------------------
# JSON export for the frontend
# ----------------------------------------------------------------------
def export_catalog_json(conn, path: Path = CATALOG_JSON_PATH) -> None:
    """Emit site/public/data/parts_catalog.json from the parts_catalog table.

    The Astro frontend reads this file at runtime instead of having
    hardcoded part lists in db.ts.
    """
    cat = PartsCatalog.load(conn)

    payload = {
        "lock_chips": [
            {"name": n, "metal": n in cat.metal_lock_chips}
            for n in sorted(cat.lock_chips)
        ],
        "main_blades": sorted(cat.main_blades),
        "metal_blades": sorted(cat.metal_blades),
        "over_blades": sorted(cat.over_blades),
        "assists": sorted(cat.assists),
        "bits": sorted(cat.bits),
        "ratchets": sorted(cat.ratchets),
        "blades": [
            {"name": n, "system": cat.series_for_blade.get(n, "BX")}
            for n in sorted(cat.blades)
        ],
        "cx_full_names": sorted(cat.cx_full_names),
        "bit_aliases": cat.bit_aliases,
        "assist_aliases": cat.assist_aliases,
        "metal_lock_chips": sorted(cat.metal_lock_chips),
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
