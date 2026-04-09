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


# ----------------------------------------------------------------------
# RAGFlow-powered smart drift resolver
# ----------------------------------------------------------------------

# Maps the wiki filename prefix to our internal part_type vocabulary.
# Mirrors scripts/scrapers/fandom.py:URL_TYPE_MAP but applied to filenames
# (Lock_Chip_-_Eva.md, Bit_-_Flat.md, ...).
_FILENAME_TYPE_PREFIX_MAP = {
    "Lock_Chip_-_": "lock_chip",
    "Main_Blade_-_": "main_blade",
    "Over_Blade_-_": "over_blade",
    "Assist_Blade_-_": "assist",
    "Metal_Blade_-_": "metal_blade",
    "Bit_-_": "bit",
    "Ratchet_-_": "ratchet",
    "Blade_-_": "blade",
    "Ratchet_Integrated_Bit_-_": "bit",
}


def _parse_wiki_filename(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Parse a RAGFlow-uploaded filename like 'Lock_Chip_-_Eva.md' into
    (part_type, canonical_name).

    Returns (None, None) if the filename doesn't match the convention.
    """
    if not name:
        return None, None
    base = name.rsplit(".", 1)[0]  # strip .md
    for prefix, ptype in _FILENAME_TYPE_PREFIX_MAP.items():
        if base.startswith(prefix):
            canonical = base[len(prefix):].replace("_", " ")
            # Wiki spelling fix used elsewhere in the codebase
            canonical = canonical.replace("Disk ", "Disc ")
            return ptype, canonical
    return None, None


def _lexical_match(name: str, candidates: set[str], cutoff: float = 0.75) -> tuple[Optional[str], float]:
    """Find the best fuzzy string match for `name` in `candidates`.

    Uses difflib.SequenceMatcher (stdlib, no extra deps). Returns
    (best_match, ratio) or (None, 0.0) if nothing scores above cutoff.

    This handles the 95% case for drift cleanup: typos ('Rrush'→'Rush'),
    case ('Free ball'→'Free Ball'), missing spaces ('Lowrush'→'Low Rush'),
    one-letter swaps ('Elavate'→'Elevate'), etc. RAGFlow's vector
    retrieval isn't built for these short-string transformations and
    consistently fails on them.
    """
    import difflib

    if not name or not candidates:
        return None, 0.0

    name_lower = name.lower().strip()
    # Try a normalized comparison: lowercase + collapsed spaces
    candidate_map = {c.lower(): c for c in candidates}
    candidate_map_nospace = {c.lower().replace(" ", ""): c for c in candidates}

    # 1. Exact case-insensitive match
    if name_lower in candidate_map:
        return candidate_map[name_lower], 1.0
    # 2. Exact case-insensitive no-space match (handles "Lowrush" → "Low Rush")
    if name_lower.replace(" ", "") in candidate_map_nospace:
        return candidate_map_nospace[name_lower.replace(" ", "")], 0.99

    # 3. Best Levenshtein-style fuzzy match
    best_ratio = 0.0
    best_match: Optional[str] = None
    for cand in candidates:
        ratio = difflib.SequenceMatcher(None, name_lower, cand.lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = cand
    if best_match and best_ratio >= cutoff:
        return best_match, best_ratio
    return None, 0.0


def _candidates_for_type(cat: "PartsCatalog", part_type: str) -> set[str]:
    """Pick the right catalog set to fuzzy-match against for a given part_type."""
    return {
        "lock_chip": cat.lock_chips,
        "main_blade": cat.main_blades,
        "metal_blade": cat.metal_blades,
        "blade": cat.blades | cat.cx_full_names,
        "assist": cat.assists,
        "over_blade": cat.over_blades,
        "bit": cat.bits,
        "ratchet": cat.ratchets,
    }.get(part_type, set())


def resolve_pending_with_ragflow(
    conn,
    lexical_cutoff: float = 0.75,
    ragflow_similarity_threshold: float = 0.55,
    only_unsuggested: bool = True,
) -> int:
    """Suggest canonical names for pending catalog rows using a two-pass
    approach: lexical fuzzy match first, RAGFlow retrieval as fallback.

    Why two-pass: nomic-embed-text (the embedding model behind our RAGFlow)
    doesn't produce useful neighbors for short typo strings like 'Rrush' or
    'Lowrush' — the embeddings just aren't differentiated at that scale.
    Lexical (difflib SequenceMatcher) handles those perfectly. RAGFlow then
    catches the long-form semantic cases lexical missed.

    For each pending row:
      1. Lexical fuzzy match against the catalog set for that part_type.
         If ratio >= lexical_cutoff, write a suggestion and continue.
      2. Otherwise, query RAGFlow with the part name. Walk results, find
         the highest-scoring chunk whose filename type matches the pending
         part_type, and if its combined similarity is >= ragflow_similarity_threshold,
         write the parsed canonical name as the suggestion.

    Idempotent. Returns the total number of suggestions written.
    """
    from ragflow import RAGFlowClient  # local import keeps catalog usable without ragflow.py

    cat = PartsCatalog.load(conn)

    where = "status = 'pending'"
    if only_unsuggested:
        where += " AND suggested_canonical IS NULL"
    rows = conn.execute(f"SELECT name, part_type FROM parts_catalog WHERE {where}").fetchall()
    if not rows:
        return 0

    client = RAGFlowClient()
    ragflow_available = client.is_configured()

    written = 0
    lex_count = 0
    rag_count = 0

    for name, part_type in rows:
        candidates = _candidates_for_type(cat, part_type)

        # Ratchets are structured strings (e.g. "3-60") — fuzzy matching is
        # dangerous because "3-30"/"3-80"/"3-85" all score the same. Require
        # an effectively exact match for this type.
        cutoff = 0.99 if part_type == "ratchet" else lexical_cutoff

        # Pass 1: lexical fuzzy match
        match, ratio = _lexical_match(name, candidates, cutoff=cutoff)
        if match is not None:
            conn.execute("""
                UPDATE parts_catalog SET
                    suggested_canonical = ?,
                    suggestion_reason = ?,
                    suggestion_confidence = ?,
                    suggested_at = current_timestamp
                WHERE name = ? AND part_type = ?
            """, [
                match,
                f"Lexical fuzzy match (ratio={ratio:.3f})",
                ratio,
                name,
                part_type,
            ])
            written += 1
            lex_count += 1
            continue

        # Pass 2: RAGFlow retrieval (only if configured + lexical missed)
        if not ragflow_available:
            continue
        try:
            chunks = client.retrieve(
                question=name,
                top_k=10,
                similarity_threshold=0.05,
                vector_similarity_weight=0.9,
            )
        except Exception as e:
            print(f"  RAGFlow query failed for {name!r}: {e}")
            continue
        if not chunks:
            continue

        # Highest-scoring chunk whose filename type matches the pending part_type
        best_match = None
        for chunk in chunks:
            chunk_type, canonical = _parse_wiki_filename(chunk.document_keyword)
            if chunk_type == part_type and canonical:
                best_match = (chunk, canonical)
                break

        if best_match is None:
            continue
        chunk, canonical = best_match
        if chunk.similarity < ragflow_similarity_threshold:
            continue

        conn.execute("""
            UPDATE parts_catalog SET
                suggested_canonical = ?,
                suggestion_reason = ?,
                suggestion_confidence = ?,
                suggested_at = current_timestamp
            WHERE name = ? AND part_type = ?
        """, [
            canonical,
            f"RAGFlow nearest wiki page (similarity={chunk.similarity:.3f})",
            chunk.similarity,
            name,
            part_type,
        ])
        written += 1
        rag_count += 1

    conn.commit()

    if written:
        print(f"  Resolved {written} pending parts ({lex_count} lexical, {rag_count} RAGFlow)")
    return written


def apply_canonical_rewrites(conn) -> dict:
    """Walk every parts_catalog row that has a canonical_name and rewrite
    the matching placement columns to use the canonical form.

    Called from /api/parts/apply-changes after the admin finishes triaging
    on /admin/new-parts. Idempotent — running it twice does nothing the
    second time because the dirty strings are no longer in the placements.

    CX-aware: when a blade canonical_name parses as "[LockChip] [MainBlade]"
    via parse_cx_blade(), the rewrite splits it into the blade + lock_chip
    columns instead of dumping the whole string into blade. So
    "Empero Flare → Emperor Flare" becomes blade='Flare', lock_chip='Emperor'
    — merging into the existing aggregations for that combo.

    Returns a dict {part_type: count_of_placement_rows_rewritten}.
    """
    from db import parse_cx_blade

    summary: dict[str, int] = {}

    # Map (part_type, column_index) -> column name in placements
    BLADE_COLS = ["blade_1", "blade_2", "blade_3"]
    BIT_COLS = ["bit_1", "bit_2", "bit_3"]
    RATCHET_COLS = ["ratchet_1", "ratchet_2", "ratchet_3"]
    ASSIST_COLS = ["assist_1", "assist_2", "assist_3"]
    OVER_COLS = ["over_blade_1", "over_blade_2", "over_blade_3"]
    LOCK_COLS = ["lock_chip_1", "lock_chip_2", "lock_chip_3"]

    # Pull every row that has a canonical_name set (accepted-with-mapping
    # OR rejected-with-mapping — both are user-confirmed remappings).
    rows = conn.execute("""
        SELECT name, part_type, canonical_name
        FROM parts_catalog
        WHERE canonical_name IS NOT NULL AND canonical_name != name
    """).fetchall()

    def _count_and_update(col: str, dirty: str, set_clause: str, params: list) -> int:
        # DuckDB cursor.rowcount is unreliable for UPDATEs (returns -1).
        # Count first, then UPDATE — both queries against the same column,
        # cheap on indexed lookups.
        n = conn.execute(f"SELECT COUNT(*) FROM placements WHERE {col} = ?", [dirty]).fetchone()[0]
        if n:
            conn.execute(f"UPDATE placements SET {set_clause} WHERE {col} = ?", params + [dirty])
        return n

    for dirty_name, part_type, canonical in rows:
        rewritten_for_this = 0

        if part_type == "blade":
            # CX-aware: re-parse the canonical to see if it splits into
            # lock_chip + main_blade.
            lock_chip, main_blade = parse_cx_blade(canonical)
            for blade_col, lock_col in zip(BLADE_COLS, LOCK_COLS):
                if lock_chip is not None:
                    rewritten_for_this += _count_and_update(
                        blade_col, dirty_name,
                        f"{blade_col} = ?, {lock_col} = COALESCE({lock_col}, ?)",
                        [main_blade, lock_chip],
                    )
                else:
                    rewritten_for_this += _count_and_update(
                        blade_col, dirty_name, f"{blade_col} = ?", [canonical]
                    )

        elif part_type == "bit":
            for col in BIT_COLS:
                rewritten_for_this += _count_and_update(col, dirty_name, f"{col} = ?", [canonical])
        elif part_type == "ratchet":
            for col in RATCHET_COLS:
                rewritten_for_this += _count_and_update(col, dirty_name, f"{col} = ?", [canonical])
        elif part_type == "assist":
            for col in ASSIST_COLS:
                rewritten_for_this += _count_and_update(col, dirty_name, f"{col} = ?", [canonical])
        elif part_type == "over_blade":
            for col in OVER_COLS:
                rewritten_for_this += _count_and_update(col, dirty_name, f"{col} = ?", [canonical])
        elif part_type == "lock_chip":
            for col in LOCK_COLS:
                rewritten_for_this += _count_and_update(col, dirty_name, f"{col} = ?", [canonical])

        if rewritten_for_this:
            summary[part_type] = summary.get(part_type, 0) + rewritten_for_this

    conn.commit()
    return summary


def detect_drift(conn) -> dict:
    """Scan the placements table and record any tokens that aren't in the
    accepted parts catalog as 'pending' entries for admin review.

    Skips tokens that already exist with status='rejected' so the admin's
    rejection decisions persist across drift runs (otherwise every refresh
    would re-flag the same typos).

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
            # Skip rows the admin has already triaged (accepted or rejected).
            # Otherwise the same typo would re-flag itself on every refresh.
            if existing and existing[1] in ("accepted", "rejected"):
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
            # Skip already-triaged rows so admin decisions persist
            if existing and existing[1] in ("accepted", "rejected"):
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
