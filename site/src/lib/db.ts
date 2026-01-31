/**
 * DuckDB-WASM integration for in-browser SQL queries.
 *
 * Scoring system (matches Python analysis.py):
 * - Recency weighting: Recent results weighted higher (exponential decay)
 * - Placement scoring: 1st = 3 pts, 2nd = 2 pts, 3rd = 1 pt
 * - Combined score = sum(placement_points * recency_weight)
 */

import * as duckdb from '@duckdb/duckdb-wasm';

// Scoring configuration (matches Python analysis.py)
const PLACEMENT_POINTS: Record<number, number> = { 1: 3, 2: 2, 3: 1 };
const RECENCY_HALF_LIFE_DAYS = 30; // Emphasize recent month more heavily
const TREND_RECENT_DAYS = 30; // Compare last 30 days...
const TREND_COMPARE_DAYS = 30; // ...vs previous 30 days (30-60 days ago)

// Stage weighting: rewards consistency and finals performance
// - 'both': Highest weight - combo proved itself across all competition levels
// - 'final': Strong weight - performed in tougher top cut matches
// - 'first': Reduced weight - easier competition in pools/Swiss
const STAGE_MULTIPLIER: Record<string, number> = {
  'first': 0.5,   // 50% - first stage only (easier competition)
  'final': 1.0,   // 100% - finals only (tougher competition)
  'both': 1.15,   // 115% - used in both stages (consistent performer, slight bonus)
};

// Blade series classification (matches Python db.py)
// BX = Basic Line, UX = Unique Line, CX = Custom Line
export const BLADE_SERIES: Record<string, 'BX' | 'CX' | 'UX'> = {
  // ==========================================================================
  // BX Series (Basic Line) - Standard releases
  // ==========================================================================
  'Dran Sword': 'BX',        // BX-01
  'Hells Scythe': 'BX',      // BX-02
  'Wizard Arrow': 'BX',      // BX-03
  'Knight Shield': 'BX',     // BX-04
  'Knight Lance': 'BX',      // BX-13
  'Leon Claw': 'BX',         // BX-15
  'Shark Edge': 'BX',        // BX-14 Random Booster
  'Viper Tail': 'BX',        // BX-14 Random Booster
  'Dran Dagger': 'BX',       // BX-14 Random Booster
  'Rhino Horn': 'BX',        // BX-19
  'Phoenix Wing': 'BX',      // BX-23, also UX-04 Entry Set
  'Hells Chain': 'BX',       // BX-24 Random Booster
  'Unicorn Sting': 'BX',     // BX-26
  'Black Shell': 'BX',       // BX-24 Random Booster
  'Tyranno Beat': 'BX',      // BX-24 Random Booster
  'Weiss Tiger': 'BX',       // BX-33
  'Cobalt Dragoon': 'BX',    // BX-34
  'Cobalt Drake': 'BX',      // BX-31 Random Booster
  'Crimson Garuda': 'BX',    // BX-38
  'Talon Ptera': 'BX',       // BX-35 Random Booster
  'Roar Tyranno': 'BX',      // BX-35 Random Booster
  'Sphinx Cowl': 'BX',       // BX-35 Random Booster
  'Wyvern Gale': 'BX',       // BX-35 Random Booster
  'Shelter Drake': 'BX',     // BX-39
  'Tricera Press': 'BX',     // BX-44
  'Samurai Calibur': 'BX',   // BX-45
  'Bear Scratch': 'BX',      // BX-48 Random Booster
  'Xeno Xcalibur': 'BX',     // BXG-13
  'Chain Incendio': 'BX',    // BX Random Booster
  'Scythe Incendio': 'BX',   // BX Random Booster
  'Steel Samurai': 'BX',     // BX
  'Optimus Primal': 'BX',    // BX (Collab)
  'Bite Croc': 'BX',         // BX (Hasbro exclusive)
  'Knife Shinobi': 'BX',     // BX (Hasbro exclusive)
  'Venom': 'BX',             // BX
  'Keel Shark': 'BX',        // BX (Hasbro name for Shark Edge)
  'Whale Wave': 'BX',        // BX
  'Gill Shark': 'BX',        // BX (in CX-11 deck set but blade is BX)
  'Driger Slash': 'BX',      // BX remake of classic Driger
  'Dragoon Storm': 'BX',     // BX remake of classic Dragoon

  // ==========================================================================
  // UX Series (Unique Line) - More metal to perimeter, plastic interior hooks
  // ==========================================================================
  'Dran Buster': 'UX',       // UX-01
  'Hells Hammer': 'UX',      // UX-02
  'Wizard Rod': 'UX',        // UX-03
  // 'Soar Phoenix' removed - same blade as 'Phoenix Wing' (UX-04 Entry Set)
  'Leon Crest': 'UX',        // UX-06
  'Knight Mail': 'UX',       // UX-07
  'Silver Wolf': 'UX',       // UX-08
  'Samurai Saber': 'UX',     // UX-09
  'Phoenix Feather': 'UX',   // UX-10
  'Impact Drake': 'UX',      // UX-11
  'Tusk Mammoth': 'UX',      // UX-12 Random Booster
  'Phoenix Rudder': 'UX',    // UX-12 Random Booster
  'Ghost Circle': 'UX',      // UX-12 Random Booster
  'Golem Rock': 'UX',        // UX-13
  'Scorpio Spear': 'UX',     // UX-14
  'Shinobi Shadow': 'UX',    // UX-15 Random Booster
  'Clock Mirage': 'UX',      // UX-16
  'Meteor Dragoon': 'UX',    // UX-17
  'Mummy Curse': 'UX',       // UX-18 Random Booster
  'Dranzer Spiral': 'UX',    // UX-12 Random Booster
  'Shark Scale': 'UX',       // UX-15 Shark Scale Deck Set
  'Hover Wyvern': 'UX',      // UX
  'Aero Pegasus': 'UX',      // UX
  // 'Wand Wizard' removed - same blade as 'Wizard Rod'

  // ==========================================================================
  // CX Series (Custom Line) - Main Blade names (lock chip stored separately)
  // After parsing: "Pegasus Blast" -> lock_chip="Pegasus", blade="Blast"
  // ==========================================================================
  // Main blade types (what gets stored in blade column after parsing)
  'Brave': 'CX',             // CX-01 main blade
  'Arc': 'CX',               // CX-02 main blade
  'Dark': 'CX',              // CX-03 main blade
  'Reaper': 'CX',            // CX-05 main blade
  'Brush': 'CX',             // CX-06 main blade
  'Blast': 'CX',             // CX-07 main blade
  'Eclipse': 'CX',           // CX-09 main blade
  'Hunt': 'CX',              // CX-10 main blade
  'Might': 'CX',             // CX-11 main blade
  'Flare': 'CX',             // CX-12 main blade
  'Volt': 'CX',              // CX Random Booster main blade
  'Storm': 'CX',             // CX Random Booster main blade
  'Emperor': 'CX',           // CX main blade (Blast Emperor)
  // Also keep full names for backwards compatibility with existing data
  'Dran Brave': 'CX',
  'Wizard Arc': 'CX',
  'Perseus Dark': 'CX',
  'Hells Reaper': 'CX',
  'Fox Brush': 'CX',
  'Pegasus Blast': 'CX',
  'Sol Eclipse': 'CX',
  'Wolf Hunt': 'CX',
  'Emperor Might': 'CX',
  'Phoenix Flare': 'CX',
  'Valkyrie Volt': 'CX',
  'Blast Emperor': 'CX',
  'Emperor Brave': 'CX',
  'Cerberus Blast': 'CX',
  'Hells Blast': 'CX',
  'Might Blast': 'CX',
};

// Display name corrections (database name -> display name)
const BLADE_DISPLAY_NAMES: Record<string, string> = {
  'Soar Phoenix': 'Phoenix Wing',  // UX-04 should display as Phoenix Wing
  'Mail Knight': 'Knight Mail',    // UX-07 correct name
  'Beat Tyranno': 'Tyranno Beat',  // BX-24 correct name
};

// CX main blade names that REQUIRE a lock chip to be valid
// These are the base blade names stored in the database that need lock chip prepended
const CX_BLADES_REQUIRING_LOCKCHIP = new Set([
  'Brave', 'Arc', 'Dark', 'Reaper', 'Brush', 'Blast', 
  'Eclipse', 'Hunt', 'Might', 'Flare', 'Volt', 'Storm', 'Emperor'
]);

// Normalize blade name for display
function normalizeBladeDisplay(name: string): string {
  return BLADE_DISPLAY_NAMES[name] || name;
}

// Normalize ratchet - strip blade type prefixes (H, J, W, F, S, T, etc.)
// These prefixes belong to the blade (Heavy, Jump, Wide, etc.), not the ratchet
function normalizeRatchet(ratchet: string): string {
  if (!ratchet) return ratchet;
  // Match pattern: optional letter prefix + number + dash + number
  // e.g., "H9-60" -> "9-60", "W3-60" -> "3-60", "9-60" -> "9-60"
  const match = ratchet.match(/^[A-Za-z]*(\d+-\d+)$/);
  return match ? match[1] : ratchet;
}

// Bit display names - abbreviations to full names
const BIT_DISPLAY_NAMES: Record<string, string> = {
  // Single letter abbreviations
  'J': 'Jolt',
  'W': 'Wedge',
  'Z': 'Zap',
  
  // Two letter abbreviations
  'BS': 'Ball Spike',
  'FB': 'Free Ball',
  'GR': 'Gear Rush',
  'HN': 'High Needle',
  'HT': 'High Taper',
  'LF': 'Low Flat',
  'LO': 'Low Orb',
  'LR': 'Low Rush',
  'RA': 'Rubber Accel',
  'TK': 'Trans Kick',
  'TP': 'Taper',
  'UF': 'Under Flat',
  'UN': 'Under Needle',
  'WB': 'Wall Ball',
  'GB': 'Gear Ball',
  'GF': 'Gear Flat',
  'GN': 'Gear Needle',
  'GP': 'Gear Point',
  'DB': 'Disc Ball',
  
  // Normalize inconsistent casing/spacing
  'FreeBall': 'Free Ball',
  'Freeball': 'Free Ball',
  'RubberAccel': 'Rubber Accel',
  'GearBall': 'Gear Ball',
  'GearFlat': 'Gear Flat',
  'GearNeedle': 'Gear Needle',
  'GearPoint': 'Gear Point',
  'LowOrb': 'Low Orb',
  'High Needle': 'High Needle',
  'High Taper': 'High Taper',
  'Low Flat': 'Low Flat',
  'Low Orb': 'Low Orb',
  'Low Rush': 'Low Rush',
  'Rush Accel': 'Rush Accel',
  'Trans Kick': 'Trans Kick',
  'Disc Ball': 'Disc Ball',
};

// Normalize bit name for display
function normalizeBit(bit: string): string {
  if (!bit) return bit;
  return BIT_DISPLAY_NAMES[bit] || bit;
}

/**
 * Get the full blade name, combining lock chip for CX blades.
 * For CX blades: "Pegasus" + "Blast" = "Pegasus Blast"
 * For incomplete CX data (missing lock chip): returns just the main blade name
 */
function getFullBladeName(baseBlade: string, lockChip: string | null): string {
  const normalized = normalizeBladeDisplay(baseBlade);
  
  // If we have a lock chip, prepend it (CX blade format)
  if (lockChip) {
    return `${lockChip} ${normalized}`;
  }
  
  // No lock chip - return normalized name as-is
  // Note: If this is a CX main blade without lock chip, it's incomplete data
  return normalized;
}

// Singleton database instance
let db: duckdb.AsyncDuckDB | null = null;
let conn: duckdb.AsyncDuckDBConnection | null = null;
let initPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null;

/**
 * Force refresh the database connection.
 * Call this after new data is uploaded to reload from the server.
 */
export async function refreshDB(): Promise<void> {
  console.log('[DuckDB] Refreshing database connection...');

  // Close existing connection
  if (conn) {
    await conn.close();
    conn = null;
  }

  // Reset the init promise to force re-initialization
  initPromise = null;

  // Re-initialize (this will fetch fresh data from server)
  await initDB();

  console.log('[DuckDB] Database refreshed');
}

/**
 * Calculate recency weight using exponential decay.
 * More recent = higher weight (max 1.0)
 * Weight halves every RECENCY_HALF_LIFE_DAYS days.
 */
function calculateRecencyWeight(tournamentDate: Date, referenceDate: Date = new Date()): number {
  const msPerDay = 24 * 60 * 60 * 1000;
  const daysAgo = Math.max(0, Math.floor((referenceDate.getTime() - tournamentDate.getTime()) / msPerDay));
  return Math.pow(0.5, daysAgo / RECENCY_HALF_LIFE_DAYS);
}

/**
 * Get points for a placement, with optional stage weighting.
 * First stage combos get reduced points (67%), final/both get full points.
 */
function getPlacementScore(place: number, stage?: string | null): number {
  const basePoints = PLACEMENT_POINTS[place] || 0;
  const stageMultiplier = stage ? (STAGE_MULTIPLIER[stage] ?? 1.0) : 1.0;
  return basePoints * stageMultiplier;
}

/**
 * Initialize DuckDB-WASM and load the database.
 */
export async function initDB(): Promise<duckdb.AsyncDuckDBConnection> {
  // Return existing connection if available
  if (conn) return conn;

  // Return existing promise if initialization is in progress
  if (initPromise) return initPromise;

  initPromise = (async () => {
    // Show loading indicator
    const loader = document.getElementById('global-loader');
    if (loader) loader.classList.remove('hidden');
    loader?.classList.add('flex');

    try {
      // Get the bundles - use CDN for WASM files
      const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

      // Select a bundle based on browser checks
      const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
      );

      // Instantiate the async version
      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger();
      db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      URL.revokeObjectURL(worker_url);

      // Open connection
      conn = await db.connect();

      // Fetch and register the database file (with cache busting)
      const cacheBuster = Date.now();
      const response = await fetch(`/data/beyblade.duckdb?v=${cacheBuster}`, {
        cache: 'no-store'
      });
      if (!response.ok) {
        throw new Error(`Failed to fetch database: ${response.status} ${response.statusText}`);
      }
      const buffer = await response.arrayBuffer();
      await db.registerFileBuffer('beyblade.duckdb', new Uint8Array(buffer));

      // Attach the database
      await conn.query(`ATTACH 'beyblade.duckdb' AS beyblade (READ_ONLY)`);

      // Create the combo_usage view in our session (includes stage for weighted scoring)
      await conn.query(`
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
        FROM beyblade.placements p
        JOIN beyblade.tournaments t ON p.tournament_id = t.id
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
        FROM beyblade.placements p
        JOIN beyblade.tournaments t ON p.tournament_id = t.id
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
        FROM beyblade.placements p
        JOIN beyblade.tournaments t ON p.tournament_id = t.id
        WHERE p.blade_3 IS NOT NULL
      `);

      console.log('DuckDB initialized successfully');
      return conn;
    } catch (error) {
      console.error('Failed to initialize DuckDB:', error);
      throw error;
    } finally {
      // Hide loading indicator
      if (loader) {
        loader.classList.add('hidden');
        loader.classList.remove('flex');
      }
    }
  })();

  return initPromise;
}

/**
 * Convert Arrow/DuckDB value to plain JavaScript value.
 * Handles BigInt, Date, and other special types.
 */
function toJSValue(value: unknown): unknown {
  if (value === null || value === undefined) {
    return value;
  }
  // Convert BigInt to Number (safe for our use case - tournament counts, placements)
  if (typeof value === 'bigint') {
    return Number(value);
  }
  // Convert Date objects to ISO strings for consistent handling
  if (value instanceof Date) {
    return value.toISOString();
  }
  // Handle Arrow Decimal/Int64 types that have a valueOf method
  if (typeof value === 'object' && value !== null && 'valueOf' in value && typeof (value as any).valueOf === 'function') {
    const primitive = (value as any).valueOf();
    if (typeof primitive === 'bigint') {
      return Number(primitive);
    }
    return primitive;
  }
  return value;
}

/**
 * Execute a query and return results as an array of objects.
 *
 * Note: DuckDB-WASM returns StructRowProxy objects from toArray().
 * Object.entries() doesn't work reliably on these proxy objects in all browsers.
 * We use the schema to get column names and build plain objects manually.
 * Values are converted from Arrow types to plain JavaScript types.
 */
export async function query<T = Record<string, unknown>>(sql: string, _params?: unknown[]): Promise<T[]> {
  const connection = await initDB();
  const result = await connection.query(sql);

  // Get column names from the schema
  const columns = result.schema.fields.map(f => f.name);

  // Convert each row to a plain object using column names
  // This is more reliable than Object.entries() on StructRowProxy objects
  return result.toArray().map((row) => {
    const obj: Record<string, unknown> = {};
    for (const col of columns) {
      obj[col] = toJSValue(row[col]);
    }
    return obj;
  }) as T[];
}

// =============================================================================
// Type definitions
// =============================================================================

export interface BladeStats {
  blade: string;
  raw_score: number;
  uses: number;
  first: number;
  second: number;
  third: number;
  avg_score: number;
  trend: number;
  tier?: 'SS' | 'S' | 'A' | 'B' | 'C' | 'D' | 'F';
  series?: 'BX' | 'CX' | 'UX';
  rank?: number;
}

export interface ComboStats {
  combo: string;
  blade: string;
  ratchet: string;
  bit: string;
  lockChip?: string | null;  // Lock chip for CX blades
  raw_score: number;
  uses: number;
  first: number;
  second: number;
  third: number;
  avg_score: number;
  trend: number;
  tier?: 'SS' | 'S' | 'A' | 'B' | 'C' | 'D' | 'F';
  rank?: number;
}

export interface PartStats {
  name: string;
  raw_score: number;
  uses: number;
  first: number;
  second: number;
  third: number;
  avg_score: number;
  trend: number;
  rank?: number;
}

export interface DatabaseSummary {
  tournaments: number;
  placements: number;
  unique_players: number;
  unique_blades: number;
  earliest_tournament: string | null;
  latest_tournament: string | null;
}

export interface ComparisonResult {
  blade1: {
    name: string;
    score: number;
    uses: number;
    first: number;
    second: number;
    third: number;
    win_rate: number;
  };
  blade2: {
    name: string;
    score: number;
    uses: number;
    first: number;
    second: number;
    third: number;
    win_rate: number;
  };
  head_to_head: {
    common_tournaments: number;
    blade1_placed_higher: number;
    blade2_placed_higher: number;
    ties: number;
  };
}

// =============================================================================
// Query functions (matching Python analysis.py)
// =============================================================================

// Region type for filtering
export type Region = 'ALL' | 'NA' | 'EU' | 'JAPAN' | 'ASIA' | 'OCEANIA' | 'SA';

/**
 * Helper to build region WHERE clause.
 */
function getRegionWhereClause(region?: Region, tableAlias = ''): string {
  if (!region || region === 'ALL') return '';
  const prefix = tableAlias ? `${tableAlias}.` : '';
  return ` AND ${prefix}region = '${region}'`;
}

/**
 * Get database summary.
 */
export async function getDatabaseSummary(region?: Region): Promise<DatabaseSummary> {
  const regionFilter = region && region !== 'ALL' ? `WHERE region = '${region}'` : '';
  const regionFilterAnd = getRegionWhereClause(region);

  const [stats] = await query<{
    tournaments: bigint;
    placements: bigint;
    unique_players: bigint;
    unique_blades: bigint;
    earliest: string | null;
    latest: string | null;
  }>(`
    SELECT
      (SELECT COUNT(*) FROM beyblade.tournaments ${regionFilter}) as tournaments,
      (SELECT COUNT(*) FROM beyblade.placements p JOIN beyblade.tournaments t ON p.tournament_id = t.id WHERE 1=1${regionFilterAnd.replace('region', 't.region')}) as placements,
      (SELECT COUNT(DISTINCT player_name) FROM beyblade.placements p JOIN beyblade.tournaments t ON p.tournament_id = t.id WHERE 1=1${regionFilterAnd.replace('region', 't.region')}) as unique_players,
      (SELECT COUNT(DISTINCT blade) FROM combo_usage WHERE 1=1${regionFilterAnd}) as unique_blades,
      (SELECT MIN(date) FROM beyblade.tournaments ${regionFilter})::VARCHAR as earliest,
      (SELECT MAX(date) FROM beyblade.tournaments ${regionFilter})::VARCHAR as latest
  `);

  return {
    tournaments: Number(stats.tournaments),
    placements: Number(stats.placements),
    unique_players: Number(stats.unique_players),
    unique_blades: Number(stats.unique_blades),
    earliest_tournament: stats.earliest,
    latest_tournament: stats.latest,
  };
}

/**
 * Get ranked blades with weighted scores.
 */
export async function getRankedBlades(limit = 20, minUses = 3, region?: Region): Promise<BladeStats[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    blade: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const bladeScores: Record<string, BladeStats> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    if (!bladeScores[blade]) {
      bladeScores[blade] = {
        blade,
        raw_score: 0,
        uses: 0,
        first: 0,
        second: 0,
        third: 0,
        avg_score: 0,
        trend: 0,
      };
    }

    const tournamentDate = new Date(row.tournament_date);
    const weight = calculateRecencyWeight(tournamentDate, referenceDate);
    const points = getPlacementScore(row.place, row.stage);
    const weightedScore = points * weight;

    const stats = bladeScores[blade];
    stats.raw_score += weightedScore;
    stats.uses += 1;

    if (row.place === 1) stats.first += 1;
    else if (row.place === 2) stats.second += 1;
    else if (row.place === 3) stats.third += 1;

    // Track recent (last 30 days) vs previous period (30-60 days ago) for trend
    if (tournamentDate >= recentCutoff) {
      (stats as any).recent_score = ((stats as any).recent_score || 0) + weightedScore;
      (stats as any).recent_uses = ((stats as any).recent_uses || 0) + 1;
    } else if (tournamentDate >= olderCutoff) {
      (stats as any).older_score = ((stats as any).older_score || 0) + weightedScore;
      (stats as any).older_uses = ((stats as any).older_uses || 0) + 1;
    }
  }

  // Calculate derived metrics and filter
  const bladeList = Object.values(bladeScores)
    .filter((b) => b.uses >= minUses)
    .map((b) => {
      b.avg_score = b.raw_score / b.uses;
      const recentScore = (b as any).recent_score || 0;
      const olderScore = (b as any).older_score || 0;
      const recentUses = (b as any).recent_uses || 0;
      const olderUses = (b as any).older_uses || 0;

      // Calculate trend based on usage change between periods
      if (recentUses > 0 && olderUses > 0) {
        // Both periods have data - compare rates
        const recentRate = recentScore / recentUses;
        const olderRate = olderScore / olderUses;
        b.trend = (recentRate - olderRate) / olderRate;
      } else if (recentUses > 0 && olderUses === 0) {
        // New/rising: appeared recently but not in previous period
        b.trend = 0.5;
      } else if (recentUses === 0 && olderUses > 0) {
        // Declining: was used before but not recently
        // Scale by how much they were used before (more uses = bigger decline indicator)
        b.trend = -Math.min(0.5, olderUses * 0.1);
      } else {
        // No data in either period - neutral
        b.trend = 0;
      }


      delete (b as any).recent_score;
      delete (b as any).older_score;
      delete (b as any).recent_uses;
      delete (b as any).older_uses;
      return b;
    })
    .sort((a, b) => b.raw_score - a.raw_score)
    .slice(0, limit);

  // Assign ranks and tiers using bell curve distribution based on percentile rank
  // SS: top 3%, S: 3-13%, A: 13-30%, B: 30-50%, C: 50-70%, D: 70-97%, F: bottom 3%
  // This creates a bell curve with C as the middle/most common tier
  if (bladeList.length > 0) {
    const total = bladeList.length;
    for (let i = 0; i < bladeList.length; i++) {
      const blade = bladeList[i];
      blade.rank = i + 1; // Assign rank (1-indexed)
      const percentile = (i / total) * 100; // 0 = best, 100 = worst

      if (percentile < 3) blade.tier = 'SS';        // Top 3%
      else if (percentile < 13) blade.tier = 'S';   // Next 10%
      else if (percentile < 30) blade.tier = 'A';   // Next 17%
      else if (percentile < 50) blade.tier = 'B';   // Next 20%
      else if (percentile < 70) blade.tier = 'C';   // Middle 20% (most common)
      else if (percentile < 97) blade.tier = 'D';   // Next 27%
      else blade.tier = 'F';                        // Bottom 3%

      // Add series info
      blade.series = BLADE_SERIES[blade.blade];
    }
  }

  return bladeList;
}

/**
 * Get ranked combos with weighted scores.
 * For CX blades, lock chip is prepended to blade name (e.g., "Pegasus Blast").
 */
export async function getRankedCombos(limit = 20, minUses = 2, region?: Region): Promise<ComboStats[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    lock_chip: string | null;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, lock_chip, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const comboScores: Record<string, ComboStats & { recent_score?: number; older_score?: number }> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const lockChip = row.lock_chip;
    const blade = getFullBladeName(row.blade, lockChip);
    const key = `${blade}|${ratchet}|${bit}`;
    const comboStr = `${blade} ${ratchet} ${bit}`;
    
    if (!comboScores[key]) {
      comboScores[key] = {
        combo: comboStr,
        blade: blade,
        ratchet: ratchet,
        bit: bit,
        lockChip: lockChip,
        raw_score: 0,
        uses: 0,
        first: 0,
        second: 0,
        third: 0,
        avg_score: 0,
        trend: 0,
        recent_score: 0,
        older_score: 0,
      };
    }

    const tournamentDate = new Date(row.tournament_date);
    const weight = calculateRecencyWeight(tournamentDate, referenceDate);
    const points = getPlacementScore(row.place, row.stage);
    const weightedScore = points * weight;

    const stats = comboScores[key];
    stats.raw_score += weightedScore;
    stats.uses += 1;

    if (row.place === 1) stats.first += 1;
    else if (row.place === 2) stats.second += 1;
    else if (row.place === 3) stats.third += 1;

    // Track recent (last 30 days) vs previous period (30-60 days ago) for trend
    if (tournamentDate >= recentCutoff) {
      stats.recent_score! += weightedScore;
      (stats as any).recent_uses = ((stats as any).recent_uses || 0) + 1;
    } else if (tournamentDate >= olderCutoff) {
      stats.older_score! += weightedScore;
      (stats as any).older_uses = ((stats as any).older_uses || 0) + 1;
    }
  }

  const comboList = Object.values(comboScores)
    .filter((c) => c.uses >= minUses)
    .map((c) => {
      c.avg_score = c.raw_score / c.uses;
      const recentUses = (c as any).recent_uses || 0;
      const olderUses = (c as any).older_uses || 0;

      if (recentUses > 0 && olderUses > 0) {
        const recentRate = c.recent_score! / recentUses;
        const olderRate = c.older_score! / olderUses;
        c.trend = (recentRate - olderRate) / olderRate;
      } else if (recentUses > 0 && olderUses === 0) {
        c.trend = 0.5;
      } else if (recentUses === 0 && olderUses > 0) {
        c.trend = -Math.min(0.5, olderUses * 0.1);
      } else {
        c.trend = 0;
      }

      delete c.recent_score;
      delete c.older_score;
      delete (c as any).recent_uses;
      delete (c as any).older_uses;
      return c;
    })
    .sort((a, b) => b.raw_score - a.raw_score)
    .slice(0, limit);

  // Assign ranks and tiers using bell curve distribution based on percentile rank
  if (comboList.length > 0) {
    const total = comboList.length;
    for (let i = 0; i < comboList.length; i++) {
      const combo = comboList[i];
      combo.rank = i + 1; // Assign rank (1-indexed)
      const percentile = (i / total) * 100;

      if (percentile < 3) combo.tier = 'SS';
      else if (percentile < 13) combo.tier = 'S';
      else if (percentile < 30) combo.tier = 'A';
      else if (percentile < 50) combo.tier = 'B';
      else if (percentile < 70) combo.tier = 'C';
      else if (percentile < 97) combo.tier = 'D';
      else combo.tier = 'F';
    }
  }

  return comboList;
}

/**
 * Get ranked ratchets.
 */
export async function getRankedRatchets(limit = 15, minUses = 3, region?: Region): Promise<PartStats[]> {
  return getRankedParts('ratchet', limit, minUses, region);
}

/**
 * Get ranked bits.
 */
export async function getRankedBits(limit = 15, minUses = 3, region?: Region): Promise<PartStats[]> {
  return getRankedParts('bit', limit, minUses, region);
}

/**
 * Generic part ranking (ratchets or bits).
 */
async function getRankedParts(partType: 'ratchet' | 'bit', limit: number, minUses: number, region?: Region): Promise<PartStats[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    part: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT ${partType} as part, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
  `);

  const partScores: Record<string, PartStats & { recent_score?: number; older_score?: number }> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const part = partType === 'ratchet' ? normalizeRatchet(row.part) : normalizeBit(row.part);
    if (!partScores[part]) {
      partScores[part] = {
        name: part,
        raw_score: 0,
        uses: 0,
        first: 0,
        second: 0,
        third: 0,
        avg_score: 0,
        trend: 0,
        recent_score: 0,
        older_score: 0,
      };
    }

    const tournamentDate = new Date(row.tournament_date);
    const weight = calculateRecencyWeight(tournamentDate, referenceDate);
    const points = getPlacementScore(row.place, row.stage);
    const weightedScore = points * weight;

    const stats = partScores[part];
    stats.raw_score += weightedScore;
    stats.uses += 1;

    if (row.place === 1) stats.first += 1;
    else if (row.place === 2) stats.second += 1;
    else if (row.place === 3) stats.third += 1;

    // Track recent (last 30 days) vs previous period (30-60 days ago) for trend
    if (tournamentDate >= recentCutoff) {
      stats.recent_score! += weightedScore;
      (stats as any).recent_uses = ((stats as any).recent_uses || 0) + 1;
    } else if (tournamentDate >= olderCutoff) {
      stats.older_score! += weightedScore;
      (stats as any).older_uses = ((stats as any).older_uses || 0) + 1;
    }
  }

  const partList = Object.values(partScores)
    .filter((p) => p.uses >= minUses)
    .map((p) => {
      p.avg_score = p.raw_score / p.uses;
      const recentUses = (p as any).recent_uses || 0;
      const olderUses = (p as any).older_uses || 0;

      if (recentUses > 0 && olderUses > 0) {
        const recentRate = p.recent_score! / recentUses;
        const olderRate = p.older_score! / olderUses;
        p.trend = (recentRate - olderRate) / olderRate;
      } else if (recentUses > 0 && olderUses === 0) {
        p.trend = 0.5;
      } else if (recentUses === 0 && olderUses > 0) {
        p.trend = -Math.min(0.5, olderUses * 0.1);
      } else {
        p.trend = 0;
      }

      delete p.recent_score;
      delete p.older_score;
      delete (p as any).recent_uses;
      delete (p as any).older_uses;
      return p;
    })
    .sort((a, b) => b.raw_score - a.raw_score)
    .slice(0, limit);

  // Assign ranks
  partList.forEach((p, i) => {
    p.rank = i + 1;
  });

  return partList;
}

/**
 * Get best ratchet+bit combinations for a specific blade.
 */
export async function getBestCombosForBlade(
  bladeName: string,
  limit = 10,
  region?: Region
): Promise<{ ratchet: string; bit: string; combo: string; raw_score: number; uses: number; first: number; second: number; third: number }[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE LOWER(blade) = LOWER('${bladeName.replace(/'/g, "''")}')${regionFilter}
  `);

  const comboScores: Record<
    string,
    { ratchet: string; bit: string; combo: string; raw_score: number; uses: number; first: number; second: number; third: number }
  > = {};
  const referenceDate = new Date();

  for (const row of rows) {
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const key = `${ratchet}|${bit}`;
    if (!comboScores[key]) {
      comboScores[key] = {
        ratchet: ratchet,
        bit: bit,
        combo: `${ratchet} ${bit}`,
        raw_score: 0,
        uses: 0,
        first: 0,
        second: 0,
        third: 0,
      };
    }

    const tournamentDate = new Date(row.tournament_date);
    const weight = calculateRecencyWeight(tournamentDate, referenceDate);
    const points = getPlacementScore(row.place, row.stage);

    const stats = comboScores[key];
    stats.raw_score += points * weight;
    stats.uses += 1;

    if (row.place === 1) stats.first += 1;
    else if (row.place === 2) stats.second += 1;
    else if (row.place === 3) stats.third += 1;
  }

  return Object.values(comboScores)
    .sort((a, b) => b.raw_score - a.raw_score)
    .slice(0, limit);
}

/**
 * Compare two blades head-to-head.
 */
export async function compareBlades(blade1: string, blade2: string, region?: Region): Promise<ComparisonResult> {
  const regionFilter = getRegionWhereClause(region);
  const [blade1Data, blade2Data] = await Promise.all([
    query<{ place: number; tournament_date: string; tournament_id: number; stage: string | null }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id, stage
      FROM combo_usage
      WHERE LOWER(blade) = LOWER('${blade1.replace(/'/g, "''")}')${regionFilter}
    `),
    query<{ place: number; tournament_date: string; tournament_id: number; stage: string | null }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id, stage
      FROM combo_usage
      WHERE LOWER(blade) = LOWER('${blade2.replace(/'/g, "''")}')${regionFilter}
    `),
  ]);

  const referenceDate = new Date();

  function calcBladeStats(data: { place: number; tournament_date: string; tournament_id: number; stage: string | null }[]) {
    const stats = { raw_score: 0, uses: 0, first: 0, second: 0, third: 0, tournaments: new Set<number>() };
    for (const row of data) {
      const weight = calculateRecencyWeight(new Date(row.tournament_date), referenceDate);
      const points = getPlacementScore(row.place, row.stage);
      stats.raw_score += points * weight;
      stats.uses += 1;
      stats.tournaments.add(row.tournament_id);
      if (row.place === 1) stats.first += 1;
      else if (row.place === 2) stats.second += 1;
      else if (row.place === 3) stats.third += 1;
    }
    return stats;
  }

  const stats1 = calcBladeStats(blade1Data);
  const stats2 = calcBladeStats(blade2Data);

  // Find common tournaments
  const commonTournaments = new Set([...stats1.tournaments].filter((t) => stats2.tournaments.has(t)));

  // Head-to-head comparison
  const blade1ByTournament: Record<number, number> = {};
  const blade2ByTournament: Record<number, number> = {};

  for (const row of blade1Data) {
    if (!(row.tournament_id in blade1ByTournament) || row.place < blade1ByTournament[row.tournament_id]) {
      blade1ByTournament[row.tournament_id] = row.place;
    }
  }

  for (const row of blade2Data) {
    if (!(row.tournament_id in blade2ByTournament) || row.place < blade2ByTournament[row.tournament_id]) {
      blade2ByTournament[row.tournament_id] = row.place;
    }
  }

  let blade1Higher = 0;
  let blade2Higher = 0;
  let ties = 0;

  for (const tid of commonTournaments) {
    const p1 = blade1ByTournament[tid] ?? 99;
    const p2 = blade2ByTournament[tid] ?? 99;
    if (p1 < p2) blade1Higher++;
    else if (p2 < p1) blade2Higher++;
    else ties++;
  }

  return {
    blade1: {
      name: blade1,
      score: Math.round(stats1.raw_score * 100) / 100,
      uses: stats1.uses,
      first: stats1.first,
      second: stats1.second,
      third: stats1.third,
      win_rate: stats1.uses > 0 ? stats1.first / stats1.uses : 0,
    },
    blade2: {
      name: blade2,
      score: Math.round(stats2.raw_score * 100) / 100,
      uses: stats2.uses,
      first: stats2.first,
      second: stats2.second,
      third: stats2.third,
      win_rate: stats2.uses > 0 ? stats2.first / stats2.uses : 0,
    },
    head_to_head: {
      common_tournaments: commonTournaments.size,
      blade1_placed_higher: blade1Higher,
      blade2_placed_higher: blade2Higher,
      ties,
    },
  };
}

/**
 * Get all unique blade names (for dropdowns).
 */
export async function getAllBlades(): Promise<string[]> {
  const rows = await query<{ blade: string }>(`
    SELECT DISTINCT blade FROM combo_usage ORDER BY blade
  `);
  return rows.map((r) => r.blade);
}

/**
 * Get all unique ratchet names (for dropdowns).
 */
export async function getAllRatchets(): Promise<string[]> {
  const rows = await query<{ ratchet: string }>(`
    SELECT DISTINCT ratchet FROM combo_usage WHERE ratchet IS NOT NULL ORDER BY ratchet
  `);
  // Normalize and deduplicate
  const normalized = [...new Set(rows.map((r) => normalizeRatchet(r.ratchet)))];
  return normalized.sort();
}

/**
 * Get all unique bit names (for dropdowns).
 */
export async function getAllBits(): Promise<string[]> {
  const rows = await query<{ bit: string }>(`
    SELECT DISTINCT bit FROM combo_usage WHERE bit IS NOT NULL ORDER BY bit
  `);
  // Normalize and deduplicate
  const normalized = [...new Set(rows.map((r) => normalizeBit(r.bit)))];
  return normalized.sort();
}

/**
 * Get all unique assist blade names (for dropdowns).
 */
export async function getAllAssists(): Promise<string[]> {
  const rows = await query<{ assist: string }>(`
    SELECT DISTINCT assist FROM combo_usage WHERE assist IS NOT NULL ORDER BY assist
  `);
  return rows.map((r) => r.assist);
}

/**
 * Get all unique lock chip names (for dropdowns).
 */
export async function getAllLockChips(): Promise<string[]> {
  const rows = await query<{ lock_chip: string }>(`
    SELECT DISTINCT lock_chip FROM combo_usage WHERE lock_chip IS NOT NULL ORDER BY lock_chip
  `);
  return rows.map((r) => r.lock_chip);
}

/**
 * Get stats for a specific combo configuration.
 */
export async function getComboStats(
  blade: string,
  ratchet: string,
  bit: string,
  lockChip?: string,
  assist?: string
): Promise<{
  combo: string;
  raw_score: number;
  uses: number;
  first: number;
  second: number;
  third: number;
  avg_placement: number;
} | null> {
  let whereClause = `LOWER(blade) = LOWER('${blade.replace(/'/g, "''")}')
    AND LOWER(ratchet) = LOWER('${ratchet.replace(/'/g, "''")}')
    AND LOWER(bit) = LOWER('${bit.replace(/'/g, "''")}')`;

  if (lockChip) {
    whereClause += ` AND LOWER(lock_chip) = LOWER('${lockChip.replace(/'/g, "''")}')`;
  }
  if (assist) {
    whereClause += ` AND LOWER(assist) = LOWER('${assist.replace(/'/g, "''")}')`;
  }

  const rows = await query<{
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE ${whereClause}
  `);

  if (rows.length === 0) return null;

  const referenceDate = new Date();
  let raw_score = 0;
  let first = 0;
  let second = 0;
  let third = 0;

  for (const row of rows) {
    const weight = calculateRecencyWeight(new Date(row.tournament_date), referenceDate);
    const points = getPlacementScore(row.place, row.stage);
    raw_score += points * weight;

    if (row.place === 1) first++;
    else if (row.place === 2) second++;
    else if (row.place === 3) third++;
  }

  // Format: [LockChip] [Blade] [Assist] [Ratchet][Bit]
  let comboStr = blade;
  if (lockChip) {
    comboStr = `${lockChip} ${blade}`;
  }
  if (assist) {
    comboStr = `${comboStr} ${assist}`;
  }
  comboStr = `${comboStr} ${ratchet}${bit}`;

  // Calculate average placement (1=1st, 2=2nd, 3=3rd)
  const totalPlacements = first + second + third;
  const avg_placement = totalPlacements > 0
    ? (first * 1 + second * 2 + third * 3) / totalPlacements
    : 0;

  return {
    combo: comboStr,
    raw_score: Math.round(raw_score * 100) / 100,
    uses: rows.length,
    first,
    second,
    third,
    avg_placement: Math.round(avg_placement * 100) / 100,
  };
}

/**
 * Compare two full combos head-to-head.
 */
export interface FullComboComparisonResult {
  combo1: {
    combo: string;
    blade: string;
    ratchet: string;
    bit: string;
    lockChip?: string;
    assist?: string;
    score: number;
    uses: number;
    first: number;
    second: number;
    third: number;
    avg_placement: number;
  };
  combo2: {
    combo: string;
    blade: string;
    ratchet: string;
    bit: string;
    lockChip?: string;
    assist?: string;
    score: number;
    uses: number;
    first: number;
    second: number;
    third: number;
    avg_placement: number;
  };
  head_to_head: {
    common_tournaments: number;
    combo1_placed_higher: number;
    combo2_placed_higher: number;
    ties: number;
  };
}

export async function compareFullCombos(
  combo1: { blade: string; ratchet: string; bit: string; lockChip?: string; assist?: string },
  combo2: { blade: string; ratchet: string; bit: string; lockChip?: string; assist?: string }
): Promise<FullComboComparisonResult> {
  function buildWhereClause(c: { blade: string; ratchet: string; bit: string; lockChip?: string; assist?: string }) {
    let clause = `LOWER(blade) = LOWER('${c.blade.replace(/'/g, "''")}')
      AND LOWER(ratchet) = LOWER('${c.ratchet.replace(/'/g, "''")}')
      AND LOWER(bit) = LOWER('${c.bit.replace(/'/g, "''")}')`;
    if (c.lockChip) {
      clause += ` AND LOWER(lock_chip) = LOWER('${c.lockChip.replace(/'/g, "''")}')`;
    }
    if (c.assist) {
      clause += ` AND LOWER(assist) = LOWER('${c.assist.replace(/'/g, "''")}')`;
    }
    return clause;
  }

  const [combo1Data, combo2Data] = await Promise.all([
    query<{ place: number; tournament_date: string; tournament_id: number; stage: string | null }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id, stage
      FROM combo_usage
      WHERE ${buildWhereClause(combo1)}
    `),
    query<{ place: number; tournament_date: string; tournament_id: number; stage: string | null }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id, stage
      FROM combo_usage
      WHERE ${buildWhereClause(combo2)}
    `),
  ]);

  const referenceDate = new Date();

  function calcComboStats(data: { place: number; tournament_date: string; tournament_id: number; stage: string | null }[]) {
    const stats = { raw_score: 0, uses: 0, first: 0, second: 0, third: 0, tournaments: new Set<number>() };
    for (const row of data) {
      const weight = calculateRecencyWeight(new Date(row.tournament_date), referenceDate);
      const points = getPlacementScore(row.place, row.stage);
      stats.raw_score += points * weight;
      stats.uses += 1;
      stats.tournaments.add(row.tournament_id);
      if (row.place === 1) stats.first += 1;
      else if (row.place === 2) stats.second += 1;
      else if (row.place === 3) stats.third += 1;
    }
    return stats;
  }

  const stats1 = calcComboStats(combo1Data);
  const stats2 = calcComboStats(combo2Data);

  // Find common tournaments
  const commonTournaments = new Set([...stats1.tournaments].filter((t) => stats2.tournaments.has(t)));

  // Head-to-head comparison
  const combo1ByTournament: Record<number, number> = {};
  const combo2ByTournament: Record<number, number> = {};

  for (const row of combo1Data) {
    if (!(row.tournament_id in combo1ByTournament) || row.place < combo1ByTournament[row.tournament_id]) {
      combo1ByTournament[row.tournament_id] = row.place;
    }
  }

  for (const row of combo2Data) {
    if (!(row.tournament_id in combo2ByTournament) || row.place < combo2ByTournament[row.tournament_id]) {
      combo2ByTournament[row.tournament_id] = row.place;
    }
  }

  let combo1Higher = 0;
  let combo2Higher = 0;
  let ties = 0;

  for (const tid of commonTournaments) {
    const p1 = combo1ByTournament[tid] ?? 99;
    const p2 = combo2ByTournament[tid] ?? 99;
    if (p1 < p2) combo1Higher++;
    else if (p2 < p1) combo2Higher++;
    else ties++;
  }

  function formatComboString(c: { blade: string; ratchet: string; bit: string; lockChip?: string; assist?: string }) {
    // Format: [LockChip] [Blade] [Assist] [Ratchet][Bit]
    let name = c.blade;
    if (c.lockChip) {
      name = `${c.lockChip} ${c.blade}`;
    }
    if (c.assist) {
      name = `${name} ${c.assist}`;
    }
    return `${name} ${c.ratchet}${c.bit}`;
  }

  // Calculate average placement (1=1st, 2=2nd, 3=3rd)
  const calcAvgPlacement = (s: typeof stats1) => {
    const total = s.first + s.second + s.third;
    return total > 0 ? (s.first * 1 + s.second * 2 + s.third * 3) / total : 0;
  };

  return {
    combo1: {
      combo: formatComboString(combo1),
      blade: combo1.blade,
      ratchet: combo1.ratchet,
      bit: combo1.bit,
      lockChip: combo1.lockChip,
      assist: combo1.assist,
      score: Math.round(stats1.raw_score * 100) / 100,
      uses: stats1.uses,
      first: stats1.first,
      second: stats1.second,
      third: stats1.third,
      avg_placement: Math.round(calcAvgPlacement(stats1) * 100) / 100,
    },
    combo2: {
      combo: formatComboString(combo2),
      blade: combo2.blade,
      ratchet: combo2.ratchet,
      bit: combo2.bit,
      lockChip: combo2.lockChip,
      assist: combo2.assist,
      score: Math.round(stats2.raw_score * 100) / 100,
      uses: stats2.uses,
      first: stats2.first,
      second: stats2.second,
      third: stats2.third,
      avg_placement: Math.round(calcAvgPlacement(stats2) * 100) / 100,
    },
    head_to_head: {
      common_tournaments: commonTournaments.size,
      combo1_placed_higher: combo1Higher,
      combo2_placed_higher: combo2Higher,
      ties,
    },
  };
}

/**
 * Get recent meta snapshot.
 */
export async function getMetaSnapshot(days = 30, region?: Region): Promise<{
  period_days: number;
  tournaments: number;
  top_blades: { blade: string; uses: number; wins: number }[];
  top_combos: { combo: string; uses: number; wins: number }[];
}> {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - days);
  const cutoffStr = cutoffDate.toISOString().split('T')[0];
  const regionFilter = getRegionWhereClause(region);

  const [tournamentsResult, bladesResult, combosResult] = await Promise.all([
    query<{ count: bigint }>(`
      SELECT COUNT(*) as count FROM beyblade.tournaments WHERE date >= '${cutoffStr}'${regionFilter}
    `),
    query<{ blade: string; uses: bigint; wins: bigint }>(`
      SELECT blade, COUNT(*) as uses,
             SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
      FROM combo_usage
      WHERE tournament_date >= '${cutoffStr}'${regionFilter}
      GROUP BY blade
      ORDER BY uses DESC
      LIMIT 10
    `),
    query<{ combo: string; uses: bigint; wins: bigint }>(`
      SELECT blade || ' ' || ratchet || ' ' || bit as combo, COUNT(*) as uses,
             SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
      FROM combo_usage
      WHERE tournament_date >= '${cutoffStr}'${regionFilter}
      GROUP BY blade, ratchet, bit
      ORDER BY uses DESC
      LIMIT 10
    `),
  ]);

  return {
    period_days: days,
    tournaments: Number(tournamentsResult[0]?.count || 0),
    top_blades: bladesResult.map((b) => ({ blade: b.blade, uses: Number(b.uses), wins: Number(b.wins) })),
    top_combos: combosResult.map((c) => ({ combo: c.combo, uses: Number(c.uses), wins: Number(c.wins) })),
  };
}

/**
 * Get ranked assists with weighted scores.
 */
export async function getRankedAssists(limit = 15, minUses = 2, region?: Region): Promise<PartStats[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    assist: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT assist, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE assist IS NOT NULL${regionFilter}
  `);

  const assistScores: Record<string, PartStats & { recent_score?: number; older_score?: number }> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const assist = row.assist;
    if (!assistScores[assist]) {
      assistScores[assist] = {
        name: assist,
        raw_score: 0,
        uses: 0,
        first: 0,
        second: 0,
        third: 0,
        avg_score: 0,
        trend: 0,
        recent_score: 0,
        older_score: 0,
      };
    }

    const tournamentDate = new Date(row.tournament_date);
    const weight = calculateRecencyWeight(tournamentDate, referenceDate);
    const points = getPlacementScore(row.place, row.stage);
    const weightedScore = points * weight;

    const stats = assistScores[assist];
    stats.raw_score += weightedScore;
    stats.uses += 1;

    if (row.place === 1) stats.first += 1;
    else if (row.place === 2) stats.second += 1;
    else if (row.place === 3) stats.third += 1;

    // Track recent (last 30 days) vs previous period (30-60 days ago) for trend
    if (tournamentDate >= recentCutoff) {
      stats.recent_score! += weightedScore;
      (stats as any).recent_uses = ((stats as any).recent_uses || 0) + 1;
    } else if (tournamentDate >= olderCutoff) {
      stats.older_score! += weightedScore;
      (stats as any).older_uses = ((stats as any).older_uses || 0) + 1;
    }
  }

  return Object.values(assistScores)
    .filter((a) => a.uses >= minUses)
    .map((a) => {
      a.avg_score = a.raw_score / a.uses;
      const recentUses = (a as any).recent_uses || 0;
      const olderUses = (a as any).older_uses || 0;

      if (recentUses > 0 && olderUses > 0) {
        const recentRate = a.recent_score! / recentUses;
        const olderRate = a.older_score! / olderUses;
        a.trend = (recentRate - olderRate) / olderRate;
      } else if (recentUses > 0 && olderUses === 0) {
        a.trend = 0.5;
      } else if (recentUses === 0 && olderUses > 0) {
        a.trend = -Math.min(0.5, olderUses * 0.1);
      } else {
        a.trend = 0;
      }

      delete a.recent_score;
      delete a.older_score;
      delete (a as any).recent_uses;
      delete (a as any).older_uses;
      return a;
    })
    .sort((a, b) => b.raw_score - a.raw_score)
    .slice(0, limit);
}

/**
 * Get ranked lock chips with weighted scores.
 */
export async function getRankedLockChips(limit = 15, minUses = 2, region?: Region): Promise<PartStats[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    lock_chip: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT lock_chip, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE lock_chip IS NOT NULL${regionFilter}
  `);

  const lockChipScores: Record<string, PartStats & { recent_score?: number; older_score?: number }> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const lockChip = row.lock_chip;
    if (!lockChipScores[lockChip]) {
      lockChipScores[lockChip] = {
        name: lockChip,
        raw_score: 0,
        uses: 0,
        first: 0,
        second: 0,
        third: 0,
        avg_score: 0,
        trend: 0,
        recent_score: 0,
        older_score: 0,
      };
    }

    const tournamentDate = new Date(row.tournament_date);
    const weight = calculateRecencyWeight(tournamentDate, referenceDate);
    const points = getPlacementScore(row.place, row.stage);
    const weightedScore = points * weight;

    const stats = lockChipScores[lockChip];
    stats.raw_score += weightedScore;
    stats.uses += 1;

    if (row.place === 1) stats.first += 1;
    else if (row.place === 2) stats.second += 1;
    else if (row.place === 3) stats.third += 1;

    if (tournamentDate >= recentCutoff) {
      stats.recent_score! += weightedScore;
      (stats as any).recent_uses = ((stats as any).recent_uses || 0) + 1;
    } else if (tournamentDate >= olderCutoff) {
      stats.older_score! += weightedScore;
      (stats as any).older_uses = ((stats as any).older_uses || 0) + 1;
    }
  }

  return Object.values(lockChipScores)
    .filter((l) => l.uses >= minUses)
    .map((l) => {
      l.avg_score = l.raw_score / l.uses;
      const recentUses = (l as any).recent_uses || 0;
      const olderUses = (l as any).older_uses || 0;

      if (recentUses > 0 && olderUses > 0) {
        const recentRate = l.recent_score! / recentUses;
        const olderRate = l.older_score! / olderUses;
        l.trend = (recentRate - olderRate) / olderRate;
      } else if (recentUses > 0 && olderUses === 0) {
        l.trend = 0.5;
      } else if (recentUses === 0 && olderUses > 0) {
        l.trend = -Math.min(0.5, olderUses * 0.1);
      } else {
        l.trend = 0;
      }

      delete l.recent_score;
      delete l.older_score;
      delete (l as any).recent_uses;
      delete (l as any).older_uses;
      return l;
    })
    .sort((a, b) => b.raw_score - a.raw_score)
    .slice(0, limit);
}

/**
 * Get ranked blades filtered by series.
 */
export async function getRankedBladesBySeries(
  series: 'BX' | 'CX' | 'UX' | 'all',
  limit = 50,
  minUses = 2,
  region?: Region
): Promise<BladeStats[]> {
  // Get all blades first
  const allBlades = await getRankedBlades(200, minUses, region);

  // Add series info to each blade
  const bladesWithSeries = allBlades.map((blade) => ({
    ...blade,
    series: BLADE_SERIES[blade.blade] as 'BX' | 'CX' | 'UX' | undefined,
  }));

  // Filter by series if specified
  const filtered =
    series === 'all' ? bladesWithSeries : bladesWithSeries.filter((b) => b.series === series);

  return filtered.slice(0, limit);
}

/**
 * Get blade series for a blade name.
 */
export function getBladeSeries(bladeName: string): 'BX' | 'CX' | 'UX' | undefined {
  return BLADE_SERIES[bladeName];
}

/**
 * Get top blades for each series (BX, CX, UX).
 */
export async function getTopBladesBySeries(limitPerSeries = 5, minUses = 2, region?: Region): Promise<{
  BX: BladeStats[];
  CX: BladeStats[];
  UX: BladeStats[];
}> {
  const allBlades = await getRankedBlades(200, minUses, region);

  const result: { BX: BladeStats[]; CX: BladeStats[]; UX: BladeStats[] } = {
    BX: [],
    CX: [],
    UX: [],
  };

  for (const blade of allBlades) {
    const series = BLADE_SERIES[blade.blade];
    if (series && result[series].length < limitPerSeries) {
      blade.series = series;
      result[series].push(blade);
    }
  }

  return result;
}

// ============================================================================
// Rate My Deck System
// ============================================================================

export interface DeckCombo {
  blade: string;
  ratchet: string;
  bit: string;
}

export interface ComboRating {
  combo: string;
  blade: string;
  ratchet: string;
  bit: string;
  // Core stats
  score: number;
  uses: number;
  wins: number;
  avgPlace: number;
  winRate: number;
  // Individual scores (0-100 scale)
  metaScore: number;        // How it compares to top meta
  consistencyScore: number; // How reliable/consistent
  upsideScore: number;      // High ceiling potential
  synergyScore: number;     // How well parts work together
  surpriseScore: number;    // Potential for unexpected wins (low usage + high win rate)
  trendScore: number;       // Recent performance trend
  // Tier based on overall score
  tier: 'S' | 'A' | 'B' | 'C' | 'D' | 'F' | '?';
  // Flags
  isMetaPick: boolean;
  isHiddenGem: boolean;
  isRising: boolean;
  hasData: boolean;
}

export interface DeckRating {
  combos: ComboRating[];
  // Deck-wide scores (0-100)
  overallScore: number;
  metaCoverage: number;      // How well deck covers the meta
  diversityScore: number;    // Variety of strategies/types
  consistencyScore: number;  // Overall reliability
  upsideScore: number;       // Ceiling potential
  surpriseScore: number;     // Upset potential
  // Overall tier
  tier: 'S' | 'A' | 'B' | 'C' | 'D' | 'F';
  // Analysis text
  strengths: string[];
  weaknesses: string[];
  suggestions: string[];
}

/**
 * Rate a single combo based on tournament data.
 */
async function rateCombo(combo: DeckCombo, metaData: {
  topCombos: Array<{ blade: string; ratchet: string; bit: string; score: number; uses: number; wins: number; avgPlace: number }>;
  topScore: number;
  avgScore: number;
  partStats: { blades: Record<string, { score: number; uses: number; winRate: number }>; ratchets: Record<string, { score: number; uses: number; winRate: number }>; bits: Record<string, { score: number; uses: number; winRate: number }> };
  trends: Record<string, number>;
}, region?: Region): Promise<ComboRating> {
  const blade = normalizeBladeDisplay(combo.blade);
  const comboKey = `${blade} ${combo.ratchet} ${combo.bit}`;
  
  // Get combo stats - note: getComboStats uses (blade, ratchet, bit, lockChip?, assist?)
  // For deck rating we don't use lock chip or assist
  const stats = await getComboStats(blade, combo.ratchet, combo.bit);
  
  const hasData = stats !== null && stats.uses > 0;
  
  if (!hasData) {
    // No data - evaluate based on part stats
    const bladeStats = metaData.partStats.blades[blade];
    const ratchetStats = metaData.partStats.ratchets[combo.ratchet];
    const bitStats = metaData.partStats.bits[combo.bit];
    
    // Calculate theoretical scores based on parts
    let partScore = 0;
    let partCount = 0;
    
    if (bladeStats) {
      partScore += bladeStats.winRate * 50 + Math.min(bladeStats.uses, 20) * 2;
      partCount++;
    }
    if (ratchetStats) {
      partScore += ratchetStats.winRate * 30 + Math.min(ratchetStats.uses, 20);
      partCount++;
    }
    if (bitStats) {
      partScore += bitStats.winRate * 30 + Math.min(bitStats.uses, 20);
      partCount++;
    }
    
    const theoreticalScore = partCount > 0 ? partScore / partCount : 0;
    
    return {
      combo: comboKey,
      blade,
      ratchet: combo.ratchet,
      bit: combo.bit,
      score: 0,
      uses: 0,
      wins: 0,
      avgPlace: 0,
      winRate: 0,
      metaScore: Math.min(theoreticalScore * 0.5, 50), // Cap at 50 for untested
      consistencyScore: 0,
      upsideScore: theoreticalScore * 0.6,
      synergyScore: partCount === 3 ? theoreticalScore * 0.7 : 0,
      surpriseScore: partCount === 3 ? 40 : 0, // Unknown = surprise potential
      trendScore: 50, // Neutral
      tier: '?',
      isMetaPick: false,
      isHiddenGem: false,
      isRising: false,
      hasData: false,
    };
  }
  
  // Use correct property names from getComboStats return type
  const wins = stats.first; // first place = wins
  const avgPlace = stats.avg_placement;
  const score = stats.raw_score;
  
  // Calculate individual scores
  const winRate = stats.uses > 0 ? wins / stats.uses : 0;
  
  // Meta Score: How does this compare to top meta combos? (0-100)
  const metaScore = metaData.topScore > 0 
    ? Math.min((score / metaData.topScore) * 100, 100)
    : 50;
  
  // Consistency Score: Low variance in placements (0-100)
  // Higher avg place = lower consistency, more uses = more reliable data
  const avgPlaceScore = avgPlace > 0 ? Math.max(0, (4 - avgPlace) / 3) * 100 : 0;
  const usageBonus = Math.min(stats.uses / 10, 1) * 20;
  const consistencyScore = Math.min(avgPlaceScore * 0.8 + usageBonus, 100);
  
  // Upside Score: High ceiling potential (0-100)
  // Based on wins and best possible outcomes
  const winRateScore = winRate * 100;
  const winsBonus = Math.min(wins * 10, 50);
  const upsideScore = Math.min(winRateScore * 0.6 + winsBonus, 100);
  
  // Synergy Score: How well parts work together (0-100)
  // Compare combo performance vs expected from parts
  const bladeStats = metaData.partStats.blades[blade];
  const ratchetStats = metaData.partStats.ratchets[combo.ratchet];
  const bitStats = metaData.partStats.bits[combo.bit];
  
  let expectedWinRate = 0;
  let partCount = 0;
  if (bladeStats) { expectedWinRate += bladeStats.winRate; partCount++; }
  if (ratchetStats) { expectedWinRate += ratchetStats.winRate; partCount++; }
  if (bitStats) { expectedWinRate += bitStats.winRate; partCount++; }
  expectedWinRate = partCount > 0 ? expectedWinRate / partCount : 0.25;
  
  // Synergy = actual performance vs expected
  const synergyMultiplier = expectedWinRate > 0 ? winRate / expectedWinRate : 1;
  const synergyScore = Math.min(Math.max((synergyMultiplier - 0.5) * 100, 0), 100);
  
  // Surprise Score: Low usage but high win rate = upset potential (0-100)
  const usageRarity = Math.max(0, 1 - (stats.uses / 20)); // Rarer = higher
  const performanceBonus = winRate > 0.3 ? (winRate - 0.3) * 100 : 0;
  const surpriseScore = Math.min(usageRarity * 50 + performanceBonus, 100);
  
  // Trend Score: Is it rising or falling? (0-100, 50 = neutral)
  const trend = metaData.trends[comboKey] || 0;
  const trendScore = 50 + Math.max(-50, Math.min(50, trend * 10));
  
  // Calculate overall score (weighted average)
  const overallScore = 
    metaScore * 0.30 +       // Meta relevance
    consistencyScore * 0.20 + // Reliability
    upsideScore * 0.20 +      // Win potential
    synergyScore * 0.15 +     // Part synergy
    surpriseScore * 0.05 +    // Upset factor
    trendScore * 0.10;        // Momentum
  
  // Determine tier
  let tier: ComboRating['tier'];
  if (overallScore >= 85) tier = 'S';
  else if (overallScore >= 70) tier = 'A';
  else if (overallScore >= 55) tier = 'B';
  else if (overallScore >= 40) tier = 'C';
  else if (overallScore >= 25) tier = 'D';
  else tier = 'F';
  
  // Flags
  const isMetaPick = metaData.topCombos.some(c => 
    c.blade === blade && c.ratchet === combo.ratchet && c.bit === combo.bit
  );
  const isHiddenGem = stats.uses >= 2 && stats.uses <= 10 && winRate >= 0.3;
  const isRising = trend > 2;
  
  return {
    combo: comboKey,
    blade,
    ratchet: combo.ratchet,
    bit: combo.bit,
    score: score,
    uses: stats.uses,
    wins: wins,
    avgPlace: avgPlace,
    winRate: winRate * 100,
    metaScore,
    consistencyScore,
    upsideScore,
    synergyScore,
    surpriseScore,
    trendScore,
    tier,
    isMetaPick,
    isHiddenGem,
    isRising,
    hasData: true,
  };
}

/**
 * Rate a full deck (3 combos) for tournament play.
 */
export async function rateDeck(
  combos: [DeckCombo, DeckCombo, DeckCombo],
  region?: Region
): Promise<DeckRating> {
  // Gather meta context data
  const [topCombosRaw, rankedBlades, rankedRatchets, rankedBits] = await Promise.all([
    getRankedCombos(20, 2, region),
    getRankedBlades(30, 2, region),
    getRankedRatchets(15, 2, region),
    getRankedBits(15, 2, region),
  ]);
  
  // Map to consistent property names for internal use
  const topCombos = topCombosRaw.map(c => ({
    blade: normalizeBladeDisplay(c.blade),
    ratchet: c.ratchet,
    bit: c.bit,
    score: c.raw_score,
    uses: c.uses,
    wins: c.first,
    avgPlace: c.avg_score, // avg_score is actually avg placement weighted score
  }));
  
  const topScore = topCombos[0]?.score || 1;
  const avgScore = topCombos.length > 0 
    ? topCombos.reduce((sum, c) => sum + c.score, 0) / topCombos.length 
    : 1;
  
  // Build part stats lookup - BladeStats has 'blade', PartStats has 'name'
  const partStats = {
    blades: {} as Record<string, { score: number; uses: number; winRate: number }>,
    ratchets: {} as Record<string, { score: number; uses: number; winRate: number }>,
    bits: {} as Record<string, { score: number; uses: number; winRate: number }>,
  };
  
  for (const b of rankedBlades) {
    // BladeStats: blade, raw_score, uses, first (wins)
    const winRate = b.uses > 0 ? b.first / b.uses : 0;
    partStats.blades[b.blade] = { score: b.raw_score, uses: b.uses, winRate };
  }
  for (const r of rankedRatchets) {
    // PartStats: name, raw_score, uses, first (wins)
    const winRate = r.uses > 0 ? r.first / r.uses : 0;
    partStats.ratchets[r.name] = { score: r.raw_score, uses: r.uses, winRate };
  }
  for (const b of rankedBits) {
    // PartStats: name, raw_score, uses, first (wins)
    const winRate = b.uses > 0 ? b.first / b.uses : 0;
    partStats.bits[b.name] = { score: b.raw_score, uses: b.uses, winRate };
  }
  
  // Get trend data
  const sparklineData = await getCombosSparklineData(
    combos.map(c => ({ blade: c.blade, ratchet: c.ratchet, bit: c.bit })),
    8,
    region
  );
  
  const trends: Record<string, number> = {};
  for (const [key, values] of Object.entries(sparklineData)) {
    if (values.length >= 4) {
      const recent = values.slice(-4).reduce((a, b) => a + b, 0);
      const older = values.slice(0, 4).reduce((a, b) => a + b, 0);
      trends[key] = older > 0 ? ((recent - older) / older) * 10 : 0;
    }
  }
  
  const metaData = { topCombos, topScore, avgScore, partStats, trends };
  
  // Rate each combo
  const comboRatings = await Promise.all(
    combos.map(combo => rateCombo(combo, metaData, region))
  );
  
  // Calculate deck-wide metrics
  const validCombos = comboRatings.filter(c => c.hasData);
  const hasAnyData = validCombos.length > 0;
  
  // Overall Score: Weighted average of combo scores
  const comboScores = comboRatings.map(c => 
    c.metaScore * 0.3 + c.consistencyScore * 0.2 + c.upsideScore * 0.2 + 
    c.synergyScore * 0.15 + c.surpriseScore * 0.05 + c.trendScore * 0.1
  );
  const overallScore = comboScores.reduce((a, b) => a + b, 0) / 3;
  
  // Meta Coverage: Does deck have answers to top meta?
  const topBlades = new Set(topCombos.slice(0, 10).map(c => c.blade));
  const deckBlades = new Set(comboRatings.map(c => c.blade));
  const metaOverlap = [...topBlades].filter(b => deckBlades.has(b)).length;
  const metaCoverage = (metaOverlap / Math.min(topBlades.size, 3)) * 50 + 
    (comboRatings.filter(c => c.isMetaPick).length / 3) * 50;
  
  // Diversity Score: Variety of blades/strategies
  const uniqueBlades = new Set(comboRatings.map(c => c.blade)).size;
  const uniqueRatchets = new Set(comboRatings.map(c => c.ratchet)).size;
  const uniqueBits = new Set(comboRatings.map(c => c.bit)).size;
  const diversityScore = ((uniqueBlades / 3) * 40 + (uniqueRatchets / 3) * 30 + (uniqueBits / 3) * 30);
  
  // Consistency Score: Average of combo consistencies
  const consistencyScore = comboRatings.reduce((sum, c) => sum + c.consistencyScore, 0) / 3;
  
  // Upside Score: Best combo's upside + average
  const maxUpside = Math.max(...comboRatings.map(c => c.upsideScore));
  const avgUpside = comboRatings.reduce((sum, c) => sum + c.upsideScore, 0) / 3;
  const upsideScore = maxUpside * 0.6 + avgUpside * 0.4;
  
  // Surprise Score: Upset potential
  const hiddenGemCount = comboRatings.filter(c => c.isHiddenGem).length;
  const avgSurprise = comboRatings.reduce((sum, c) => sum + c.surpriseScore, 0) / 3;
  const surpriseScore = Math.min(avgSurprise + hiddenGemCount * 15, 100);
  
  // Determine tier
  let tier: DeckRating['tier'];
  if (overallScore >= 80) tier = 'S';
  else if (overallScore >= 65) tier = 'A';
  else if (overallScore >= 50) tier = 'B';
  else if (overallScore >= 35) tier = 'C';
  else if (overallScore >= 20) tier = 'D';
  else tier = 'F';
  
  // Generate analysis
  const strengths: string[] = [];
  const weaknesses: string[] = [];
  const suggestions: string[] = [];
  
  // Analyze strengths
  if (metaCoverage >= 70) strengths.push('Strong meta coverage - your deck can compete with top combos');
  if (consistencyScore >= 70) strengths.push('Highly consistent - reliable placements across tournaments');
  if (upsideScore >= 70) strengths.push('High ceiling - capable of winning tournaments');
  if (diversityScore >= 80) strengths.push('Good diversity - multiple strategies available');
  if (surpriseScore >= 60) strengths.push('Upset potential - can catch opponents off guard');
  if (comboRatings.some(c => c.isRising)) strengths.push('Rising picks - you have trending combos');
  if (comboRatings.filter(c => c.tier === 'S' || c.tier === 'A').length >= 2) {
    strengths.push('Multiple S/A tier combos - strong core lineup');
  }
  
  // Analyze weaknesses
  if (metaCoverage < 40) weaknesses.push('Limited meta coverage - may struggle vs top combos');
  if (consistencyScore < 40) weaknesses.push('Inconsistent results - high variance in placements');
  if (upsideScore < 40) weaknesses.push('Low ceiling - limited tournament winning potential');
  if (diversityScore < 50) weaknesses.push('Lack of diversity - predictable strategies');
  if (!hasAnyData) weaknesses.push('Untested combos - no tournament data available');
  if (comboRatings.filter(c => c.tier === 'D' || c.tier === 'F' || c.tier === '?').length >= 2) {
    weaknesses.push('Multiple weak/untested combos - consider replacements');
  }
  
  // Generate suggestions
  if (metaCoverage < 50 && topCombos.length > 0) {
    const suggestedBlade = topCombos[0].blade;
    if (!deckBlades.has(suggestedBlade)) {
      suggestions.push(`Consider adding ${suggestedBlade} for better meta coverage`);
    }
  }
  
  const worstCombo = comboRatings.reduce((worst, c) => 
    (c.metaScore + c.consistencyScore) < (worst.metaScore + worst.consistencyScore) ? c : worst
  );
  if (worstCombo.tier === 'D' || worstCombo.tier === 'F' || worstCombo.tier === '?') {
    suggestions.push(`Consider replacing ${worstCombo.combo} with a more proven combo`);
  }
  
  if (diversityScore < 50 && uniqueBlades < 3) {
    suggestions.push('Try using different blade types for more strategic flexibility');
  }
  
  if (consistencyScore < 50 && upsideScore > 60) {
    suggestions.push('Your deck is boom-or-bust - consider adding consistent performers');
  }
  
  if (surpriseScore < 30 && metaCoverage > 70) {
    suggestions.push('Deck is predictable - consider a hidden gem for surprise factor');
  }
  
  // Add default positive if no strengths found
  if (strengths.length === 0 && hasAnyData) {
    strengths.push('Solid tournament presence - your combos have been tested');
  }
  if (strengths.length === 0) {
    strengths.push('Creative picks - experimenting with new combinations');
  }
  
  return {
    combos: comboRatings,
    overallScore,
    metaCoverage,
    diversityScore,
    consistencyScore,
    upsideScore,
    surpriseScore,
    tier,
    strengths,
    weaknesses,
    suggestions,
  };
}


export interface RisingTrend {
  name: string;
  type: 'blade' | 'combo';
  weeklyScores: number[];
  weeklyUses: number[];
  growthRate: number; // Percentage growth
  momentum: number; // Acceleration of growth
  projectedScore: number;
}

export interface PartAnalysis {
  name: string;
  type: 'blade' | 'ratchet' | 'bit';
  uses: number;
  winRate: number;
  avgPlacement: number;
  versatility: number; // How many different combos it appears in
  synergyScore: number; // How well it pairs with top parts
  consistency: number; // Low variance = high consistency
}

export interface VarianceData {
  combo: string;
  blade: string;
  ratchet: string;
  bit: string;
  uses: number;
  avgScore: number;
  variance: number;
  stdDev: number;
  ceiling: number; // Best performance
  floor: number; // Worst performance
  highVariance: boolean;
}

// Legacy HiddenGem interface for backward compatibility
export interface HiddenGem {
  blade: string;
  ratchet: string;
  bit: string;
  combo: string;
  uses: number;
  winRate: number;
  avgScore: number;
  potential: number;
  recentTrend: 'up' | 'down' | 'stable';
}

// ============================================================================
// Enhanced Hidden Gems System
// ============================================================================

export type GemCategory = 'underused' | 'forgotten' | 'rising' | 'counter-meta';
export type MetaEra = 'early' | 'mid' | 'current';

export interface EnhancedGem {
  // Identity
  blade: string;
  ratchet: string;
  bit: string;
  lockChip?: string | null;
  combo: string;
  category: GemCategory;
  
  // Stats
  totalUses: number;
  wins: number;
  winRate: number;
  avgPlacement: number;
  score: number;
  
  // Category-specific data
  reason: string;           // Why this is a gem
  insight: string;          // Actionable insight
  dataQuality: 'strong' | 'moderate' | 'limited';  // Based on sample size
  
  // Historical context
  peakEra?: MetaEra;
  peakDate?: string;
  peakRank?: number;
  currentRank?: number;
  
  // Trend data
  recentUses: number;       // Last 60 days
  recentWinRate: number;
  trend: 'rising' | 'falling' | 'stable' | 'returning';
  
  // For counter-meta
  beatsTopMeta?: string[];  // Which top combos it has beaten
}

export interface HiddenGemsData {
  underused: EnhancedGem[];
  forgotten: EnhancedGem[];
  rising: EnhancedGem[];
  counterMeta: EnhancedGem[];
  eraAnalysis: {
    early: { start: string; end: string; topCombos: string[] };
    mid: { start: string; end: string; topCombos: string[] };
    current: { start: string; end: string; topCombos: string[] };
  };
}

/**
 * Build a combo display name, including lock chip for CX blades.
 * Format: "[LockChip] [Blade] [Ratchet][Bit]" or "[Blade] [Ratchet][Bit]"
 */
function buildComboName(blade: string, ratchet: string, bit: string, lockChip?: string | null): string {
  if (lockChip) {
    return `${lockChip} ${blade} ${ratchet} ${bit}`;
  }
  return `${blade} ${ratchet} ${bit}`;
}

/**
 * Find hidden gems - low usage combos with high win rates.
 * These are combos that might break out if they get more exposure.
 *
 * Requirements for a "hidden gem":
 * - At least 4 uses (enough for statistical meaning)
 * - Not more than 20 uses (still "hidden", not mainstream)
 * - At least 33% win rate (1st place finishes)
 * - Good average placement
 */
export async function getHiddenGems(minUses = 4, maxUses = 20, region?: Region): Promise<HiddenGem[]> {
  const regionFilter = getRegionWhereClause(region);
  
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const now = new Date();
  const sixtyDaysAgo = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);
  const ninetyDaysAgo = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
  
  // Aggregate combo stats
  const comboStats: Record<string, {
    uses: number;
    wins: number;
    topThree: number; // 1st, 2nd, or 3rd place finishes
    totalScore: number;
    recentUses: number;   // Last 60 days
    recentScore: number;
    olderUses: number;    // 60-90 days ago
    olderScore: number;
    placements: number[];
    lastUsed: Date;
  }> = {};

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const key = `${blade}|${ratchet}|${bit}`;
    const tournamentDate = new Date(row.tournament_date);
    
    if (!comboStats[key]) {
      comboStats[key] = { 
        uses: 0, wins: 0, topThree: 0, totalScore: 0, 
        recentUses: 0, recentScore: 0, olderUses: 0, olderScore: 0, 
        placements: [], lastUsed: tournamentDate 
      };
    }
    
    const stats = comboStats[key];
    const points = getPlacementScore(row.place, row.stage);
    const weight = calculateRecencyWeight(tournamentDate, now);
    
    stats.uses++;
    stats.totalScore += points * weight;
    stats.placements.push(row.place);
    if (row.place === 1) stats.wins++;
    if (row.place <= 3) stats.topThree++;
    if (tournamentDate > stats.lastUsed) stats.lastUsed = tournamentDate;
    
    // Track activity in different time windows for trend
    if (tournamentDate >= sixtyDaysAgo) {
      stats.recentUses++;
      stats.recentScore += points;
    } else if (tournamentDate >= ninetyDaysAgo) {
      stats.olderUses++;
      stats.olderScore += points;
    }
  }

  // Calculate hidden gem potential
  const gems: HiddenGem[] = [];
  
  for (const [key, stats] of Object.entries(comboStats)) {
    if (stats.uses < minUses || stats.uses > maxUses) continue;
    
    const [blade, ratchet, bit] = key.split('|');
    const winRate = stats.wins / stats.uses;
    const topThreeRate = stats.topThree / stats.uses;
    const avgScore = stats.totalScore / stats.uses;
    
    // Skip if win rate is too low - need at least 1 win in ~4 uses
    if (winRate < 0.20) continue;
    
    // Skip if top-3 rate is poor (should place top 3 at least half the time)
    if (topThreeRate < 0.40) continue;
    
    // Potential score formula:
    // - Higher win rate = higher potential
    // - Higher avg score = higher potential  
    // - Fewer uses = higher "hidden" factor (but not too few)
    // - Recent activity bonus
    const hiddenFactor = Math.max(1, Math.log2(maxUses / stats.uses));
    const recencyBonus = stats.recentUses > 0 ? 1.2 : 1.0;
    const potential = (winRate * 40 + topThreeRate * 30 + avgScore * 5) * hiddenFactor * recencyBonus;
    
    // Trend: only show trend if we have enough data in both periods
    // Otherwise show 'stable' to avoid misleading "falling" labels
    let recentTrend: 'up' | 'down' | 'stable' = 'stable';
    
    if (stats.recentUses >= 2 && stats.olderUses >= 2) {
      // Compare average score per use in each period
      const recentAvg = stats.recentScore / stats.recentUses;
      const olderAvg = stats.olderScore / stats.olderUses;
      
      if (recentAvg > olderAvg * 1.15) recentTrend = 'up';
      else if (recentAvg < olderAvg * 0.85) recentTrend = 'down';
    } else if (stats.recentUses >= 2 && stats.olderUses === 0) {
      // New combo with recent activity - trending up
      recentTrend = 'up';
    }
    // If no recent uses, keep as 'stable' rather than showing 'down'
    
    gems.push({
      blade,
      ratchet,
      bit,
      combo: `${blade} ${ratchet} ${bit}`,
      uses: stats.uses,
      winRate,
      avgScore,
      potential,
      recentTrend,
    });
  }

  // Sort by potential (highest first)
  return gems
    .sort((a, b) => b.potential - a.potential)
    .slice(0, 20);
}

/**
 * Get comprehensive hidden gems analysis across multiple categories.
 * Analyzes all-time data and groups gems by type.
 */
export async function getEnhancedHiddenGems(region?: Region): Promise<HiddenGemsData> {
  const regionFilter = getRegionWhereClause(region);
  
  // Get all tournament data (including lock_chip for CX blades)
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    lock_chip: string | null;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, lock_chip, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date ASC
  `);

  if (rows.length === 0) {
    return {
      underused: [],
      forgotten: [],
      rising: [],
      counterMeta: [],
      eraAnalysis: {
        early: { start: '', end: '', topCombos: [] },
        mid: { start: '', end: '', topCombos: [] },
        current: { start: '', end: '', topCombos: [] },
      },
    };
  }

  const now = new Date();
  const dates = rows.map(r => new Date(r.tournament_date));
  const earliestDate = new Date(Math.min(...dates.map(d => d.getTime())));
  const latestDate = new Date(Math.max(...dates.map(d => d.getTime())));
  
  // Define eras based on data span
  const totalDays = (latestDate.getTime() - earliestDate.getTime()) / (24 * 60 * 60 * 1000);
  const eraLength = totalDays / 3;
  
  const earlyEnd = new Date(earliestDate.getTime() + eraLength * 24 * 60 * 60 * 1000);
  const midEnd = new Date(earliestDate.getTime() + eraLength * 2 * 24 * 60 * 60 * 1000);
  
  // Time windows for analysis
  const sixtyDaysAgo = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);
  const ninetyDaysAgo = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
  const oneEightyDaysAgo = new Date(now.getTime() - 180 * 24 * 60 * 60 * 1000);

  // Aggregate all combo stats with era tracking
  interface ComboData {
    blade: string;
    ratchet: string;
    bit: string;
    lockChip: string | null;
    totalUses: number;
    wins: number;
    topThree: number;
    totalScore: number;
    placements: number[];
    // Era data
    earlyUses: number;
    earlyWins: number;
    earlyScore: number;
    midUses: number;
    midWins: number;
    midScore: number;
    currentUses: number;
    currentWins: number;
    currentScore: number;
    // Recent data
    last60Uses: number;
    last60Wins: number;
    last60Score: number;
    last90Uses: number;
    // First and last seen
    firstSeen: Date;
    lastSeen: Date;
    peakDate: Date;
    peakScore: number;
  }

  const comboData: Record<string, ComboData> = {};
  const bladeData: Record<string, { uses: number; wins: number; score: number; recentUses: number; recentWins: number }> = {};

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const lockChip = row.lock_chip;
    // Include lock_chip in key for CX blades
    const key = lockChip ? `${lockChip}|${blade}|${ratchet}|${bit}` : `${blade}|${ratchet}|${bit}`;
    const tournamentDate = new Date(row.tournament_date);
    const points = getPlacementScore(row.place, row.stage);
    const weight = calculateRecencyWeight(tournamentDate, now);
    const weightedScore = points * weight;

    // Initialize combo data
    if (!comboData[key]) {
      comboData[key] = {
        blade,
        ratchet: ratchet,
        bit: bit,
        lockChip: lockChip,
        totalUses: 0,
        wins: 0,
        topThree: 0,
        totalScore: 0,
        placements: [],
        earlyUses: 0, earlyWins: 0, earlyScore: 0,
        midUses: 0, midWins: 0, midScore: 0,
        currentUses: 0, currentWins: 0, currentScore: 0,
        last60Uses: 0, last60Wins: 0, last60Score: 0,
        last90Uses: 0,
        firstSeen: tournamentDate,
        lastSeen: tournamentDate,
        peakDate: tournamentDate,
        peakScore: weightedScore,
      };
    }

    // Initialize blade data
    if (!bladeData[blade]) {
      bladeData[blade] = { uses: 0, wins: 0, score: 0, recentUses: 0, recentWins: 0 };
    }

    const combo = comboData[key];
    const bladeStats = bladeData[blade];

    // Update totals
    combo.totalUses++;
    combo.totalScore += weightedScore;
    combo.placements.push(row.place);
    bladeStats.uses++;
    bladeStats.score += weightedScore;

    if (row.place === 1) {
      combo.wins++;
      bladeStats.wins++;
    }
    if (row.place <= 3) combo.topThree++;

    // Update dates
    if (tournamentDate < combo.firstSeen) combo.firstSeen = tournamentDate;
    if (tournamentDate > combo.lastSeen) combo.lastSeen = tournamentDate;
    if (weightedScore > combo.peakScore) {
      combo.peakScore = weightedScore;
      combo.peakDate = tournamentDate;
    }

    // Era tracking
    if (tournamentDate <= earlyEnd) {
      combo.earlyUses++;
      combo.earlyScore += points;
      if (row.place === 1) combo.earlyWins++;
    } else if (tournamentDate <= midEnd) {
      combo.midUses++;
      combo.midScore += points;
      if (row.place === 1) combo.midWins++;
    } else {
      combo.currentUses++;
      combo.currentScore += points;
      if (row.place === 1) combo.currentWins++;
    }

    // Recent tracking
    if (tournamentDate >= sixtyDaysAgo) {
      combo.last60Uses++;
      combo.last60Score += points;
      bladeStats.recentUses++;
      if (row.place === 1) {
        combo.last60Wins++;
        bladeStats.recentWins++;
      }
    }
    if (tournamentDate >= ninetyDaysAgo) {
      combo.last90Uses++;
    }
  }

  // Get current top meta combos (for counter-meta analysis)
  const currentTopCombos = Object.entries(comboData)
    .filter(([_, c]) => c.currentUses >= 3)
    .sort((a, b) => b[1].currentScore - a[1].currentScore)
    .slice(0, 10)
    .map(([key, _]) => key);

  const topMetaBlades = new Set(currentTopCombos.map(k => k.split('|')[0]));

  // Helper to determine peak era
  const getPeakEra = (combo: ComboData): MetaEra => {
    const scores = [
      { era: 'early' as MetaEra, score: combo.earlyScore },
      { era: 'mid' as MetaEra, score: combo.midScore },
      { era: 'current' as MetaEra, score: combo.currentScore },
    ];
    return scores.sort((a, b) => b.score - a.score)[0].era;
  };

  // Helper to determine trend
  const getTrend = (combo: ComboData): EnhancedGem['trend'] => {
    const wasActive = combo.totalUses - combo.last90Uses > 0;
    const isActive = combo.last60Uses > 0;
    const wasStrong = (combo.earlyUses + combo.midUses) >= 3 && 
                      (combo.earlyWins + combo.midWins) / (combo.earlyUses + combo.midUses) >= 0.25;
    
    if (!wasActive && isActive) return 'rising';
    if (wasStrong && !isActive && combo.last90Uses > 0) return 'returning';
    if (combo.last60Uses >= 2 && combo.currentScore > combo.midScore) return 'rising';
    if (combo.last60Uses === 0 && combo.last90Uses === 0) return 'falling';
    return 'stable';
  };

  // 1. UNDERUSED BLADES - Good blades that aren't seeing much current play
  const underused: EnhancedGem[] = [];
  for (const [blade, stats] of Object.entries(bladeData)) {
    // Skip if it's already top meta
    if (topMetaBlades.has(blade)) continue;
    
    // Need decent historical performance but low recent usage
    const winRate = stats.uses > 0 ? stats.wins / stats.uses : 0;
    if (stats.uses < 5 || winRate < 0.20) continue;
    if (stats.recentUses > 5) continue; // Too much recent usage = not underused
    
    // Find best combo for this blade
    const bladeCombos = Object.entries(comboData)
      .filter(([_, c]) => c.blade === blade)
      .sort((a, b) => b[1].totalScore - a[1].currentScore);
    
    if (bladeCombos.length === 0) continue;
    const [bestKey, bestCombo] = bladeCombos[0];
    
    // Data quality based on sample size
    const dataQuality: 'strong' | 'moderate' | 'limited' =
      stats.uses >= 15 ? 'strong' : stats.uses >= 8 ? 'moderate' : 'limited';
    
    underused.push({
      blade: bestCombo.blade,
      ratchet: bestCombo.ratchet,
      bit: bestCombo.bit,
      lockChip: bestCombo.lockChip,
      combo: buildComboName(bestCombo.blade, bestCombo.ratchet, bestCombo.bit, bestCombo.lockChip),
      category: 'underused',
      // Use blade-level stats for consistency with the reason text
      totalUses: stats.uses,
      wins: stats.wins,
      winRate: winRate,
      avgPlacement: bestCombo.placements.reduce((a, b) => a + b, 0) / bestCombo.placements.length,
      score: stats.score,
      reason: stats.recentUses === 0
        ? `${blade} has ${(winRate * 100).toFixed(0)}% win rate with no recent appearances`
        : `${blade} has ${(winRate * 100).toFixed(0)}% win rate but only ${stats.recentUses} recent appearance${stats.recentUses === 1 ? '' : 's'}`,
      insight: `Best combo shown above. This blade is undervalued in current meta.`,
      dataQuality,
      peakEra: getPeakEra(bestCombo),
      currentRank: undefined,
      recentUses: stats.recentUses,
      recentWinRate: stats.recentUses > 0 ? stats.recentWins / stats.recentUses : 0,
      trend: getTrend(bestCombo),
    });
  }

  // 2. FORGOTTEN CHAMPIONS - Combos that won before but fell off
  const forgotten: EnhancedGem[] = [];
  for (const [key, combo] of Object.entries(comboData)) {
    // Must have won tournaments in early/mid era
    const pastWins = combo.earlyWins + combo.midWins;
    const pastUses = combo.earlyUses + combo.midUses;
    if (pastWins < 1 || pastUses < 2) continue;
    
    // Must have little/no current presence
    if (combo.currentUses > 3) continue;
    
    // Skip if it's active recently
    if (combo.last60Uses > 2) continue;
    
    const pastWinRate = pastWins / pastUses;

    // Data quality based on past sample size
    const dataQuality: 'strong' | 'moderate' | 'limited' =
      pastUses >= 8 ? 'strong' : pastUses >= 4 ? 'moderate' : 'limited';

    forgotten.push({
      blade: combo.blade,
      ratchet: combo.ratchet,
      bit: combo.bit,
      lockChip: combo.lockChip,
      combo: buildComboName(combo.blade, combo.ratchet, combo.bit, combo.lockChip),
      category: 'forgotten',
      // Show past-era stats (when they were champions) for consistency with reason text
      totalUses: pastUses,
      wins: pastWins,
      winRate: pastWinRate,
      avgPlacement: combo.placements.reduce((a, b) => a + b, 0) / combo.placements.length,
      score: combo.totalScore,
      reason: `Won ${pastWins} tournament${pastWins === 1 ? '' : 's'} in ${getPeakEra(combo)} era`,
      insight: `Former champion with ${(pastWinRate * 100).toFixed(0)}% past win rate - meta may have moved on prematurely`,
      dataQuality,
      peakEra: getPeakEra(combo),
      peakDate: combo.peakDate.toISOString().split('T')[0],
      recentUses: combo.last60Uses,
      recentWinRate: combo.last60Uses > 0 ? combo.last60Wins / combo.last60Uses : 0,
      trend: combo.last90Uses > 0 ? 'returning' : 'stable',
    });
  }

  // 3. RISING NEWCOMERS - Parts that just started appearing and performing well
  const rising: EnhancedGem[] = [];
  for (const [key, combo] of Object.entries(comboData)) {
    // Must be relatively new (mostly current era)
    const currentRatio = combo.currentUses / Math.max(1, combo.totalUses);
    if (currentRatio < 0.6) continue;
    
    // Must have recent activity
    if (combo.last60Uses < 2) continue;
    
    // Must have good recent performance
    const recentWinRate = combo.last60Uses > 0 ? combo.last60Wins / combo.last60Uses : 0;
    if (recentWinRate < 0.20 && combo.last60Wins < 1) continue;
    
    // Skip if already too mainstream
    if (combo.totalUses > 15) continue;

    // Data quality based on recent sample size (rising combos have less data by definition)
    const dataQuality: 'strong' | 'moderate' | 'limited' =
      combo.last60Uses >= 6 ? 'strong' : combo.last60Uses >= 3 ? 'moderate' : 'limited';

    rising.push({
      blade: combo.blade,
      ratchet: combo.ratchet,
      bit: combo.bit,
      lockChip: combo.lockChip,
      combo: buildComboName(combo.blade, combo.ratchet, combo.bit, combo.lockChip),
      category: 'rising',
      // Show recent stats since these are new/rising combos
      totalUses: combo.last60Uses,
      wins: combo.last60Wins,
      winRate: recentWinRate,
      avgPlacement: combo.placements.reduce((a, b) => a + b, 0) / combo.placements.length,
      score: combo.last60Score,
      reason: combo.last60Wins === 0
        ? `New combo showing potential in last 60 days`
        : `New combo with ${combo.last60Wins} win${combo.last60Wins === 1 ? '' : 's'} in last 60 days`,
      insight: `Emerging pick gaining traction - could be next meta contender`,
      dataQuality,
      peakEra: 'current',
      recentUses: combo.last60Uses,
      recentWinRate,
      trend: 'rising',
    });
  }

  // 4. COUNTER-META PICKS - Combos that beat top meta when they face them
  // This requires tournament-level data to know who faced who, which we don't have directly
  // So we'll approximate: combos that do well but use different blades than top meta
  const counterMeta: EnhancedGem[] = [];
  for (const [key, combo] of Object.entries(comboData)) {
    // Must not use a top meta blade
    if (topMetaBlades.has(combo.blade)) continue;
    
    // Must have good current performance
    if (combo.currentUses < 2) continue;
    const currentWinRate = combo.currentUses > 0 ? combo.currentWins / combo.currentUses : 0;
    if (currentWinRate < 0.25 && combo.currentWins < 1) continue;
    
    // Must be active recently
    if (combo.last90Uses < 2) continue;
    
    // Confidence for counter-meta: needs proven results against the meta
    // Data quality based on current era sample size
    const dataQuality: 'strong' | 'moderate' | 'limited' =
      combo.currentUses >= 6 ? 'strong' : combo.currentUses >= 3 ? 'moderate' : 'limited';

    counterMeta.push({
      blade: combo.blade,
      ratchet: combo.ratchet,
      bit: combo.bit,
      lockChip: combo.lockChip,
      combo: buildComboName(combo.blade, combo.ratchet, combo.bit, combo.lockChip),
      category: 'counter-meta',
      // Show current era stats since that's what makes them counter-meta
      totalUses: combo.currentUses,
      wins: combo.currentWins,
      winRate: currentWinRate,
      avgPlacement: combo.placements.reduce((a, b) => a + b, 0) / combo.placements.length,
      score: combo.currentScore,
      reason: combo.currentWins > 0
        ? `Non-meta blade with ${combo.currentWins} win${combo.currentWins === 1 ? '' : 's'} in current era`
        : `Non-meta blade placing well in current era`,
      insight: `Could exploit weaknesses in popular meta choices`,
      dataQuality,
      beatsTopMeta: [...topMetaBlades].slice(0, 3), // Approximation
      recentUses: combo.last60Uses,
      recentWinRate: combo.last60Uses > 0 ? combo.last60Wins / combo.last60Uses : 0,
      trend: getTrend(combo),
    });
  }

  // Sort each category by relevant metrics
  underused.sort((a, b) => b.winRate - a.winRate || b.totalUses - a.totalUses);
  forgotten.sort((a, b) => b.wins - a.wins || b.winRate - a.winRate);
  rising.sort((a, b) => b.recentWinRate - a.recentWinRate || b.recentUses - a.recentUses);
  counterMeta.sort((a, b) => b.wins - a.wins || b.winRate - a.winRate);

  // Get era top combos for context
  const getEraTopCombos = (era: 'early' | 'mid' | 'current') => {
    return Object.entries(comboData)
      .filter(([_, c]) => {
        if (era === 'early') return c.earlyUses >= 2;
        if (era === 'mid') return c.midUses >= 2;
        return c.currentUses >= 2;
      })
      .sort((a, b) => {
        if (era === 'early') return b[1].earlyScore - a[1].earlyScore;
        if (era === 'mid') return b[1].midScore - a[1].midScore;
        return b[1].currentScore - a[1].currentScore;
      })
      .slice(0, 5)
      .map(([_, c]) => buildComboName(c.blade, c.ratchet, c.bit, c.lockChip));
  };

  return {
    underused: underused.slice(0, 8),
    forgotten: forgotten.slice(0, 8),
    rising: rising.slice(0, 8),
    counterMeta: counterMeta.slice(0, 8),
    eraAnalysis: {
      early: {
        start: earliestDate.toISOString().split('T')[0],
        end: earlyEnd.toISOString().split('T')[0],
        topCombos: getEraTopCombos('early'),
      },
      mid: {
        start: earlyEnd.toISOString().split('T')[0],
        end: midEnd.toISOString().split('T')[0],
        topCombos: getEraTopCombos('mid'),
      },
      current: {
        start: midEnd.toISOString().split('T')[0],
        end: latestDate.toISOString().split('T')[0],
        topCombos: getEraTopCombos('current'),
      },
    },
  };
}

/**
 * Meta evolution data for visualization
 */
export interface MetaEvolutionData {
  eras: Array<{
    name: string;
    startDate: string;
    endDate: string;
    topBlades: Array<{ blade: string; score: number; wins: number; uses: number }>;
    topCombos: Array<{ combo: string; score: number; wins: number; uses: number }>;
  }>;
  timeline: Array<{
    month: string;
    topBlade: string;
    topCombo: string;
    tournaments: number;
  }>;
  bladeJourneys: Array<{
    blade: string;
    monthlyRanks: Array<{ month: string; rank: number | null }>;
    peakRank: number;
    peakMonth: string;
    currentRank: number | null;
    trend: 'rising' | 'falling' | 'stable' | 'new' | 'gone';
  }>;
}

/**
 * Get meta evolution data showing how the meta changed over time.
 * Divides history into eras and tracks blade popularity.
 */
export async function getMetaEvolution(region?: Region): Promise<MetaEvolutionData> {
  const regionFilter = getRegionWhereClause(region);
  
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date ASC
  `);

  if (rows.length === 0) {
    return { eras: [], timeline: [], bladeJourneys: [] };
  }

  const now = new Date();
  const dates = rows.map(r => new Date(r.tournament_date));
  const earliestDate = new Date(Math.min(...dates.map(d => d.getTime())));
  const latestDate = new Date(Math.max(...dates.map(d => d.getTime())));
  
  // Divide into 4 eras for more granularity
  const totalDays = (latestDate.getTime() - earliestDate.getTime()) / (24 * 60 * 60 * 1000);
  const eraLength = totalDays / 4;
  
  const eraBoundaries = [
    earliestDate,
    new Date(earliestDate.getTime() + eraLength * 24 * 60 * 60 * 1000),
    new Date(earliestDate.getTime() + eraLength * 2 * 24 * 60 * 60 * 1000),
    new Date(earliestDate.getTime() + eraLength * 3 * 24 * 60 * 60 * 1000),
    latestDate,
  ];
  
  const eraNames = ['Early Meta', 'Rising Meta', 'Established Meta', 'Current Meta'];

  // Track data by era
  interface EraData {
    blades: Record<string, { score: number; wins: number; uses: number }>;
    combos: Record<string, { score: number; wins: number; uses: number }>;
    tournaments: Set<string>;
  }
  
  const eraData: EraData[] = Array.from({ length: 4 }, () => ({
    blades: {},
    combos: {},
    tournaments: new Set(),
  }));

  // Track data by month for timeline
  const monthlyData: Record<string, {
    blades: Record<string, number>;
    combos: Record<string, number>;
    tournaments: Set<string>;
  }> = {};

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const combo = `${blade} ${ratchet} ${bit}`;
    const tournamentDate = new Date(row.tournament_date);
    const points = getPlacementScore(row.place, row.stage);
    const monthKey = `${tournamentDate.getFullYear()}-${String(tournamentDate.getMonth() + 1).padStart(2, '0')}`;
    const tournamentKey = row.tournament_date;

    // Determine era
    let eraIndex = 0;
    for (let i = 1; i < eraBoundaries.length; i++) {
      if (tournamentDate <= eraBoundaries[i]) {
        eraIndex = i - 1;
        break;
      }
    }

    const era = eraData[eraIndex];
    
    // Update era blade data
    if (!era.blades[blade]) era.blades[blade] = { score: 0, wins: 0, uses: 0 };
    era.blades[blade].score += points;
    era.blades[blade].uses++;
    if (row.place === 1) era.blades[blade].wins++;
    
    // Update era combo data
    if (!era.combos[combo]) era.combos[combo] = { score: 0, wins: 0, uses: 0 };
    era.combos[combo].score += points;
    era.combos[combo].uses++;
    if (row.place === 1) era.combos[combo].wins++;
    
    era.tournaments.add(tournamentKey);

    // Update monthly data
    if (!monthlyData[monthKey]) {
      monthlyData[monthKey] = { blades: {}, combos: {}, tournaments: new Set() };
    }
    const month = monthlyData[monthKey];
    month.blades[blade] = (month.blades[blade] || 0) + points;
    month.combos[combo] = (month.combos[combo] || 0) + points;
    month.tournaments.add(tournamentKey);
  }

  // Build eras array
  const eras = eraData.map((era, i) => {
    const topBlades = Object.entries(era.blades)
      .sort((a, b) => b[1].score - a[1].score)
      .slice(0, 5)
      .map(([blade, data]) => ({ blade, ...data }));
    
    const topCombos = Object.entries(era.combos)
      .sort((a, b) => b[1].score - a[1].score)
      .slice(0, 3)
      .map(([combo, data]) => ({ combo, ...data }));

    return {
      name: eraNames[i],
      startDate: eraBoundaries[i].toISOString().split('T')[0],
      endDate: eraBoundaries[i + 1].toISOString().split('T')[0],
      topBlades,
      topCombos,
    };
  });

  // Build timeline
  const sortedMonths = Object.keys(monthlyData).sort();
  const timeline = sortedMonths.map(month => {
    const data = monthlyData[month];
    const topBlade = Object.entries(data.blades).sort((a, b) => b[1] - a[1])[0];
    const topCombo = Object.entries(data.combos).sort((a, b) => b[1] - a[1])[0];
    
    return {
      month,
      topBlade: topBlade ? topBlade[0] : '',
      topCombo: topCombo ? topCombo[0] : '',
      tournaments: data.tournaments.size,
    };
  });

  // Build blade journeys (track rank changes over time)
  const allBlades = new Set<string>();
  for (const month of sortedMonths) {
    Object.keys(monthlyData[month].blades).forEach(b => allBlades.add(b));
  }

  // Get monthly rankings for each blade
  const monthlyRankings: Record<string, Record<string, number>> = {};
  for (const month of sortedMonths) {
    const sorted = Object.entries(monthlyData[month].blades)
      .sort((a, b) => b[1] - a[1]);
    monthlyRankings[month] = {};
    sorted.forEach(([blade], idx) => {
      monthlyRankings[month][blade] = idx + 1;
    });
  }

  // Track journeys for top blades (those that were ever in top 5)
  const notableBlades = new Set<string>();
  for (const month of sortedMonths) {
    const topInMonth = Object.entries(monthlyData[month].blades)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([b]) => b);
    topInMonth.forEach(b => notableBlades.add(b));
  }

  const bladeJourneys = [...notableBlades].map(blade => {
    const monthlyRanks = sortedMonths.map(month => ({
      month,
      rank: monthlyRankings[month][blade] ?? null,
    }));

    // Find peak
    const rankedMonths = monthlyRanks.filter(m => m.rank !== null);
    const peakMonth = rankedMonths.length > 0
      ? rankedMonths.reduce((best, curr) => (curr.rank! < (best.rank ?? 999) ? curr : best))
      : { month: '', rank: null };
    
    const currentRank = monthlyRanks[monthlyRanks.length - 1]?.rank ?? null;
    const previousRank = monthlyRanks.length > 1 ? monthlyRanks[monthlyRanks.length - 2]?.rank : null;

    // Determine trend
    let trend: 'rising' | 'falling' | 'stable' | 'new' | 'gone' = 'stable';
    const firstAppearance = monthlyRanks.findIndex(m => m.rank !== null);
    const lastAppearance = monthlyRanks.length - 1 - [...monthlyRanks].reverse().findIndex(m => m.rank !== null);
    
    if (firstAppearance === sortedMonths.length - 1 || firstAppearance === sortedMonths.length - 2) {
      trend = 'new';
    } else if (currentRank === null && lastAppearance < sortedMonths.length - 2) {
      trend = 'gone';
    } else if (currentRank !== null && previousRank !== null) {
      if (currentRank < previousRank - 1) trend = 'rising';
      else if (currentRank > previousRank + 1) trend = 'falling';
    }

    return {
      blade,
      monthlyRanks,
      peakRank: peakMonth.rank ?? 99,
      peakMonth: peakMonth.month,
      currentRank,
      trend,
    };
  }).sort((a, b) => a.peakRank - b.peakRank).slice(0, 10);

  return { eras, timeline, bladeJourneys };
}

/**
 * Get rising trends - parts/combos with accelerating performance.
 */
export async function getRisingTrends(weeks = 8, region?: Region): Promise<RisingTrend[]> {
  const regionFilter = getRegionWhereClause(region);
  
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const now = new Date();
  
  // Track weekly scores for blades
  const bladeWeekly: Record<string, { scores: number[]; uses: number[] }> = {};
  
  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const tournamentDate = new Date(row.tournament_date);
    const weeksAgo = Math.floor((now.getTime() - tournamentDate.getTime()) / (7 * 24 * 60 * 60 * 1000));
    
    if (weeksAgo < 0 || weeksAgo >= weeks) continue;
    
    if (!bladeWeekly[blade]) {
      bladeWeekly[blade] = { 
        scores: new Array(weeks).fill(0), 
        uses: new Array(weeks).fill(0) 
      };
    }
    
    const weekIndex = weeks - 1 - weeksAgo;
    const points = getPlacementScore(row.place, row.stage);
    bladeWeekly[blade].scores[weekIndex] += points;
    bladeWeekly[blade].uses[weekIndex]++;
  }

  const trends: RisingTrend[] = [];
  
  for (const [name, data] of Object.entries(bladeWeekly)) {
    const totalUses = data.uses.reduce((a, b) => a + b, 0);
    if (totalUses < 3) continue; // Need minimum data
    
    // Calculate growth rate (compare last 4 weeks to first 4 weeks)
    const recentSum = data.scores.slice(-4).reduce((a, b) => a + b, 0);
    const olderSum = data.scores.slice(0, 4).reduce((a, b) => a + b, 0);
    
    const growthRate = olderSum > 0 ? ((recentSum - olderSum) / olderSum) * 100 : 
                       recentSum > 0 ? 100 : 0;
    
    // Calculate momentum (acceleration - is growth speeding up?)
    const midSum = data.scores.slice(2, 6).reduce((a, b) => a + b, 0);
    const earlyGrowth = midSum - olderSum;
    const lateGrowth = recentSum - midSum;
    const momentum = lateGrowth - earlyGrowth;
    
    // Project future score based on trend
    const avgRecentScore = recentSum / 4;
    const projectedScore = avgRecentScore * (1 + growthRate / 100);
    
    trends.push({
      name,
      type: 'blade',
      weeklyScores: data.scores,
      weeklyUses: data.uses,
      growthRate,
      momentum,
      projectedScore,
    });
  }

  // Sort by growth rate, filter for positive growth
  return trends
    .filter(t => t.growthRate > 10) // At least 10% growth
    .sort((a, b) => b.growthRate - a.growthRate)
    .slice(0, 15);
}

/**
 * Analyze parts for undervalued potential.
 */
export async function getPartAnalysis(partType: 'blade' | 'ratchet' | 'bit', region?: Region): Promise<PartAnalysis[]> {
  const regionFilter = getRegionWhereClause(region);
  
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
  `);

  const now = new Date();
  
  // Get top parts for synergy calculation
  const partCounts: Record<string, number> = {};
  for (const row of rows) {
    const part = partType === 'blade' ? normalizeBladeDisplay(row.blade) : 
                 partType === 'ratchet' ? normalizeRatchet(row.ratchet) : normalizeBit(row.bit);
    partCounts[part] = (partCounts[part] || 0) + 1;
  }
  const topParts = Object.entries(partCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([name]) => name);

  // Aggregate part stats
  const partStats: Record<string, {
    uses: number;
    wins: number;
    totalScore: number;
    placements: number[];
    combos: Set<string>;
    topPartPairings: number;
  }> = {};

  for (const row of rows) {
    const part = partType === 'blade' ? normalizeBladeDisplay(row.blade) : 
                 partType === 'ratchet' ? normalizeRatchet(row.ratchet) : normalizeBit(row.bit);
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const comboKey = `${blade}|${ratchet}|${bit}`;
    
    if (!partStats[part]) {
      partStats[part] = { 
        uses: 0, wins: 0, totalScore: 0, placements: [], 
        combos: new Set(), topPartPairings: 0 
      };
    }
    
    const stats = partStats[part];
    const points = getPlacementScore(row.place, row.stage);
    const weight = calculateRecencyWeight(new Date(row.tournament_date), now);
    
    stats.uses++;
    stats.totalScore += points * weight;
    stats.placements.push(row.place);
    stats.combos.add(comboKey);
    if (row.place === 1) stats.wins++;
    
    // Check if paired with top parts
    const otherParts = partType === 'blade' ? [ratchet, bit] :
                       partType === 'ratchet' ? [blade, bit] : [blade, ratchet];
    if (otherParts.some(p => topParts.includes(p))) {
      stats.topPartPairings++;
    }
  }

  const analysis: PartAnalysis[] = [];
  
  for (const [name, stats] of Object.entries(partStats)) {
    if (stats.uses < 3) continue;
    
    const winRate = stats.wins / stats.uses;
    const avgPlacement = stats.placements.reduce((a, b) => a + b, 0) / stats.placements.length;
    const versatility = stats.combos.size;
    const synergyScore = stats.topPartPairings / stats.uses;
    
    // Calculate consistency (inverse of variance)
    const mean = avgPlacement;
    const variance = stats.placements.reduce((sum, p) => sum + Math.pow(p - mean, 2), 0) / stats.placements.length;
    const consistency = 1 / (1 + Math.sqrt(variance));
    
    analysis.push({
      name,
      type: partType,
      uses: stats.uses,
      winRate,
      avgPlacement,
      versatility,
      synergyScore,
      consistency,
    });
  }

  return analysis.sort((a, b) => b.winRate - a.winRate);
}

/**
 * Get variance analysis for combos - find high ceiling/high variance options.
 */
export async function getVarianceAnalysis(minUses = 3, region?: Region): Promise<VarianceData[]> {
  const regionFilter = getRegionWhereClause(region);
  
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
  `);

  const now = new Date();
  
  // Aggregate combo scores
  const comboScores: Record<string, number[]> = {};

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const key = `${blade}|${ratchet}|${bit}`;
    const points = getPlacementScore(row.place, row.stage);
    const weight = calculateRecencyWeight(new Date(row.tournament_date), now);
    
    if (!comboScores[key]) comboScores[key] = [];
    comboScores[key].push(points * weight);
  }

  const varianceData: VarianceData[] = [];
  
  for (const [key, scores] of Object.entries(comboScores)) {
    if (scores.length < minUses) continue;
    
    const [blade, ratchet, bit] = key.split('|');
    const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
    const variance = scores.reduce((sum, s) => sum + Math.pow(s - avgScore, 2), 0) / scores.length;
    const stdDev = Math.sqrt(variance);
    const ceiling = Math.max(...scores);
    const floor = Math.min(...scores);
    
    varianceData.push({
      combo: `${blade} ${ratchet} ${bit}`,
      blade,
      ratchet,
      bit,
      uses: scores.length,
      avgScore,
      variance,
      stdDev,
      ceiling,
      floor,
      highVariance: stdDev > avgScore * 0.5, // High variance if stdDev > 50% of mean
    });
  }

  return varianceData.sort((a, b) => b.variance - a.variance);
}

/**
 * Get correlation data between parts for heatmap.
 */
export async function getPartCorrelations(region?: Region): Promise<{
  blades: string[];
  ratchets: string[];
  bits: string[];
  bladeRatchetMatrix: number[][];
  bladeBitMatrix: number[][];
}> {
  const regionFilter = getRegionWhereClause(region);
  
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
  }>(`
    SELECT blade, ratchet, bit, place
    FROM combo_usage
    WHERE 1=1${regionFilter}
  `);

  // Get top parts
  const bladeCounts: Record<string, number> = {};
  const ratchetCounts: Record<string, number> = {};
  const bitCounts: Record<string, number> = {};
  
  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    bladeCounts[blade] = (bladeCounts[blade] || 0) + 1;
    ratchetCounts[ratchet] = (ratchetCounts[ratchet] || 0) + 1;
    bitCounts[bit] = (bitCounts[bit] || 0) + 1;
  }

  const topBlades = Object.entries(bladeCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([n]) => n);
  const topRatchets = Object.entries(ratchetCounts).sort((a, b) => b[1] - a[1]).slice(0, 8).map(([n]) => n);
  const topBits = Object.entries(bitCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([n]) => n);

  // Build correlation matrices (win rate when paired)
  const bladeRatchetWins: Record<string, { wins: number; total: number }> = {};
  const bladeBitWins: Record<string, { wins: number; total: number }> = {};

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    if (!topBlades.includes(blade)) continue;
    
    if (topRatchets.includes(ratchet)) {
      const key = `${blade}|${ratchet}`;
      if (!bladeRatchetWins[key]) bladeRatchetWins[key] = { wins: 0, total: 0 };
      bladeRatchetWins[key].total++;
      if (row.place === 1) bladeRatchetWins[key].wins++;
    }
    
    if (topBits.includes(bit)) {
      const key = `${blade}|${bit}`;
      if (!bladeBitWins[key]) bladeBitWins[key] = { wins: 0, total: 0 };
      bladeBitWins[key].total++;
      if (row.place === 1) bladeBitWins[key].wins++;
    }
  }

  // Build matrices
  const bladeRatchetMatrix = topBlades.map(blade => 
    topRatchets.map(ratchet => {
      const key = `${blade}|${ratchet}`;
      const data = bladeRatchetWins[key];
      return data ? data.wins / data.total : 0;
    })
  );

  const bladeBitMatrix = topBlades.map(blade => 
    topBits.map(bit => {
      const key = `${blade}|${bit}`;
      const data = bladeBitWins[key];
      return data ? data.wins / data.total : 0;
    })
  );

  return {
    blades: topBlades,
    ratchets: topRatchets,
    bits: topBits,
    bladeRatchetMatrix,
    bladeBitMatrix,
  };
}


/**
 * Meta spotlight data - current champion and movers.
 */
export interface MetaSpotlightData {
  champion: {
    combo: string;
    blade: string;
    ratchet: string;
    bit: string;
    score: number;
    uses: number;
    winRate: number;
    dominance: number;
  } | null;
  risers: Array<{
    combo: string;
    blade: string;
    ratchet: string;
    bit: string;
    change: number;
    newRank: number;
  }>;
  fallers: Array<{
    combo: string;
    blade: string;
    ratchet: string;
    bit: string;
    change: number;
    newRank: number;
  }>;
  lastTournamentDate: string | null;
  isStale: boolean;
  dataSource: 'recent' | 'extended';
}

/**
 * Get meta spotlight data - the current champion and movers.
 * Uses the 30 days leading up to the most recent tournament in the region.
 */
export async function getMetaSpotlight(region?: Region): Promise<MetaSpotlightData> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  // Get the most recent tournament date for this region
  const lastTournamentDate = rows.length > 0 ? rows[0].tournament_date : null;

  if (rows.length === 0 || !lastTournamentDate) {
    return { champion: null, risers: [], fallers: [], lastTournamentDate: null, isStale: false, dataSource: 'recent' };
  }

  // Use the most recent tournament date as the anchor point
  const anchorDate = new Date(lastTournamentDate);
  const now = new Date();
  const recentDays = 30;
  const olderDays = 30;

  // 30 days before the most recent tournament
  const recentCutoff = new Date(anchorDate.getTime() - recentDays * 24 * 60 * 60 * 1000);
  // 60 days before the most recent tournament (for comparison)
  const olderCutoff = new Date(anchorDate.getTime() - (recentDays + olderDays) * 24 * 60 * 60 * 1000);

  // Check if the data is stale (most recent tournament is more than 30 days ago from today)
  const todayCutoff = new Date(now.getTime() - recentDays * 24 * 60 * 60 * 1000);
  const isStale = anchorDate < todayCutoff;

  // Helper function to calculate stats for a set of rows
  function calculateStats(dataRows: typeof rows, minUses: number) {
    const comboStats: Record<string, {
      blade: string;
      ratchet: string;
      bit: string;
      score: number;
      uses: number;
      wins: number;
    }> = {};

    let totalPlacements = 0;

    for (const row of dataRows) {
      const blade = normalizeBladeDisplay(row.blade);
      const ratchet = normalizeRatchet(row.ratchet);
      const bit = normalizeBit(row.bit);
      const combo = `${blade} ${ratchet} ${bit}`;
      const points = getPlacementScore(row.place, row.stage);

      if (!comboStats[combo]) {
        comboStats[combo] = {
          blade: blade,
          ratchet: ratchet,
          bit: bit,
          score: 0,
          uses: 0,
          wins: 0,
        };
      }

      comboStats[combo].score += points;
      comboStats[combo].uses++;
      if (row.place === 1) comboStats[combo].wins++;
      totalPlacements++;
    }

    const sortedCombos = Object.entries(comboStats)
      .map(([combo, stats]) => ({ combo, ...stats }))
      .filter(c => c.uses >= minUses)
      .sort((a, b) => b.score - a.score);

    const champion = sortedCombos[0] ? {
      combo: sortedCombos[0].combo,
      blade: sortedCombos[0].blade,
      ratchet: sortedCombos[0].ratchet,
      bit: sortedCombos[0].bit,
      score: sortedCombos[0].score,
      uses: sortedCombos[0].uses,
      winRate: sortedCombos[0].uses > 0 ? Math.round((sortedCombos[0].wins / sortedCombos[0].uses) * 100) : 0,
      dominance: totalPlacements > 0 ? Math.round((sortedCombos[0].uses / totalPlacements) * 100) : 0,
    } : null;

    return { champion, comboStats, totalPlacements };
  }

  // Filter to the 30-day window ending at the most recent tournament
  const recentRows = rows.filter(row => {
    const d = new Date(row.tournament_date);
    return d >= recentCutoff && d <= anchorDate;
  });

  console.log('[MetaSpotlight DEBUG] Total rows:', rows.length);
  console.log('[MetaSpotlight DEBUG] Anchor date:', anchorDate, 'Recent cutoff:', recentCutoff);
  console.log('[MetaSpotlight DEBUG] Recent rows count:', recentRows.length);
  if (rows.length > 0) {
    console.log('[MetaSpotlight DEBUG] First row date:', rows[0].tournament_date, 'Parsed:', new Date(rows[0].tournament_date));
  }

  const { champion } = calculateStats(recentRows, 2);
  console.log('[MetaSpotlight DEBUG] Champion result:', champion);

  // Calculate risers and fallers by comparing recent 30 days vs previous 30 days
  const olderRows = rows.filter(row => {
    const d = new Date(row.tournament_date);
    return d >= olderCutoff && d < recentCutoff;
  });

  let risers: MetaSpotlightData['risers'] = [];
  let fallers: MetaSpotlightData['fallers'] = [];

  if (olderRows.length > 0) {
    const recentStats = calculateStats(recentRows, 1);
    const olderStats = calculateStats(olderRows, 1);

    const recentRanking = Object.entries(recentStats.comboStats)
      .sort((a, b) => b[1].score - a[1].score)
      .map(([combo, stats], i) => ({ combo, ...stats, rank: i + 1 }));

    const olderRanking = Object.entries(olderStats.comboStats)
      .sort((a, b) => b[1].score - a[1].score)
      .map(([combo, stats], i) => ({ combo, ...stats, rank: i + 1 }));

    const olderRankMap: Record<string, number> = {};
    olderRanking.forEach(({ combo, rank }) => { olderRankMap[combo] = rank; });

    const recentRankMap: Record<string, number> = {};
    recentRanking.forEach(({ combo, rank }) => { recentRankMap[combo] = rank; });

    risers = recentRanking
      .filter(({ combo, rank }) => {
        const oldRank = olderRankMap[combo];
        return oldRank && oldRank - rank >= 3;
      })
      .map(({ combo, blade, ratchet, bit, rank }) => ({
        combo, blade, ratchet, bit,
        change: olderRankMap[combo] - rank,
        newRank: rank,
      }))
      .sort((a, b) => b.change - a.change)
      .slice(0, 3);

    fallers = olderRanking
      .filter(({ combo }) => {
        const newRank = recentRankMap[combo];
        const oldRank = olderRankMap[combo];
        return newRank && oldRank && newRank - oldRank >= 3;
      })
      .map(({ combo, blade, ratchet, bit }) => ({
        combo, blade, ratchet, bit,
        change: olderRankMap[combo] - recentRankMap[combo],
        newRank: recentRankMap[combo],
      }))
      .sort((a, b) => a.change - b.change)
      .slice(0, 3);
  }

  return { champion, risers, fallers, lastTournamentDate, isStale, dataSource: 'recent' };
}

/**
 * Meta distribution data for donut chart.
 */
export interface MetaDistribution {
  blade: string;
  uses: number;
  percentage: number;
  wins: number;
  color: string;
}

/**
 * Get meta distribution for top blades (donut chart).
 */
export async function getMetaDistribution(days = 30, topN = 6, region?: Region): Promise<MetaDistribution[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    blade: string;
    place: number;
    tournament_date: string;
  }>(`
    SELECT blade, place, tournament_date::VARCHAR as tournament_date
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const now = new Date();
  const cutoff = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);

  const bladeStats: Record<string, { uses: number; wins: number }> = {};
  let total = 0;

  for (const row of rows) {
    const tournamentDate = new Date(row.tournament_date);
    if (tournamentDate < cutoff) continue;

    const blade = normalizeBladeDisplay(row.blade);
    if (!bladeStats[blade]) {
      bladeStats[blade] = { uses: 0, wins: 0 };
    }
    bladeStats[blade].uses++;
    if (row.place === 1) bladeStats[blade].wins++;
    total++;
  }

  // Vibrant colors for the donut
  const colors = [
    '#00ffff', // Cyan
    '#ff00ff', // Magenta
    '#00ff88', // Green
    '#ffaa00', // Orange
    '#ff6b6b', // Coral
    '#aa88ff', // Purple
    '#ffff00', // Yellow
    '#ff8888', // Pink
  ];

  const sorted = Object.entries(bladeStats)
    .map(([blade, stats]) => ({
      blade,
      uses: stats.uses,
      wins: stats.wins,
      percentage: total > 0 ? Math.round((stats.uses / total) * 1000) / 10 : 0,
      color: '',
    }))
    .sort((a, b) => b.uses - a.uses)
    .slice(0, topN);

  // Assign colors
  sorted.forEach((item, i) => {
    item.color = colors[i % colors.length];
  });

  return sorted;
}

/**
 * Meta share data for stacked area chart.
 */
export interface MetaShareData {
  labels: string[];
  combos: {
    combo: string;
    blade: string;
    color: string;
    data: number[]; // Percentage of meta share per period
    isOthers?: boolean;
  }[];
}

/**
 * Get meta share data for stacked area chart.
 * Shows how the top combos' share of tournament placements changes over time.
 * Tracks top combos per month so new meta entries appear dynamically.
 */
export async function getMetaShareData(months = 6, region?: Region): Promise<MetaShareData> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date ASC
  `);

  // Calculate month boundaries
  const now = new Date();
  const monthBoundaries: { start: Date; end: Date; label: string }[] = [];

  for (let i = months - 1; i >= 0; i--) {
    const start = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const end = new Date(now.getFullYear(), now.getMonth() - i + 1, 0, 23, 59, 59);
    const label = start.toLocaleDateString('en-US', { month: 'short' });
    monthBoundaries.push({ start, end, label });
  }

  // Track placements per combo per month
  const comboMonthlyUses: Record<string, { blade: string; months: number[] }> = {};
  const monthlyTotals: number[] = new Array(months).fill(0);

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    const ratchet = normalizeRatchet(row.ratchet);
    const bit = normalizeBit(row.bit);
    const combo = `${blade} ${ratchet} ${bit}`;
    const tournamentDate = new Date(row.tournament_date);

    // Find which month
    let monthIndex = -1;
    for (let i = 0; i < monthBoundaries.length; i++) {
      if (tournamentDate >= monthBoundaries[i].start && tournamentDate <= monthBoundaries[i].end) {
        monthIndex = i;
        break;
      }
    }

    if (monthIndex < 0) continue;

    if (!comboMonthlyUses[combo]) {
      comboMonthlyUses[combo] = {
        blade: blade,
        months: new Array(months).fill(0),
      };
    }

    comboMonthlyUses[combo].months[monthIndex]++;
    monthlyTotals[monthIndex]++;
  }

  // Find top 5 combos for EACH month, then get union of all
  const topCombosPerMonth: Set<string>[] = [];
  const TOP_N = 5;

  for (let monthIdx = 0; monthIdx < months; monthIdx++) {
    const monthTop = Object.entries(comboMonthlyUses)
      .map(([combo, data]) => ({ combo, uses: data.months[monthIdx] }))
      .filter(c => c.uses > 0)
      .sort((a, b) => b.uses - a.uses)
      .slice(0, TOP_N)
      .map(c => c.combo);
    topCombosPerMonth.push(new Set(monthTop));
  }

  // Union of all combos that were ever in top 5
  const allTopCombos = new Set<string>();
  topCombosPerMonth.forEach(monthSet => {
    monthSet.forEach(combo => allTopCombos.add(combo));
  });

  // Color palette - vibrant cyberpunk colors (enough for many combos)
  const colors = [
    '#00ffff', // Cyan
    '#ff00ff', // Magenta
    '#00ff88', // Green
    '#ffaa00', // Orange
    '#ff6b6b', // Coral
    '#aa88ff', // Purple
    '#ffff00', // Yellow
    '#ff8800', // Dark Orange
    '#88ffff', // Light Cyan
    '#ff88ff', // Light Magenta
  ];

  // Assign colors to combos (consistent across the chart)
  const comboColorMap: Record<string, string> = {};
  let colorIndex = 0;

  // Sort by total usage to assign colors consistently (most popular gets first color)
  const sortedTopCombos = [...allTopCombos].sort((a, b) => {
    const aTotal = comboMonthlyUses[a]?.months.reduce((sum, m) => sum + m, 0) || 0;
    const bTotal = comboMonthlyUses[b]?.months.reduce((sum, m) => sum + m, 0) || 0;
    return bTotal - aTotal;
  });

  sortedTopCombos.forEach(combo => {
    comboColorMap[combo] = colors[colorIndex % colors.length];
    colorIndex++;
  });

  // Build combo data with percentages
  const comboData = sortedTopCombos.map(combo => {
    const data = comboMonthlyUses[combo];
    return {
      combo,
      blade: data.blade,
      color: comboColorMap[combo],
      data: data.months.map((uses, monthIdx) => {
        const total = monthlyTotals[monthIdx];
        return total > 0 ? Math.round((uses / total) * 100) : 0;
      }),
    };
  });

  // Calculate "Others" - everything not in top combos
  const othersData = monthlyTotals.map((total, monthIdx) => {
    if (total === 0) return 0;
    const topSum = comboData.reduce((sum, c) => sum + c.data[monthIdx], 0);
    return Math.max(0, 100 - topSum);
  });

  // Add "Others" as the bottom layer (will be rendered first/behind)
  const othersEntry = {
    combo: 'Others',
    blade: 'Others',
    color: '#3d4450', // Muted gray
    data: othersData,
    isOthers: true,
  };

  return {
    labels: monthBoundaries.map(m => m.label),
    combos: [othersEntry, ...comboData], // Others first so it's at the bottom
  };
}

/**
 * Get weekly sparkline data for a blade (last 7 weeks).
 * Returns an array of scores per week for sparkline visualization.
 */
export async function getBladeSparklineData(bladeName: string, weeks = 7, region?: Region): Promise<number[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE LOWER(blade) = LOWER('${bladeName.replace(/'/g, "''")}')${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const now = new Date();
  const weekBuckets: number[] = new Array(weeks).fill(0);

  for (const row of rows) {
    const tournamentDate = new Date(row.tournament_date);
    const weeksAgo = Math.floor((now.getTime() - tournamentDate.getTime()) / (7 * 24 * 60 * 60 * 1000));

    if (weeksAgo >= 0 && weeksAgo < weeks) {
      const points = getPlacementScore(row.place, row.stage);
      // Index 0 = oldest week, index (weeks-1) = most recent
      weekBuckets[weeks - 1 - weeksAgo] += points;
    }
  }

  return weekBuckets;
}

/**
 * Get weekly sparkline data for a combo (last 7 weeks).
 */
export async function getComboSparklineData(
  blade: string,
  ratchet: string,
  bit: string,
  weeks = 7,
  region?: Region
): Promise<number[]> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE LOWER(blade) = LOWER('${blade.replace(/'/g, "''")}')
      AND LOWER(ratchet) = LOWER('${ratchet.replace(/'/g, "''")}')
      AND LOWER(bit) = LOWER('${bit.replace(/'/g, "''")}')${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const now = new Date();
  const weekBuckets: number[] = new Array(weeks).fill(0);

  for (const row of rows) {
    const tournamentDate = new Date(row.tournament_date);
    const weeksAgo = Math.floor((now.getTime() - tournamentDate.getTime()) / (7 * 24 * 60 * 60 * 1000));

    if (weeksAgo >= 0 && weeksAgo < weeks) {
      const points = getPlacementScore(row.place, row.stage);
      weekBuckets[weeks - 1 - weeksAgo] += points;
    }
  }

  return weekBuckets;
}

/**
 * Batch get sparkline data for multiple blades (more efficient than individual calls).
 */
export async function getBladesSparklineData(bladeNames: string[], weeks = 7, region?: Region): Promise<Record<string, number[]>> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    blade: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const now = new Date();
  const result: Record<string, number[]> = {};

  // Initialize all blades
  for (const name of bladeNames) {
    result[normalizeBladeDisplay(name)] = new Array(weeks).fill(0);
  }

  for (const row of rows) {
    const blade = normalizeBladeDisplay(row.blade);
    if (!result[blade]) continue;

    const tournamentDate = new Date(row.tournament_date);
    const weeksAgo = Math.floor((now.getTime() - tournamentDate.getTime()) / (7 * 24 * 60 * 60 * 1000));

    if (weeksAgo >= 0 && weeksAgo < weeks) {
      const points = getPlacementScore(row.place, row.stage);
      result[blade][weeks - 1 - weeksAgo] += points;
    }
  }

  return result;
}

/**
 * Batch get sparkline data for multiple combos.
 */
export async function getCombosSparklineData(
  combos: { blade: string; ratchet: string; bit: string }[],
  weeks = 7,
  region?: Region
): Promise<Record<string, number[]>> {
  const regionFilter = getRegionWhereClause(region);
  const rows = await query<{
    blade: string;
    ratchet: string;
    bit: string;
    place: number;
    tournament_date: string;
    stage: string | null;
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date, stage
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const now = new Date();
  const result: Record<string, number[]> = {};

  // Initialize all combos
  for (const c of combos) {
    const key = `${normalizeBladeDisplay(c.blade)} ${normalizeRatchet(c.ratchet)} ${normalizeBit(c.bit)}`;
    result[key] = new Array(weeks).fill(0);
  }

  for (const row of rows) {
    const key = `${normalizeBladeDisplay(row.blade)} ${normalizeRatchet(row.ratchet)} ${normalizeBit(row.bit)}`;
    if (!result[key]) continue;

    const tournamentDate = new Date(row.tournament_date);
    const weeksAgo = Math.floor((now.getTime() - tournamentDate.getTime()) / (7 * 24 * 60 * 60 * 1000));

    if (weeksAgo >= 0 && weeksAgo < weeks) {
      const points = getPlacementScore(row.place, row.stage);
      result[key][weeks - 1 - weeksAgo] += points;
    }
  }

  return result;
}
