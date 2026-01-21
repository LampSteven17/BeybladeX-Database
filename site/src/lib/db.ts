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
  'Phoenix Wing': 'BX',      // BX-23
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
  'Soar Phoenix': 'UX',      // UX-04 Entry Set
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

// Singleton database instance
let db: duckdb.AsyncDuckDB | null = null;
let conn: duckdb.AsyncDuckDBConnection | null = null;
let initPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null;

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
 * Get points for a placement.
 */
function getPlacementScore(place: number): number {
  return PLACEMENT_POINTS[place] || 0;
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

      // Create the combo_usage view in our session
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
            p.lock_chip_1 as lock_chip
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
            p.lock_chip_2
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
            p.lock_chip_3
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
 * Execute a query and return results as an array of objects.
 */
export async function query<T = Record<string, unknown>>(sql: string, _params?: unknown[]): Promise<T[]> {
  const connection = await initDB();
  const result = await connection.query(sql);
  return result.toArray().map((row) => Object.fromEntries(Object.entries(row))) as T[];
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
  }>(`
    SELECT blade, place, tournament_date::VARCHAR as tournament_date
    FROM combo_usage
    WHERE 1=1${regionFilter}
    ORDER BY tournament_date DESC
  `);

  const bladeScores: Record<string, BladeStats> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const blade = row.blade;
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
    const points = getPlacementScore(row.place);
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
 */
export async function getRankedCombos(limit = 20, minUses = 2, region?: Region): Promise<ComboStats[]> {
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
    ORDER BY tournament_date DESC
  `);

  const comboScores: Record<string, ComboStats & { recent_score?: number; older_score?: number }> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const key = `${row.blade}|${row.ratchet}|${row.bit}`;
    if (!comboScores[key]) {
      comboScores[key] = {
        combo: `${row.blade} ${row.ratchet} ${row.bit}`,
        blade: row.blade,
        ratchet: row.ratchet,
        bit: row.bit,
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
    const points = getPlacementScore(row.place);
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
  }>(`
    SELECT ${partType} as part, place, tournament_date::VARCHAR as tournament_date
    FROM combo_usage
    WHERE 1=1${regionFilter}
  `);

  const partScores: Record<string, PartStats & { recent_score?: number; older_score?: number }> = {};
  const referenceDate = new Date();
  const recentCutoff = new Date(referenceDate.getTime() - TREND_RECENT_DAYS * 24 * 60 * 60 * 1000);
  const olderCutoff = new Date(referenceDate.getTime() - (TREND_RECENT_DAYS + TREND_COMPARE_DAYS) * 24 * 60 * 60 * 1000);

  for (const row of rows) {
    const part = row.part;
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
    const points = getPlacementScore(row.place);
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
  }>(`
    SELECT ratchet, bit, place, tournament_date::VARCHAR as tournament_date
    FROM combo_usage
    WHERE LOWER(blade) = LOWER('${bladeName.replace(/'/g, "''")}')${regionFilter}
  `);

  const comboScores: Record<
    string,
    { ratchet: string; bit: string; combo: string; raw_score: number; uses: number; first: number; second: number; third: number }
  > = {};
  const referenceDate = new Date();

  for (const row of rows) {
    const key = `${row.ratchet}|${row.bit}`;
    if (!comboScores[key]) {
      comboScores[key] = {
        ratchet: row.ratchet,
        bit: row.bit,
        combo: `${row.ratchet} ${row.bit}`,
        raw_score: 0,
        uses: 0,
        first: 0,
        second: 0,
        third: 0,
      };
    }

    const tournamentDate = new Date(row.tournament_date);
    const weight = calculateRecencyWeight(tournamentDate, referenceDate);
    const points = getPlacementScore(row.place);

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
    query<{ place: number; tournament_date: string; tournament_id: number }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id
      FROM combo_usage
      WHERE LOWER(blade) = LOWER('${blade1.replace(/'/g, "''")}')${regionFilter}
    `),
    query<{ place: number; tournament_date: string; tournament_id: number }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id
      FROM combo_usage
      WHERE LOWER(blade) = LOWER('${blade2.replace(/'/g, "''")}')${regionFilter}
    `),
  ]);

  const referenceDate = new Date();

  function calcBladeStats(data: { place: number; tournament_date: string; tournament_id: number }[]) {
    const stats = { raw_score: 0, uses: 0, first: 0, second: 0, third: 0, tournaments: new Set<number>() };
    for (const row of data) {
      const weight = calculateRecencyWeight(new Date(row.tournament_date), referenceDate);
      const points = getPlacementScore(row.place);
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
  return rows.map((r) => r.ratchet);
}

/**
 * Get all unique bit names (for dropdowns).
 */
export async function getAllBits(): Promise<string[]> {
  const rows = await query<{ bit: string }>(`
    SELECT DISTINCT bit FROM combo_usage WHERE bit IS NOT NULL ORDER BY bit
  `);
  return rows.map((r) => r.bit);
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
  }>(`
    SELECT place, tournament_date::VARCHAR as tournament_date
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
    const points = getPlacementScore(row.place);
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
    query<{ place: number; tournament_date: string; tournament_id: number }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id
      FROM combo_usage
      WHERE ${buildWhereClause(combo1)}
    `),
    query<{ place: number; tournament_date: string; tournament_id: number }>(`
      SELECT place, tournament_date::VARCHAR as tournament_date, tournament_id
      FROM combo_usage
      WHERE ${buildWhereClause(combo2)}
    `),
  ]);

  const referenceDate = new Date();

  function calcComboStats(data: { place: number; tournament_date: string; tournament_id: number }[]) {
    const stats = { raw_score: 0, uses: 0, first: 0, second: 0, third: 0, tournaments: new Set<number>() };
    for (const row of data) {
      const weight = calculateRecencyWeight(new Date(row.tournament_date), referenceDate);
      const points = getPlacementScore(row.place);
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
  }>(`
    SELECT assist, place, tournament_date::VARCHAR as tournament_date
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
    const points = getPlacementScore(row.place);
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
  }>(`
    SELECT lock_chip, place, tournament_date::VARCHAR as tournament_date
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
    const points = getPlacementScore(row.place);
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

/**
 * Meta spotlight data for the hero visualization.
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
    dominance: number; // % of top placements
  } | null;
  risers: {
    combo: string;
    blade: string;
    ratchet: string;
    bit: string;
    change: number; // positive
    newRank: number;
  }[];
  fallers: {
    combo: string;
    blade: string;
    ratchet: string;
    bit: string;
    change: number; // negative
    newRank: number;
  }[];
  lastTournamentDate: string | null; // Most recent tournament date for this region
  isStale: boolean; // True if using all-time data because no recent tournaments
  dataSource: 'recent' | 'alltime'; // Which data window is being used
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
  }>(`
    SELECT blade, ratchet, bit, place, tournament_date::VARCHAR as tournament_date
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
      const combo = `${row.blade} ${row.ratchet} ${row.bit}`;
      const points = getPlacementScore(row.place);

      if (!comboStats[combo]) {
        comboStats[combo] = {
          blade: row.blade,
          ratchet: row.ratchet,
          bit: row.bit,
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

  const { champion } = calculateStats(recentRows, 2);

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

    if (!bladeStats[row.blade]) {
      bladeStats[row.blade] = { uses: 0, wins: 0 };
    }
    bladeStats[row.blade].uses++;
    if (row.place === 1) bladeStats[row.blade].wins++;
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
    const combo = `${row.blade} ${row.ratchet} ${row.bit}`;
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
        blade: row.blade,
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
