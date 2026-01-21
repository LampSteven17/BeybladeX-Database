# BeybladeX Database - Project Plan

## Overview

A meta analysis tool for Beyblade X competitive tournament data. Scrapes WBO tournament results, stores in a single DuckDB database, and serves a static site for public browsing.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Collection (Python)                     │
│  scripts/scraper.py → fetch WBO forum → parse → insert to DB   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Single DuckDB Database                          │
│                 data/beyblade.duckdb                            │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │
│  │ tournaments │ │ placements  │ │   parts     │               │
│  └─────────────┘ └─────────────┘ └─────────────┘               │
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┴───────────────────┐
        ▼                                       ▼
┌───────────────────┐                   ┌───────────────────┐
│  Local Analysis   │                   │  Static Site      │
│  Python + DuckDB  │                   │  Astro + DuckDB   │
│  Jupyter/CLI      │                   │  WASM + anime.js  │
└───────────────────┘                   └───────────────────┘
```

## Single Source of Truth

**One database**: `data/beyblade.duckdb`

- All scripts read/write to this file
- Versioned in git (DuckDB files are reasonably small for this dataset)
- For the static site: export a snapshot or load directly via DuckDB-WASM

## Data Schema

### Tables

```sql
-- Reference data for all known parts
CREATE TABLE parts (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,           -- "Phoenix Wing", "9-60", "Ball"
    type VARCHAR NOT NULL,           -- "blade", "ratchet", "bit"
    spin_direction VARCHAR,          -- "right", "left", "dual" (blades only)
    series VARCHAR,                  -- "BX", "UX", etc.
    release_date DATE,
    notes VARCHAR
);

-- Tournament events
CREATE TABLE tournaments (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    date DATE NOT NULL,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    region VARCHAR,                  -- "NA", "EU", "ASIA", "OCEANIA", "SA"
    format VARCHAR,                  -- "1on1", "3on3"
    ranked BOOLEAN,
    participant_count INTEGER,
    wbo_thread_url VARCHAR,
    challonge_url VARCHAR
);

-- Top 3 placements from each tournament
CREATE TABLE placements (
    id INTEGER PRIMARY KEY,
    tournament_id INTEGER REFERENCES tournaments(id),
    place INTEGER NOT NULL,          -- 1, 2, or 3
    player_name VARCHAR NOT NULL,
    player_wbo_id VARCHAR,           -- WBO profile ID if available
    -- Combo 1 (required)
    blade_1 VARCHAR NOT NULL,
    ratchet_1 VARCHAR NOT NULL,
    bit_1 VARCHAR NOT NULL,
    -- Combo 2 (for 3on3 decks)
    blade_2 VARCHAR,
    ratchet_2 VARCHAR,
    bit_2 VARCHAR,
    -- Combo 3 (for 3on3 decks)
    blade_3 VARCHAR,
    ratchet_3 VARCHAR,
    bit_3 VARCHAR
);
```

### Computed Views (for analysis)

```sql
-- Flatten all combos used in placements
CREATE VIEW combo_usage AS
SELECT tournament_id, place, player_name, blade_1 as blade, ratchet_1 as ratchet, bit_1 as bit FROM placements
UNION ALL
SELECT tournament_id, place, player_name, blade_2, ratchet_2, bit_2 FROM placements WHERE blade_2 IS NOT NULL
UNION ALL
SELECT tournament_id, place, player_name, blade_3, ratchet_3, bit_3 FROM placements WHERE blade_3 IS NOT NULL;

-- Part win rates (1st place = win)
CREATE VIEW part_stats AS
SELECT
    part_name,
    part_type,
    COUNT(*) as total_placements,
    SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as first_place_count,
    SUM(CASE WHEN place <= 3 THEN 1 ELSE 0 END) as top3_count,
    ROUND(SUM(CASE WHEN place = 1 THEN 1.0 ELSE 0 END) / COUNT(*), 3) as win_rate
FROM (
    SELECT place, blade as part_name, 'blade' as part_type FROM combo_usage
    UNION ALL
    SELECT place, ratchet, 'ratchet' FROM combo_usage
    UNION ALL
    SELECT place, bit, 'bit' FROM combo_usage
) parts
GROUP BY part_name, part_type;
```

## Directory Structure

```
BeybladeX-Database/
├── PLAN.md                    # This file
├── README.md
├── data/
│   └── beyblade.duckdb        # THE database (single source of truth)
├── scripts/
│   ├── requirements.txt
│   ├── db.py                  # Database connection and schema setup
│   ├── scraper.py             # WBO forum scraper
│   ├── parts_seed.py          # Seed canonical parts list
│   └── analysis.py            # Analysis queries and utilities
└── site/
    ├── astro.config.mjs
    ├── package.json
    └── src/
        ├── pages/
        ├── components/
        └── styles/
```

## Data Source

**Primary**: WBO Winning Combinations Thread (Beyblade X)
- URL: `worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX`
- Format: Forum posts with structured tournament results
- ~95+ pages of data

**Post format**:
```
[Tournament Name](link) - MM/DD/YY
City, State/Province, Country - X Format - Ranked/Unranked 1on1/3on3

1st Place: [Username](profile_link)
BladeName Ratchet-Bit

2nd Place: [Username](profile_link)
BladeName Ratchet-Bit

3rd Place: [Username](profile_link)
BladeName Ratchet-Bit
```

## Tech Stack

### Data Layer
- **Python 3.11+**
- **DuckDB** - Embedded analytical database
- **BeautifulSoup** - HTML parsing for scraper
- **requests** - HTTP client

### Static Site
- **Astro** - Static site generator
- **DuckDB-WASM** - In-browser SQL queries
- **Tailwind CSS** - Styling
- **anime.js** - Animations

### Design
- Dark cyberpunk aesthetic (tasteful, not cheesy)
- Monospace fonts for data
- Subtle glow effects on interactions
- One or two accent colors

## Implementation Phases

### Phase 1: Data Foundation
- [ ] Set up DuckDB schema
- [ ] Create canonical parts database (all known blades, ratchets, bits)
- [ ] Build WBO forum scraper
- [ ] Populate database with historical data
- [ ] Basic CLI queries for validation

### Phase 2: Analysis Tools
- [ ] Part performance queries
- [ ] Combo analysis
- [ ] Time-series meta tracking
- [ ] Regional breakdowns
- [ ] Jupyter notebook examples

### Phase 3: Static Site
- [ ] Astro project setup
- [ ] DuckDB-WASM integration
- [ ] Dashboard pages (parts, combos, tournaments)
- [ ] anime.js animations
- [ ] GitHub Pages deployment

### Phase 4: Polish
- [ ] Data quality validation
- [ ] Automated scraper updates
- [ ] Mobile responsive design
- [ ] Community feedback integration

## Notes

- No external APIs required for MVP (Challonge, Supabase, etc.)
- Data versioned in git - the .duckdb file is the artifact
- Local-first: works offline, no server needed
- Static site can be rebuilt anytime from the database
