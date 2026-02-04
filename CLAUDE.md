# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BeybladeX-Database is a tournament meta analysis tool for Beyblade X competitive play. It scrapes tournament results from multiple sources (WBO, Japan, Germany), stores data in DuckDB, and serves an interactive static site with in-browser SQL queries via DuckDB-WASM.

## Commands

### Python Data Pipeline
```bash
uv sync                                    # Install dependencies
python scripts/refresh_all.py              # Full refresh all data sources
python scripts/refresh_all.py --sources wbo,jp  # Specific sources
python scripts/refresh_all.py --stats      # Show database statistics
python scripts/refresh_all.py --incremental  # Add new data without clearing
```

### Astro Website
```bash
cd site
npm install                  # Install dependencies
npm run dev                  # Dev server at localhost:4321
npm run build                # Build static site (runs astro check first)
npm run preview              # Preview production build
```

## Architecture

```
Data Pipeline (Python)           Static Site (Astro)
┌─────────────────────┐         ┌──────────────────────┐
│ scripts/            │         │ site/src/            │
│  - refresh_all.py   │         │  - lib/db.ts (WASM)  │
│  - scrapers/*.py    │───────► │  - pages/*.astro     │
│  - db.py            │         │  - components/       │
└─────────────────────┘         └──────────────────────┘
         │                                │
         ▼                                ▼
   data/beyblade.duckdb    site/public/data/beyblade.duckdb
   (source of truth)              (copy for website)
```

**Key files:**
- `scripts/db.py` - Database schema, CX blade parsing, normalization
- `scripts/refresh_all.py` - Main CLI orchestrating all scrapers
- `scripts/scrapers/` - Modular scraper implementations (wbo.py, jp.py, de.py)
- `site/src/lib/db.ts` - DuckDB-WASM client and scoring system

## Data Model

**Tables:** tournaments, placements, parts

**Combo structure:** Blade + Ratchet + Bit (+ optional Lock Chip + Assist blade for CX series)

**Scoring system (in db.ts):**
- Placement points: 1st=3, 2nd=2, 3rd=1
- Recency: 30-day half-life exponential decay
- Stage multiplier: Finals=100%, both stages=115%, first stage=50%

## Blade Series

- **BX** - Basic line (standard releases)
- **UX** - Unique line (more metal/weight)
- **CX** - Custom line (modular system, see below)

## CX Blade System (IMPORTANT)

CX blades are modular and consist of multiple parts. **ONLY CX blades can have assist blades. Non-CX blades (BX/UX) NEVER have assists.**

### CX Combo Format
```
[Lock Chip] [Main Blade] [Assist Blade] [Ratchet] [Bit]
```
Example: `Pegasus Blast Wheel 3-60 Low Flat` = Pegasus lock chip + Blast main blade + Wheel assist + 3-60 ratchet + Low Flat bit

### Lock Chips (prefix for CX blade names)
Cerberus, Dran, Emperor, Fox, Hells, Hornet, Kraken, Leon, Pegasus, Perseus, Phoenix, Rhino, Sol, Stag, Valkyrie, Whale, Wizard, Wolf

### Main Blades (CX only - require a lock chip)
Antler, Arc, Blast, Brave, Brush, Dark, Eclipse, Fang, Flame, Flare, Fort, Hunt, Might, Reaper, Volt, Wriggle

### Assist Blades (CX only - single letter abbreviations)
| Abbrev | Full Name |
|--------|-----------|
| S | Slash |
| B | Bumper |
| J | Jaggy |
| R | Round |
| T | Turn |
| C | Charge |
| M | Massive |
| H | Heavy |
| Z | Zillion |
| W | Wheel |
| F | Free |
| D | Dual |

### Key Rules
1. **Only CX main blades can have assist blades** - if you see an assist on a non-CX blade, it's a parsing error
2. **CX blades are named [LockChip] [MainBlade]** - e.g., "Pegasus Blast" = Pegasus lock chip + Blast main blade
3. **Assist blade abbreviations are NOT bit abbreviations** - W=Wheel (assist), not Wedge; H=Heavy (assist), not Hexa
4. **A standalone main blade name without lock chip is incomplete** - e.g., just "Blast" without "Pegasus Blast" is invalid/incomplete data

## Design System

Bloomberg terminal aesthetic with dark theme:
- Background: `#0d1117`, Surface: `#161b22`
- Accent: orange-500 (`#f97316`)
- Series colors: BX=blue, UX=purple, CX=pink
- Typography: Inter (headings), JetBrains Mono (data/code)
