# Plan: Top-5 Color Inheritance in Meta Evolution Chart

## Context

The chart currently shows 10 blades with 10 fixed colors assigned by identity. With limited colors it's hard to track blade succession. The user wants:
- Track only the **top 5 blades by score** at any given month (not 10)
- Keep **ranks 1-10 visible** on the Y axis — lines can oscillate through all 10 positions
- When a blade **drops out of the top 5**, the blade that **replaces it inherits its color**
- Legend shows the **current** top 5 blades

## Approach: Month-by-Month Color Pool

Walk months chronologically, maintaining 5 color slots. When a blade exits the tracked top 5 and a new one enters, the newcomer inherits the departing blade's color. Each segment is colored by the blade's assignment at that month. Lines still render through ranks 1-10.

## File Changes

### 1. `site/src/lib/db.ts` (lines 3514-3560)

- Line 3518: `.slice(0, 10)` → `.slice(0, 5)` — notable blades = ever in top 5 of any month
- Line 3560: Remove `.slice(0, 10)` — return ALL notable blades (frontend needs full history for color assignment)

### 2. `site/src/pages/index.astro`

**A. Reduce colors to 5 (line 733):**
```
const bladeColors = ['#fbbf24', '#22d3ee', '#4ade80', '#f87171', '#a78bfa'];
```

**B. Color assignment algorithm (new, before SVG rendering ~line 791):**

Build `bladeColorMap: Map<string, Map<number, string>>` — maps blade → (monthIndex → color).

Walk months left-to-right:
1. Compute top-5 blades for this month from `monthlyRanks` data across all journeys
2. Track `colorAssignment: Map<blade, colorIdx>` and `freeColors: number[]`
3. Each month:
   - Blades in top-5 last month but NOT this month → free their colorIdx
   - New blades in top-5 → assign a freed colorIdx
   - ALL currently-assigned blades (even if rank > 5 temporarily) keep their color; they only lose it when they fall out of top-5
4. Store color per blade per month

**C. Rendering (lines 816-892):**
- `topJourneys` = only blades that have a color assignment (ever in top 5)
- Keep `maxRank = 10` — lines draw through all 10 ranks
- Each line segment uses the blade's color at that month from `bladeColorMap`
- Dots same
- Keep all fade-in/fade-out logic from previous commit

**D. Legend (lines 759-773):**
- Show current top 5 blades (those with color assignment in latest month)
- Each blade shows its current color
- Keep trend icons

## Verification

1. `cd site && npm run build` — no errors
2. `npm run dev` → front page:
   - 5 blade lines at a time, oscillating through ranks 1-10
   - When a blade exits, new blade picks up its color
   - Legend shows current top 5 with correct colors
   - Fade-in/fade-out effects preserved
