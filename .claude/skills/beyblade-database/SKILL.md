---
name: beyblade-database
description: BeybladeX Database — tournament meta analysis site (Astro + DuckDB-WASM) and its Python scraper pipeline, deployed to beybladex-database.thelightlab.net on the Light Lab. Inputs- scripts/ scrapers + site/src/ pages. Outputs- DuckDB data, built static site, prod deploy. Use when touching scrapers, the DuckDB schema, ranking pages, the battle tracker, the parts catalog, or deploying/recovering prod. Does NOT cover Light Lab infra generally (see /lightlab) or router/VLAN work (see /lightlab-network).
---

# BeybladeX Database

Tournament meta analysis for competitive Beyblade X. Python scrapers fill a DuckDB file; an Astro
static site queries it in-browser via DuckDB-WASM. A standalone Python API server on prod handles
bookmarklet uploads and the personal battle tracker.

**Domain rules — CX blade structure, valid ratchets/bits, lock chips, assists, over blades, the
scoring system, and the design system — live in `CLAUDE.md` and are assumed here, not restated.**
Read it before touching parsing or ranking code; the CX rules are the easiest thing to get wrong.

## I/O contract

| | |
|---|---|
| Inputs | `scripts/scrapers/*.py`, `data/wbo_pages.json`, Fandom wiki (via bookmarklet), `site/src/**` |
| Outputs | `site/public/data/beyblade.duckdb`, `site/public/data/parts_catalog.json`, `site/dist/` build, prod deploy |
| Source of truth | `site/public/data/beyblade.duckdb` for tournament data; `data/battles.duckdb` for personal battles (**gitignored, irreplaceable**) |
| Upstream | WBO forum, okuyama3093.com (JP), official championships, Fandom wiki. Wiki + WBO arrive via user-run **bookmarklets**, not cron |
| Downstream | nginx serving `/opt/beybladex/site/dist`; Traefik publishing `beybladex-database.thelightlab.net` |
| Narrow exceptions | `.github/workflows/rebuild.yml` is the only CI workflow — it runs scrapers on `data/wbo_pages.json` push. It cannot reach the LAN |

## The two databases — never confuse them

| File | Holds | Git | If lost |
|---|---|---|---|
| `site/public/data/beyblade.duckdb` | tournaments, placements, parts_catalog, part_attributes | tracked, committed routinely | re-scrapeable |
| `data/battles.duckdb` | personal battle log + stamina trials | **gitignored** | **irreplaceable** — only backup is `data/backups/battles_backup.json`, written on explicit Commit tap |

When the user says "fix this data", default-assume the **battles** DB — that's their personal
testing. Community results are the tournament DB. Ask if ambiguous. Never edit the tournament DB to
match personal testing observations.

## Determining current state

This skill stores no state. To read it live:

```bash
# Is prod up, and what's in the DB?
curl -s https://beybladex-database.thelightlab.net/api/status

# What is prod actually running? (ALWAYS run before deploying)
ssh -o BatchMode=yes -o ConnectTimeout=10 beybladex-database \
  "cd /opt/beybladex && sudo git status --short && sudo git log --oneline -3 && sudo git branch -v"

# Where does prod live, if the IP is wrong again
ssh prxy-m710q "pvesh get /cluster/resources --type vm --output-format json"   # find node + vmid
ssh prxy-m625q "pvesh get /nodes/prxy-m625q/qemu/102/agent/network-get-interfaces"
```

Local stats: `python scripts/refresh_all.py --stats`.

## Prod

See `CLAUDE.md` → Deployment for the full host table and the deploy commands.

- `debian@192.168.99.250`, repo at `/opt/beybladex` (root-owned, use `sudo`)
- Proxmox VM `qemu/102` on node `prxy-m625q`; Traefik route comes from the VM description `traefik-port:80`
- nginx serves `site/dist`; the API is systemd `beybladex-api` on port 8081 — **restart it after deploys**
- Guests are on the lab VLAN `192.168.99.0/24`. The desktop is on `192.168.88.10` (trusted VLAN);
  VLAN 88 → 99 is permitted, so prod is reachable from desktop and WSL. App guests **do not answer
  ICMP** — a ping sweep will not find them, and an unreachable IP usually means a stale record, not
  a network outage.
- SSH always needs `-o BatchMode=yes -o ConnectTimeout=10` or it hangs in a non-interactive shell.

### Deploy preconditions

Both must hold before any deploy. Neither is guaranteed — verify, don't assume.

1. **Prod's working tree is clean and not ahead of `origin/main`.** Prod can hold unpushed commits
   and uncommitted edits; `git pull` over them is destructive. If it is dirty or ahead, recover via
   `git bundle` first — `CLAUDE.md` → Recovering work stranded on prod.
2. **Prod is on `main`.** A feature branch checked out on prod is a defect to reconverge, not a
   workflow to preserve.

## Architecture

```
Python pipeline                    Astro site
scripts/refresh_all.py  ──────►  site/src/lib/db.ts (DuckDB-WASM queries + scoring)
scripts/scrapers/{wbo,jp,fandom}   site/src/pages/*.astro
scripts/db.py (schema, CX parsing, normalization)
scripts/catalog.py (parts_catalog, drift detection)
scripts/api_server_standalone.py (prod only, :8081)
         │
         ▼
site/public/data/beyblade.duckdb ──► copied into site/dist on build
```

- `scripts/db.py` — schema, CX blade parsing, normalization, part lists
- `scripts/parser_aliases.py` — `SKIN_ALIASES`, the collab/crossover blades that must fold into
  their canonical base blade so rankings don't fragment
- `site/src/lib/db.ts` — all queries, the scoring system, `METAL_LOCK_CHIPS`, radar-chart stats
- `site/src/components/Navigation.astro` — header; carries the nav links **and** the TimeFilter /
  RegionFilter controls. `Layout.astro` passes `currentPath` for active-state highlighting

## API routes

Every route must be registered under **both** `/api/foo` and `/foo` — Traefik strips the `/api/`
prefix, so an `/api`-only route never matches in production. Full endpoint table is in the
project memory (`project_architecture.md`).

## Failure → fix

| Symptom | Cause | Fix |
|---|---|---|
| JS-created elements render unstyled | Astro scopes `<style>` by default | `<style is:global>` on that page |
| Writes hang or crawl (~10 min / 3k rows) | DuckDB single-writer over CEPH | Use the API import endpoint, or stop `beybladex-api` first |
| Route 404s in prod, works locally | Traefik strips `/api/` | Register both `/api/foo` and `/foo` |
| Rankings fragment across duplicate blades | Collab skin not folded | Add to `SKIN_ALIASES` in `scripts/parser_aliases.py` |
| A part ranks in two categories | Hybrid part (e.g. ratchet+bit) | Accept in the higher slot, reject in the lower, `NULL` the lower column |
| Prod SSH times out | Stale recorded IP | Resolve live via `pvesh` (see *Determining current state*) |

Never open a second DuckDB write connection while `beybladex-api` is running.

## Boundaries

- Light Lab compute, Traefik, Proxmox, NAS → `/lightlab`
- Router, VLANs, firewall → `/lightlab-network`
- Domain rules (CX structure, ratchets, bits, scoring), conventions, deploy commands → `CLAUDE.md`
- Accumulated corrections and rationale → project memory (`feedback_patterns.md`,
  `project_architecture.md`, `user_profile.md`). Read before proposing automation or a deploy.
