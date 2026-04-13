# BeybladeX Database — Full System Context

Load this before working on any part of the BeybladeX Database project. Read the memory files below for the complete system architecture, user preferences, and hard-won lessons from previous sessions.

## Instructions

1. Read all three memory files listed below
2. Confirm you understand the system architecture
3. Proceed with the user's request

## Memory Files to Load

Read these files in order:

1. `/home/rtx-monster/.claude/projects/-home-rtx-monster-BeybladeX-Database/memory/project_architecture.md` — Prod deployment (192.168.88.44), RAGFlow (192.168.88.38), database files, all API endpoints, deploy process, bookmarklets, battle tracker, parts catalog pipeline, skin aliases
2. `/home/rtx-monster/.claude/projects/-home-rtx-monster-BeybladeX-Database/memory/user_profile.md` — Steven is a competitive Beyblade X player, runs the Light Lab homelab, WSL dev environment, prefers manual control, mobile-first UX
3. `/home/rtx-monster/.claude/projects/-home-rtx-monster-BeybladeX-Database/memory/feedback_patterns.md` — Critical lessons: no GH Actions for LAN services, no auto-commit, Astro CSS scoping with `is:global`, DuckDB CEPH single-writer lock, Traefik strips /api/ prefix, wiki categorization can be wrong, skin blades must fold to canonical names

## Quick Reference

- **Prod SSH**: `ssh -o BatchMode=yes -o ConnectTimeout=10 beybladex-database`
- **Deploy**: stash prod DB → git pull → restore DB → npm run build → restart beybladex-api
- **Tournament DB**: `site/public/data/beyblade.duckdb` (re-scrapeable)
- **Battle DB**: `data/battles.duckdb` (gitignored, irreplaceable — backed up via Commit button to `data/backups/battles_backup.json`)
- **Parts catalog**: `site/public/data/parts_catalog.json` (generated from wiki)
- **API server**: port 8081, all routes need BOTH `/api/foo` AND `/foo` (Traefik strips prefix)
- **RAGFlow**: `http://192.168.88.38`, dataset `e68a86ac344311f1ba10124e782f30a9`, retrieval-only
- **No AI attribution** in commits per CLAUDE.md
