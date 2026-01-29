# BeybladeX-Database

Tournament meta analysis tool for Beyblade X competitive play. Scrapes tournament results from multiple sources, stores data in DuckDB, and provides an interactive web interface with in-browser SQL queries via DuckDB-WASM.

## Features

- Tournament data from WBO, Japanese, and German sources
- Bloomberg terminal-inspired dark UI
- In-browser SQL queries (no server needed for analysis)
- Part usage statistics with recency-weighted scoring
- Combo win rates and meta trends

## Self-Hosting

Deploy your own instance for personal use:

```bash
git clone https://github.com/YOUR_USERNAME/BeybladeX-Database.git
cd BeybladeX-Database
cp .env.example .env
docker compose up -d
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for full setup instructions including:
- Browser bookmarklet for easy data updates
- Scheduled automatic scraping
- Reverse proxy configuration

## Development

### Python Data Pipeline
```bash
uv sync                                    # Install dependencies
python scripts/refresh_all.py              # Full refresh all sources
python scripts/refresh_all.py --sources wbo,jp  # Specific sources
python scripts/refresh_all.py --stats      # Show database statistics
```

### Astro Website
```bash
cd site
npm install
npm run dev      # Dev server at localhost:4321
npm run build    # Build static site
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
```

## Legal Notice

This tool is for personal, non-commercial use only. Tournament data belongs to its respective sources (WBO, okuyama3093.com, BLG). Please respect rate limits when scraping.

## License

MIT
