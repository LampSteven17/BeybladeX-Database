# Self-Hosting BeybladeX Database

This guide explains how to deploy BeybladeX Database on your own server for personal use.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/BeybladeX-Database.git
cd BeybladeX-Database

# Copy and configure environment
cp .env.example .env
# Edit .env with your preferred settings

# Start the services
docker compose up -d
```

The web interface will be available at `http://localhost:8080` (or your configured port).

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   Web (nginx)   │────▶│  Static Site    │
│   Port 8080     │     │  (Astro build)  │
└────────┬────────┘     └─────────────────┘
         │
         │ reads
         ▼
┌─────────────────┐
│   Shared DB     │◀──── db-data volume
│  beyblade.duckdb│
└────────▲────────┘
         │
         │ writes
         │
┌────────┴────────┐     ┌─────────────────┐
│   API Server    │────▶│  Python Scraper │
│   Port 8081     │     │  (refresh_all)  │
└─────────────────┘     └─────────────────┘
```

## Services

### Web (`beybladex-web`)
- Serves the static Astro site via nginx
- Reads the DuckDB database for in-browser queries
- Default port: 8080

### API (`beybladex-api`)
- Receives scraped data from browser bookmarklet
- Runs scheduled scraping jobs (default: 6am daily)
- Default port: 8081

## Configuration

Edit `.env` to customize:

```bash
# Web server port
WEB_PORT=8080

# API server port (for bookmarklet)
API_PORT=8081

# Cron schedule for auto-scraping (default: 6am daily)
SCRAPE_SCHEDULE=0 6 * * *

# Timezone
TZ=America/New_York
```

## Updating Data

### Option 1: Browser Bookmarklet (Recommended)

The bookmarklet scrapes WBO directly from your browser and uploads to your server.

1. Open `scripts/bookmarklet_homelab.js`
2. Replace `YOUR_SERVER_URL` with your server's address
3. Create a browser bookmark with the minified code as the URL
4. Navigate to the WBO thread and click the bookmarklet

### Option 2: Manual Trigger

```bash
# Trigger all sources
curl http://localhost:8081/scrape

# Trigger specific sources
curl "http://localhost:8081/scrape?sources=wbo,jp"
```

### Option 3: Scheduled (Automatic)

The API container runs a cron job based on `SCRAPE_SCHEDULE`. Default is 6am daily.

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/status` | GET | Database and scraper status |
| `/scrape` | GET/POST | Trigger manual scrape |
| `/upload/wbo` | POST | Receive WBO data from bookmarklet |

## Reverse Proxy (Optional)

For Traefik users, add labels to `docker-compose.yml`:

```yaml
services:
  web:
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.beybladex.rule=Host(`beybladex.yourdomain.local`)"
```

## Troubleshooting

### Database not updating
```bash
# Check API logs
docker compose logs api

# Manually trigger scrape
docker compose exec api uv run python scripts/refresh_all.py
```

### Web showing stale data
```bash
# Restart web container to reload database
docker compose restart web
```

### Check scrape status
```bash
curl http://localhost:8081/status
```

## Data Sources

- **WBO**: World Beyblade Organization tournament results
- **JP**: Japanese tournament data from okuyama3093.com
- **DE**: German BLG tournament data

## Legal Notice

This tool is for personal, non-commercial use only. Tournament data belongs to its respective sources. Please respect rate limits when scraping.
