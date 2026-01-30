#!/bin/bash
set -e

# Database path - writes directly to shared volume via symlink
# /app/site/public/data -> /data (symlink created in Dockerfile)
DB_PATH="/data/beyblade.duckdb"

# Ensure data directory exists
mkdir -p /app/data

# Setup cron job for scheduled scraping
# Note: Only JP source works out of the box. WBO requires browser bookmarklet, DE requires instaloader.
if [ -n "$SCRAPE_SCHEDULE" ]; then
    echo "Setting up cron schedule: $SCRAPE_SCHEDULE"
    echo "$SCRAPE_SCHEDULE cd /app && /usr/local/bin/uv run python scripts/refresh_all.py --sources jp 2>&1 | logger -t scraper" > /etc/cron.d/scraper
    chmod 0644 /etc/cron.d/scraper
    crontab /etc/cron.d/scraper
    cron
    echo "Cron daemon started"
fi

# Run initial scrape if database doesn't exist
if [ ! -f "$DB_PATH" ]; then
    echo "No database found, running initial scrape (JP source only)..."
    echo "Note: Use the browser bookmarklet to add WBO data after deployment."
    cd /app && uv run python scripts/refresh_all.py --sources jp || true
    if [ -f "$DB_PATH" ]; then
        echo "Initial scrape complete"
    else
        echo "Warning: Initial scrape did not produce a database. The site will show no data until you run the bookmarklet."
    fi
fi

# Start API server
echo "Starting API server on port 8000..."
exec uv run python api_server.py
