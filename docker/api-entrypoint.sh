#!/bin/bash
set -e

# Database paths
# scripts/db.py writes to: /app/site/public/data/beyblade.duckdb
# Shared volume for web container: /data/beyblade.duckdb
LOCAL_DB="/app/site/public/data/beyblade.duckdb"
SHARED_DB="/data/beyblade.duckdb"

# Ensure directories exist
mkdir -p /app/site/public/data /app/data

# Setup cron job for scheduled scraping
# Note: Only JP source works out of the box. WBO requires browser bookmarklet, DE requires instaloader.
if [ -n "$SCRAPE_SCHEDULE" ]; then
    echo "Setting up cron schedule: $SCRAPE_SCHEDULE"
    echo "$SCRAPE_SCHEDULE cd /app && /usr/local/bin/uv run python scripts/refresh_all.py --sources jp && cp $LOCAL_DB $SHARED_DB 2>/dev/null || true" > /etc/cron.d/scraper
    chmod 0644 /etc/cron.d/scraper
    crontab /etc/cron.d/scraper
    cron
    echo "Cron daemon started"
fi

# Run initial scrape if database doesn't exist
if [ ! -f "$SHARED_DB" ]; then
    echo "No database found, running initial scrape (JP source only)..."
    echo "Note: Use the browser bookmarklet to add WBO data after deployment."
    cd /app && uv run python scripts/refresh_all.py --sources jp || true
    if [ -f "$LOCAL_DB" ]; then
        cp "$LOCAL_DB" "$SHARED_DB"
        echo "Initial scrape complete"
    else
        echo "Warning: Initial scrape did not produce a database. The site will show no data until you run the bookmarklet."
    fi
fi

# Start API server
echo "Starting API server on port 8000..."
exec uv run python api_server.py
