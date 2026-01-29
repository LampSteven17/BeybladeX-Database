#!/bin/bash
set -e

# Setup cron job for scheduled scraping
if [ -n "$SCRAPE_SCHEDULE" ]; then
    echo "Setting up cron schedule: $SCRAPE_SCHEDULE"
    echo "$SCRAPE_SCHEDULE cd /app && /usr/local/bin/uv run python scripts/refresh_all.py --sources wbo,jp,de && cp /app/data/beyblade.duckdb /data/beyblade.duckdb" > /etc/cron.d/scraper
    chmod 0644 /etc/cron.d/scraper
    crontab /etc/cron.d/scraper
    cron
    echo "Cron daemon started"
fi

# Run initial scrape if database doesn't exist
if [ ! -f /data/beyblade.duckdb ]; then
    echo "No database found, running initial scrape..."
    cd /app && uv run python scripts/refresh_all.py --sources wbo,jp,de
    cp /app/data/beyblade.duckdb /data/beyblade.duckdb
    echo "Initial scrape complete"
fi

# Start API server
echo "Starting API server on port 8000..."
exec uv run python api_server.py
