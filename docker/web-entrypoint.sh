#!/bin/sh
# Copy database from shared volume to nginx html directory
# This runs on container startup

if [ -f /data/beyblade.duckdb ]; then
    echo "Copying database from volume..."
    cp /data/beyblade.duckdb /usr/share/nginx/html/data/beyblade.duckdb
    echo "Database copied successfully"
else
    echo "Warning: No database found at /data/beyblade.duckdb"
    echo "Run the API scraper first to generate data"
fi
