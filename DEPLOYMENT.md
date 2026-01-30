# BeybladeX-Database Deployment Guide

Simple deployment without Docker - just nginx + Python API.

## System Requirements

- **OS:** Debian 12 / Ubuntu 22.04+
- **Node.js:** 20.x LTS
- **Python:** 3.12+
- **uv:** Latest (Astral Python package manager)
- **nginx:** For static file serving

## Directory Structure

```
/opt/beybladex/
├── site/
│   ├── dist/              # Built Astro static site (nginx serves this)
│   │   └── data/
│   │       └── beyblade.duckdb  # Database (copied here after scrapes)
│   └── public/data/       # Source database location
├── scripts/               # Python scrapers & API
├── data/                  # Scraper temp files (wbo_pages.json)
└── .venv/                 # Python virtual environment (managed by uv)
```

## Installation Steps

```bash
# 1. System packages
apt update && apt install -y nginx curl git

# 2. Node.js 20.x
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs

# 3. uv (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

# 4. Clone repo
git clone https://github.com/LampSteven17/BeybladeX-Database.git /opt/beybladex
cd /opt/beybladex

# 5. Python deps
uv sync

# 6. Build site
cd site && npm ci && npm run build && cd ..

# 7. Create dist data dir and run initial scrape
mkdir -p site/dist/data data
uv run python scripts/refresh_all.py --sources jp
cp site/public/data/beyblade.duckdb site/dist/data/
```

## nginx Config

`/etc/nginx/sites-available/beybladex`:
```nginx
server {
    listen 80;
    server_name beybladex.example.com;
    root /opt/beybladex/site/dist;
    index index.html;

    # No cache for database
    location /data/beyblade.duckdb {
        add_header Cache-Control "no-cache, no-store, must-revalidate";
        expires -1;
    }

    # Cache static assets
    location ~* \.(js|css|png|jpg|ico|svg|woff2?)$ {
        expires 7d;
        add_header Cache-Control "public, immutable";
    }

    # SPA routing
    location / {
        try_files $uri $uri/ $uri.html /index.html;
    }

    # API proxy
    location /api/ {
        proxy_pass http://127.0.0.1:8081/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

Enable: `ln -s /etc/nginx/sites-available/beybladex /etc/nginx/sites-enabled/`

## API Systemd Service

`/etc/systemd/system/beybladex-api.service`:
```ini
[Unit]
Description=BeybladeX API Server
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/beybladex
ExecStart=/root/.local/bin/uv run python scripts/api_server_standalone.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
Environment=DB_PATH=/opt/beybladex/site/dist/data/beyblade.duckdb

[Install]
WantedBy=multi-user.target
```

Enable: `systemctl enable --now beybladex-api`

## Cron for Scheduled Scrapes

`/etc/cron.d/beybladex`:
```cron
# Run JP scraper daily at 6 AM, copy DB to dist
0 6 * * * root cd /opt/beybladex && /root/.local/bin/uv run python scripts/refresh_all.py --sources jp && cp site/public/data/beyblade.duckdb site/dist/data/
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/status` | GET | DB & scraper status |
| `/api/scrape` | GET/POST | Trigger scrape |
| `/api/upload/wbo` | POST | Bookmarklet upload |

## Update Procedure

```bash
cd /opt/beybladex
git pull
cd site && npm ci && npm run build && cd ..
cp site/public/data/beyblade.duckdb site/dist/data/ 2>/dev/null || true
systemctl restart beybladex-api
systemctl reload nginx
```

## Bookmarklet

Update `API_URL` in bookmarklet to: `https://beybladex.example.com/api`
