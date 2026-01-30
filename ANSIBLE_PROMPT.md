# Claude Prompt for BeybladeX Ansible Playbook

Copy this prompt to generate an Ansible playbook for deploying BeybladeX-Database.

---

## Prompt

Create an Ansible playbook to deploy the BeybladeX-Database application. Here are the requirements:

### Target System
- Debian 12 or Ubuntu 22.04+
- Fresh server or VM
- Root/sudo access

### GitHub Repository
https://github.com/LampSteven17/BeybladeX-Database

### Components to Install

1. **System packages:** nginx, curl, git

2. **Node.js 20.x LTS** (via NodeSource)

3. **uv** (Astral Python package manager)
   - Install: `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - Binary ends up at `/root/.local/bin/uv`

4. **Application:**
   - Clone repo to `/opt/beybladex`
   - Run `uv sync` to install Python dependencies
   - Run `cd site && npm ci && npm run build` to build static site
   - Create `site/dist/data/` directory
   - Run initial JP scrape: `uv run python scripts/refresh_all.py --sources jp`
   - Copy database: `cp site/public/data/beyblade.duckdb site/dist/data/`

### nginx Configuration

```nginx
server {
    listen 80;
    server_name _;
    root /opt/beybladex/site/dist;
    index index.html;

    location /data/beyblade.duckdb {
        add_header Cache-Control "no-cache, no-store, must-revalidate";
        expires -1;
    }

    location ~* \.(js|css|png|jpg|ico|svg|woff2?)$ {
        expires 7d;
        add_header Cache-Control "public, immutable";
    }

    location / {
        try_files $uri $uri/ $uri.html /index.html;
    }

    location /api/ {
        proxy_pass http://127.0.0.1:8081/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Systemd Service for API

Create `/etc/systemd/system/beybladex-api.service`:

```ini
[Unit]
Description=BeybladeX API Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/beybladex
ExecStart=/root/.local/bin/uv run python scripts/api_server_standalone.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
Environment=API_PORT=8081

[Install]
WantedBy=multi-user.target
```

### Cron Job

Create `/etc/cron.d/beybladex`:

```
# Daily JP scrape at 6 AM, copy to dist
0 6 * * * root cd /opt/beybladex && /root/.local/bin/uv run python scripts/refresh_all.py --sources jp && cp site/public/data/beyblade.duckdb site/dist/data/ >> /var/log/beybladex-scrape.log 2>&1
```

### Playbook Variables

```yaml
beybladex_repo: "https://github.com/LampSteven17/BeybladeX-Database.git"
beybladex_path: "/opt/beybladex"
beybladex_domain: "beybladex.example.com"  # optional, for server_name
api_port: 8081
scrape_schedule: "0 6 * * *"
```

### Handlers

- Restart nginx when config changes
- Restart beybladex-api when service file changes
- Reload systemd daemon when service files change

### Tasks Order

1. Install system packages
2. Add NodeSource repo and install Node.js
3. Install uv
4. Clone/update git repository
5. Install Python dependencies (uv sync)
6. Install Node dependencies and build site (npm ci && npm run build)
7. Create dist/data directory
8. Run initial scrape if database doesn't exist
9. Copy nginx config and enable site
10. Deploy systemd service
11. Deploy cron job
12. Enable and start services

### Additional Requirements

- Make playbook idempotent (safe to run multiple times)
- Use `creates:` or `stat` checks to avoid re-running expensive tasks
- Add tags for partial runs (e.g., `--tags build` to just rebuild site)
- Include a `update` tag that pulls latest code and rebuilds

---

Generate a complete Ansible playbook with roles or a single-file playbook, whichever is more appropriate for this scope.
