#!/usr/bin/env python3
"""
BeybladeX API Server (Standalone)

Simple API server for non-Docker deployments.
Handles bookmarklet uploads and triggers scrapers.
"""

import json
import os
import shutil
import subprocess
import sys
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# Resolve paths relative to repo root
REPO_ROOT = Path(__file__).parent.parent.resolve()
SCRIPTS_DIR = REPO_ROOT / "scripts"
DATA_DIR = REPO_ROOT / "data"
SITE_DIR = REPO_ROOT / "site"
SOURCE_DB = SITE_DIR / "public" / "data" / "beyblade.duckdb"
DIST_DB = SITE_DIR / "dist" / "data" / "beyblade.duckdb"
WBO_PAGES_FILE = DATA_DIR / "wbo_pages.json"

# Add scripts to path
sys.path.insert(0, str(SCRIPTS_DIR))
try:
    from db import is_database_locked
except ImportError:
    def is_database_locked():
        return False

# Track scrape status
scrape_status = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "last_error": None,
}


def copy_db_to_dist():
    """Copy database from source to dist directory."""
    if SOURCE_DB.exists():
        DIST_DB.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(SOURCE_DB, DIST_DB)
        print(f"[{datetime.now().isoformat()}] Database copied to dist")


def run_scrape(sources: list[str] = None):
    """Run the scraper in background."""
    global scrape_status

    if scrape_status["running"]:
        return False, "Scrape already in progress"

    if is_database_locked():
        return False, "Database is locked by another process"

    scrape_status["running"] = True
    scrape_status["last_error"] = None

    def _scrape():
        global scrape_status
        try:
            cmd = ["uv", "run", "python", "scripts/refresh_all.py"]
            if sources:
                cmd.extend(["--sources", ",".join(sources)])

            print(f"[{datetime.now().isoformat()}] Starting scrape: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                timeout=600,
            )

            scrape_status["last_result"] = result.returncode == 0

            if result.returncode == 0:
                print(f"[{datetime.now().isoformat()}] Scrape completed successfully")
                # Copy database to dist for nginx to serve
                copy_db_to_dist()
            else:
                scrape_status["last_error"] = result.stderr[:500] if result.stderr else "Unknown error"
                print(f"[{datetime.now().isoformat()}] Scrape failed: {scrape_status['last_error']}")

        except subprocess.TimeoutExpired:
            scrape_status["last_result"] = False
            scrape_status["last_error"] = "Scrape timed out after 10 minutes"
            print(f"[{datetime.now().isoformat()}] Scrape timed out")
        except Exception as e:
            scrape_status["last_result"] = False
            scrape_status["last_error"] = str(e)
            print(f"[{datetime.now().isoformat()}] Scrape error: {e}")
        finally:
            scrape_status["running"] = False
            scrape_status["last_run"] = datetime.now().isoformat()

    thread = threading.Thread(target=_scrape, daemon=True)
    thread.start()
    return True, "Scrape started"


class APIHandler(BaseHTTPRequestHandler):
    def _send_json(self, data: dict, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/health":
            self._send_json({"status": "ok"})

        elif path == "/status":
            # Check dist database (what nginx serves)
            db_exists = DIST_DB.exists()
            db_size = DIST_DB.stat().st_size if db_exists else 0
            db_modified = datetime.fromtimestamp(DIST_DB.stat().st_mtime).isoformat() if db_exists else None

            self._send_json({
                "database": {
                    "exists": db_exists,
                    "size_bytes": db_size,
                    "last_modified": db_modified,
                    "locked": is_database_locked(),
                },
                "scraper": scrape_status,
            })

        elif path == "/scrape":
            query = parse_qs(parsed.query)
            sources = query.get("sources", [None])[0]
            source_list = sources.split(",") if sources else None
            started, message = run_scrape(source_list)
            self._send_json({"started": started, "message": message})

        else:
            self._send_json({"error": "Not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        if path == "/upload/wbo":
            try:
                data = json.loads(body.decode("utf-8"))

                if not isinstance(data, dict):
                    self._send_json({"error": "Invalid data format"}, 400)
                    return

                WBO_PAGES_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(WBO_PAGES_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f)

                page_count = len(data)
                print(f"[{datetime.now().isoformat()}] Received {page_count} WBO pages")

                started, message = run_scrape(["wbo"])

                self._send_json({
                    "success": True,
                    "pages_received": page_count,
                    "scrape_started": started,
                    "message": f"Received {page_count} pages, processing started",
                })

            except json.JSONDecodeError:
                self._send_json({"error": "Invalid JSON"}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)

        elif path == "/scrape":
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
                sources = data.get("sources")
                started, message = run_scrape(sources)
                self._send_json({"started": started, "message": message})
            except json.JSONDecodeError:
                started, message = run_scrape()
                self._send_json({"started": started, "message": message})

        else:
            self._send_json({"error": "Not found"}, 404)

    def log_message(self, format, *args):
        print(f"[{datetime.now().isoformat()}] {args[0]}")


def main():
    port = int(os.environ.get("API_PORT", 8081))

    # Ensure directories exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DIST_DB.parent.mkdir(parents=True, exist_ok=True)

    print(f"BeybladeX API Server (Standalone)")
    print(f"  Repo root: {REPO_ROOT}")
    print(f"  Source DB: {SOURCE_DB}")
    print(f"  Dist DB:   {DIST_DB}")
    print(f"  Port:      {port}")
    print()
    print("Endpoints:")
    print("  GET  /health      - Health check")
    print("  GET  /status      - Database and scraper status")
    print("  GET  /scrape      - Trigger scrape (?sources=wbo,jp,de)")
    print("  POST /scrape      - Trigger scrape")
    print("  POST /upload/wbo  - Upload WBO pages from bookmarklet")
    print()

    server = HTTPServer(("0.0.0.0", port), APIHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
