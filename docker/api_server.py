"""
BeybladeX API Server

Provides endpoints for:
- Health checks
- Receiving scraped data from browser bookmarklet
- Triggering manual scrape refreshes
- Database status
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

# Paths
DATA_DIR = Path("/app/data")
SHARED_DB = Path("/data/beyblade.duckdb")
LOCAL_DB = DATA_DIR / "beyblade.duckdb"
WBO_PAGES_FILE = DATA_DIR / "wbo_pages.json"

# Track scrape status
scrape_status = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "last_error": None,
}


def run_scrape(sources: list[str] = None):
    """Run the scraper in background."""
    global scrape_status

    if scrape_status["running"]:
        return False, "Scrape already in progress"

    scrape_status["running"] = True
    scrape_status["last_error"] = None

    def _scrape():
        global scrape_status
        try:
            cmd = ["uv", "run", "python", "scripts/refresh_all.py"]
            if sources:
                cmd.extend(["--sources", ",".join(sources)])

            result = subprocess.run(
                cmd,
                cwd="/app",
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
            )

            scrape_status["last_result"] = result.returncode == 0
            if result.returncode != 0:
                scrape_status["last_error"] = result.stderr[:500]

            # Copy database to shared volume
            if LOCAL_DB.exists():
                shutil.copy(LOCAL_DB, SHARED_DB)

        except Exception as e:
            scrape_status["last_result"] = False
            scrape_status["last_error"] = str(e)
        finally:
            scrape_status["running"] = False
            scrape_status["last_run"] = datetime.now().isoformat()

    thread = threading.Thread(target=_scrape)
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
        """Handle CORS preflight."""
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
            db_exists = SHARED_DB.exists()
            db_size = SHARED_DB.stat().st_size if db_exists else 0
            db_modified = datetime.fromtimestamp(SHARED_DB.stat().st_mtime).isoformat() if db_exists else None

            self._send_json({
                "database": {
                    "exists": db_exists,
                    "size_bytes": db_size,
                    "last_modified": db_modified,
                },
                "scraper": scrape_status,
            })

        elif path == "/scrape":
            # Trigger scrape via GET (convenience)
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
            # Receive WBO pages from bookmarklet
            try:
                data = json.loads(body.decode("utf-8"))

                # Validate data structure
                if not isinstance(data, dict):
                    self._send_json({"error": "Invalid data format"}, 400)
                    return

                # Save to file
                WBO_PAGES_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(WBO_PAGES_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f)

                page_count = len(data)

                # Trigger WBO import
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
            # Trigger scrape via POST
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
        """Custom logging format."""
        print(f"[{datetime.now().isoformat()}] {args[0]}")


def main():
    port = int(os.environ.get("API_PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), APIHandler)
    print(f"BeybladeX API server running on port {port}")
    print(f"Endpoints:")
    print(f"  GET  /health        - Health check")
    print(f"  GET  /status        - Database and scraper status")
    print(f"  GET  /scrape        - Trigger scrape (?sources=wbo,jp,de)")
    print(f"  POST /scrape        - Trigger scrape (body: {{\"sources\": [...]}})")
    print(f"  POST /upload/wbo    - Upload WBO pages from bookmarklet")
    server.serve_forever()


if __name__ == "__main__":
    main()
