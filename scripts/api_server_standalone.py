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


def run_scrape(sources: list[str] = None, full: bool = False):
    """Run the scraper in background.

    Args:
        sources: list of source keys (wbo/jp/de/fandom/champ); None = all
        full: if True, runs a full clear+rescrape (no --incremental)
    """
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
            if not full:
                cmd.append("--incremental")
            if sources:
                cmd.extend(["--sources", ",".join(sources)])

            print(f"[{datetime.now().isoformat()}] Starting scrape: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minutes for large WBO scrapes
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
            scrape_status["last_error"] = "Scrape timed out after 30 minutes"
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

    def _query_pending_parts(self):
        """Read pending parts from the catalog as a list of dicts.

        Includes the LLM/lexical suggestion fields populated by
        scripts/catalog.py:resolve_pending_with_ragflow so the admin UI
        can show one-click "Accept as suggested" buttons.
        """
        try:
            import duckdb
            conn = duckdb.connect(str(DIST_DB), read_only=True)
            try:
                rows = conn.execute("""
                    SELECT name, part_type, occurrence_count, sample_combo,
                           suggested_canonical, suggestion_reason, suggestion_confidence
                    FROM parts_catalog
                    WHERE status = 'pending'
                    ORDER BY (suggestion_confidence IS NULL),
                             suggestion_confidence DESC,
                             occurrence_count DESC
                """).fetchall()
                return [
                    {
                        "name": r[0],
                        "part_type": r[1],
                        "occurrence_count": r[2] or 0,
                        "sample_combo": r[3],
                        "suggested_canonical": r[4],
                        "suggestion_reason": r[5],
                        "suggestion_confidence": r[6],
                    }
                    for r in rows
                ]
            finally:
                conn.close()
        except Exception as e:
            return {"error": str(e)}

    def _update_part_status(self, name, part_type, status, canonical_name=None):
        """Mark a pending part as accepted or rejected."""
        try:
            sys.path.insert(0, str(SCRIPTS_DIR))
            from db import get_connection  # noqa
            conn = get_connection()
            try:
                if status == "accepted":
                    conn.execute("""
                        UPDATE parts_catalog
                        SET status = 'accepted', accepted_at = current_timestamp,
                            canonical_name = COALESCE(?, canonical_name)
                        WHERE name = ? AND part_type = ?
                    """, [canonical_name, name, part_type])
                elif status == "rejected":
                    conn.execute("""
                        UPDATE parts_catalog
                        SET status = 'rejected', canonical_name = COALESCE(?, canonical_name)
                        WHERE name = ? AND part_type = ?
                    """, [canonical_name, name, part_type])
                conn.commit()
                copy_db_to_dist()
                return True, None
            finally:
                conn.close()
        except Exception as e:
            return False, str(e)

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

            # Pending parts count
            pending_count = 0
            try:
                import duckdb
                if db_exists:
                    c = duckdb.connect(str(DIST_DB), read_only=True)
                    pending_count = c.execute(
                        "SELECT COUNT(*) FROM parts_catalog WHERE status='pending'"
                    ).fetchone()[0]
                    c.close()
            except Exception:
                pass

            self._send_json({
                "database": {
                    "exists": db_exists,
                    "size_bytes": db_size,
                    "last_modified": db_modified,
                    "locked": is_database_locked(),
                },
                "scraper": scrape_status,
                "pending_parts_count": pending_count,
            })

        elif path == "/scrape":
            query = parse_qs(parsed.query)
            sources = query.get("sources", [None])[0]
            source_list = sources.split(",") if sources else None
            started, message = run_scrape(source_list)
            self._send_json({"started": started, "message": message})

        elif path == "/api/pending-parts":
            result = self._query_pending_parts()
            if isinstance(result, dict) and "error" in result:
                self._send_json(result, 500)
            else:
                self._send_json({"parts": result})

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

        elif path == "/upload/wiki-catalog" or path == "/api/upload/wiki-catalog":
            # The wiki bookmarklet (running in the user's browser, where wiki
            # fetches aren't Cloudflare-blocked) POSTs a bundle of fetched
            # HTML pages here. We parse them in-process via the same
            # populate_parts_catalog logic the local refresh uses, then run
            # normalize_data so the smart drift resolver picks up any new
            # typos. Synchronous because the work is bounded (~9 small
            # parses + a normalize pass).
            try:
                data = json.loads(body.decode("utf-8"))
                pages = data.get("pages") if isinstance(data, dict) else None
                if not isinstance(pages, dict) or not pages:
                    self._send_json({"error": "expected JSON body {pages: {url: html, ...}}"}, 400)
                    return

                sys.path.insert(0, str(SCRIPTS_DIR))
                sys.path.insert(0, str(SCRIPTS_DIR / "scrapers"))
                from db import get_connection, init_schema, normalize_data
                from fandom import populate_parts_catalog, CATEGORY_PAGES
                from catalog import PartsCatalog

                # Debug: log which bundle URLs match the expected CATEGORY_PAGES
                page_urls = set(pages.keys())
                expected = {url for url, _ in CATEGORY_PAGES.values()}
                hits = page_urls & expected
                misses = expected - page_urls
                print(f"[{datetime.now().isoformat()}] /upload/wiki-catalog: "
                      f"{len(pages)} pages received, {len(hits)}/{len(expected)} match Category URLs")
                if misses:
                    print(f"  MISSES (expected but not in bundle): {sorted(misses)[:3]}")
                    extras = page_urls - expected
                    print(f"  EXTRAS in bundle: {sorted(extras)[:3]}")

                conn = get_connection()
                try:
                    init_schema(conn)
                    PartsCatalog._instance = PartsCatalog.load(conn)
                    counts = populate_parts_catalog(conn, verbose=False, pages=pages)
                    fixed = normalize_data(conn)
                finally:
                    conn.close()

                copy_db_to_dist()

                total_parts = sum(counts.values())
                print(f"[{datetime.now().isoformat()}] Wiki catalog upload: "
                      f"{len(pages)} pages, {total_parts} parts catalogued, "
                      f"normalize fixed {fixed}")

                self._send_json({
                    "success": True,
                    "pages_received": len(pages),
                    "catalog_counts": counts,
                    "normalize_fixed": fixed,
                    "message": f"Catalog refreshed: {total_parts} parts across "
                               f"{len(counts)} types, {fixed} records normalized",
                })
            except json.JSONDecodeError:
                self._send_json({"error": "invalid JSON body"}, 400)
            except Exception as e:
                import traceback
                traceback.print_exc()
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

        elif path == "/api/scrape/full":
            # Clear DB and rescrape everything from all sources
            started, message = run_scrape(sources=None, full=True)
            self._send_json({"started": started, "message": message})

        elif path == "/api/parts/accept":
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
                ok, err = self._update_part_status(
                    data["name"], data["part_type"], "accepted",
                    data.get("canonical_name"),
                )
                if ok:
                    self._send_json({"success": True})
                else:
                    self._send_json({"error": err}, 500)
            except Exception as e:
                self._send_json({"error": str(e)}, 400)

        elif path == "/api/parts/reject":
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
                ok, err = self._update_part_status(
                    data["name"], data["part_type"], "rejected",
                    data.get("canonical_name"),
                )
                if ok:
                    self._send_json({"success": True})
                else:
                    self._send_json({"error": err}, 500)
            except Exception as e:
                self._send_json({"error": str(e)}, 400)

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
    print("  GET  /health             - Health check")
    print("  GET  /status             - Database, scraper, and pending-parts status")
    print("  GET  /scrape             - Trigger incremental scrape (?sources=wbo,jp,de)")
    print("  POST /scrape             - Trigger incremental scrape")
    print("  POST /upload/wbo         - Upload WBO pages from bookmarklet (auto-runs incremental)")
    print("  POST /upload/wiki-catalog - Upload Fandom wiki HTML bundle from bookmarklet (refreshes catalog)")
    print("  POST /api/scrape/full    - Clear DB and full rescrape from all sources")
    print("  GET  /api/pending-parts  - List parts seen in tournaments but not in wiki catalog")
    print("  POST /api/parts/accept   - Promote a pending part to accepted")
    print("  POST /api/parts/reject   - Mark a pending part as rejected (typo etc.)")
    print()

    server = HTTPServer(("0.0.0.0", port), APIHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
