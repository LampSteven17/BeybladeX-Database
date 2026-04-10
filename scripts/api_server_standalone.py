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

        Reads from the SOURCE DB (not the dist copy) so accept/reject
        actions show up immediately without needing to copy the 21 MB
        file each time. The user-facing site doesn't read parts_catalog,
        so a stale dist DB is fine for it.
        """
        try:
            import duckdb
            conn = duckdb.connect(str(SOURCE_DB), read_only=True)
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
        """Mark a pending part as accepted or rejected.

        Fast path: only touches the parts_catalog row. Does NOT copy
        the DB to dist (~21 MB → ~1.5s on the CEPH disk) and does NOT
        rewrite placements. The admin page reads pending parts from the
        source DB so the UI updates instantly.

        The placement rewrite + dist copy happens later when the user
        clicks "Apply changes to site" (POST /api/parts/apply-changes).
        """
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

        elif path == "/api/pending-parts" or path == "/pending-parts":
            # Traefik strips the /api/ prefix when routing to this server,
            # so we register both forms for every public API route.
            result = self._query_pending_parts()
            if isinstance(result, dict) and "error" in result:
                self._send_json(result, 500)
            else:
                self._send_json({"parts": result})

        elif path == "/api/meta/top-combos" or path == "/meta/top-combos":
            try:
                sys.path.insert(0, str(SCRIPTS_DIR))
                from db import get_connection
                conn = get_connection()
                try:
                    limit = int(parse_qs(parsed.query).get("limit", ["10"])[0])
                    # Fetch more rows than requested since blade dedup reduces count
                    rows = conn.execute("""
                        SELECT blade, lock_chip, ratchet, bit, COUNT(*) as uses
                        FROM combo_usage
                        GROUP BY blade, lock_chip, ratchet, bit
                        ORDER BY uses DESC
                        LIMIT ?
                    """, [limit * 10]).fetchall()
                    combos = []
                    seen_blades = set()
                    for r in rows:
                        blade_full = f"{r[1]} {r[0]}".strip() if r[1] else r[0]
                        # Unique blades only — keep the top combo per blade
                        if blade_full in seen_blades:
                            continue
                        seen_blades.add(blade_full)
                        combo_str = f"{blade_full} {r[2]} {r[3]}"
                        combos.append({
                            "blade": r[0],
                            "lock_chip": r[1],
                            "ratchet": r[2],
                            "bit": r[3],
                            "display": combo_str,
                            "uses": r[4],
                        })
                    self._send_json({"combos": combos[:limit]})
                finally:
                    conn.close()
            except Exception as e:
                self._send_json({"error": str(e)}, 500)

        elif path == "/api/battles" or path == "/battles":
            query = parse_qs(parsed.query)
            self._handle_battles_get(query)

        elif path == "/api/battles/stats" or path == "/battles/stats":
            query = parse_qs(parsed.query)
            self._handle_battles_stats(query)

        elif path == "/api/stamina" or path == "/stamina":
            query = parse_qs(parsed.query)
            self._handle_stamina_get(query)

        else:
            self._send_json({"error": "Not found"}, 404)

    # ------------------------------------------------------------------
    # Battle tracker GET handlers
    # ------------------------------------------------------------------

    def _handle_battles_get(self, query):
        try:
            sys.path.insert(0, str(SCRIPTS_DIR))
            from db import get_battles_connection, init_battles_schema
            conn = get_battles_connection()
            init_battles_schema(conn)
            try:
                combo = query.get("combo", [None])[0]
                opponent = query.get("opponent", [None])[0]
                session = query.get("session", [None])[0]
                limit = int(query.get("limit", ["100"])[0])

                where = "1=1"
                params = []
                if combo:
                    where += " AND combo_id = ?"
                    params.append(combo)
                if opponent:
                    where += " AND opponent_id = ?"
                    params.append(opponent)
                if session:
                    where += " AND session_id = ?"
                    params.append(session)

                rows = conn.execute(f"""
                    SELECT id, combo_id, opponent_id, stadium, finish_type,
                           result, points, session_id, notes,
                           created_at::VARCHAR as created_at
                    FROM battles WHERE {where}
                    ORDER BY created_at DESC LIMIT ?
                """, params + [limit]).fetchall()

                self._send_json({"battles": [
                    {
                        "id": r[0], "combo_id": r[1], "opponent_id": r[2],
                        "stadium": r[3], "finish_type": r[4], "result": r[5],
                        "points": r[6], "session_id": r[7], "notes": r[8],
                        "created_at": r[9],
                    } for r in rows
                ]})
            finally:
                conn.close()
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_battles_stats(self, query):
        try:
            sys.path.insert(0, str(SCRIPTS_DIR))
            from db import get_battles_connection, init_battles_schema
            conn = get_battles_connection()
            init_battles_schema(conn)
            try:
                combo = query.get("combo", [None])[0]
                where = "1=1"
                params = []
                if combo:
                    where += " AND combo_id = ?"
                    params.append(combo)

                rows = conn.execute(f"""
                    SELECT combo_id, opponent_id,
                           SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins,
                           SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses,
                           SUM(CASE WHEN result='win' THEN points ELSE 0 END) as points_won,
                           SUM(CASE WHEN result='loss' THEN points ELSE 0 END) as points_lost,
                           COUNT(*) as total
                    FROM battles WHERE {where}
                    GROUP BY combo_id, opponent_id
                    ORDER BY total DESC
                """, params).fetchall()

                self._send_json({"matchups": [
                    {
                        "combo_id": r[0], "opponent_id": r[1],
                        "wins": r[2], "losses": r[3],
                        "points_won": r[4], "points_lost": r[5],
                        "total": r[6],
                        "win_rate": round(r[2] / r[6] * 100, 1) if r[6] > 0 else 0,
                    } for r in rows
                ]})
            finally:
                conn.close()
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_stamina_get(self, query):
        try:
            sys.path.insert(0, str(SCRIPTS_DIR))
            from db import get_battles_connection, init_battles_schema
            conn = get_battles_connection()
            init_battles_schema(conn)
            try:
                combo = query.get("combo", [None])[0]
                where = "1=1"
                params = []
                if combo:
                    where += " AND combo_id = ?"
                    params.append(combo)

                rows = conn.execute(f"""
                    SELECT id, combo_id, spin_time_ms, trial_number, notes,
                           created_at::VARCHAR as created_at
                    FROM stamina_trials WHERE {where}
                    ORDER BY created_at DESC
                """, params).fetchall()

                self._send_json({"trials": [
                    {
                        "id": r[0], "combo_id": r[1], "spin_time_ms": r[2],
                        "trial_number": r[3], "notes": r[4], "created_at": r[5],
                    } for r in rows
                ]})
            finally:
                conn.close()
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

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

                # Sanity check: how many of the expected Category URLs are in
                # the bundle? Logged so misconfigured bookmarklets are obvious.
                expected = {url for url, _ in CATEGORY_PAGES.values()}
                hits = set(pages.keys()) & expected
                print(f"[{datetime.now().isoformat()}] /upload/wiki-catalog: "
                      f"{len(pages)} pages received, {len(hits)}/{len(expected)} match Category URLs")

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

        elif path == "/api/scrape/full" or path == "/scrape/full":
            # Clear DB and rescrape everything from all sources
            started, message = run_scrape(sources=None, full=True)
            self._send_json({"started": started, "message": message})

        elif path == "/api/parts/auto-accept" or path == "/parts/auto-accept":
            # Bulk-accept every pending row with a suggestion at or above
            # the requested confidence threshold. Same per-row effect as
            # the admin "Accept as suggested" button — marks the dirty
            # name as rejected with canonical_name=suggested_canonical.
            # The actual placement rewrite still waits for /apply-changes.
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
                threshold = float(data.get("threshold", 0.8))
                sys.path.insert(0, str(SCRIPTS_DIR))
                from db import get_connection
                conn = get_connection()
                try:
                    rows = conn.execute("""
                        SELECT name, part_type, suggested_canonical, suggestion_confidence
                        FROM parts_catalog
                        WHERE status = 'pending'
                          AND suggested_canonical IS NOT NULL
                          AND suggestion_confidence IS NOT NULL
                          AND suggestion_confidence >= ?
                    """, [threshold]).fetchall()
                    accepted = 0
                    for name, part_type, canonical, _conf in rows:
                        conn.execute("""
                            UPDATE parts_catalog
                            SET status = 'rejected', canonical_name = ?
                            WHERE name = ? AND part_type = ?
                        """, [canonical, name, part_type])
                        accepted += 1
                    conn.commit()
                finally:
                    conn.close()
                print(f"[{datetime.now().isoformat()}] /api/parts/auto-accept "
                      f"threshold={threshold}: accepted {accepted}")
                self._send_json({
                    "success": True,
                    "accepted": accepted,
                    "threshold": threshold,
                    "message": f"Auto-accepted {accepted} suggestions at "
                               f"≥{int(threshold * 100)}% confidence",
                })
            except Exception as e:
                import traceback
                traceback.print_exc()
                self._send_json({"error": str(e)}, 500)

        elif path == "/api/parts/apply-changes" or path == "/parts/apply-changes":
            # Walk every parts_catalog row with a canonical_name and rewrite
            # the corresponding placements (CX-aware blade splitting), then
            # re-run drift detection (so the now-resolved entries drop out
            # of the pending list), then copy the DB to dist so the user-
            # facing site picks up the cleaned data. Synchronous; takes a
            # few seconds because of the placement updates and the dist copy.
            try:
                sys.path.insert(0, str(SCRIPTS_DIR))
                from db import get_connection, normalize_data
                from catalog import apply_canonical_rewrites, PartsCatalog
                conn = get_connection()
                try:
                    PartsCatalog._instance = PartsCatalog.load(conn)
                    rewrite_summary = apply_canonical_rewrites(conn)
                    fixed = normalize_data(conn)
                finally:
                    conn.close()
                copy_db_to_dist()
                total = sum(rewrite_summary.values())
                print(f"[{datetime.now().isoformat()}] /api/parts/apply-changes: "
                      f"rewrote {total} placement cells, normalize fixed {fixed}")
                self._send_json({
                    "success": True,
                    "rewritten": rewrite_summary,
                    "normalize_fixed": fixed,
                    "message": f"Applied {total} placement rewrites; "
                               f"{fixed} records normalized; site updated.",
                })
            except Exception as e:
                import traceback
                traceback.print_exc()
                self._send_json({"error": str(e)}, 500)

        elif path == "/api/parts/accept" or path == "/parts/accept":
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

        elif path == "/api/parts/reject" or path == "/parts/reject":
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

        elif path == "/api/battles" or path == "/battles":
            self._handle_battles_post(body)

        elif path == "/api/battles/commit" or path == "/battles/commit":
            try:
                sys.path.insert(0, str(SCRIPTS_DIR))
                from db import backup_battles_to_json
                backup_path = backup_battles_to_json()
                print(f"[{datetime.now().isoformat()}] Battles backed up to {backup_path}")
                self._send_json({"success": True, "path": backup_path, "message": "Battle data backed up to JSON"})
            except Exception as e:
                self._send_json({"error": str(e)}, 500)

        elif path == "/api/battles/import" or path == "/battles/import":
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
                battles_list = data.get("battles", [])
                stamina_list = data.get("stamina_trials", [])
                sys.path.insert(0, str(SCRIPTS_DIR))
                from db import get_battles_connection, init_battles_schema
                conn = get_battles_connection()
                init_battles_schema(conn)
                for b in battles_list:
                    conn.execute(
                        "INSERT INTO battles (combo_id, opponent_id, stadium, finish_type, result, points, session_id) VALUES (?,?,?,?,?,?,?)",
                        [b["combo_id"], b["opponent_id"], b.get("stadium"), b["finish_type"], b["result"], b.get("points", 0), b.get("session_id", "import")]
                    )
                for s in stamina_list:
                    conn.execute(
                        "INSERT INTO stamina_trials (combo_id, spin_time_ms, trial_number) VALUES (?,?,?)",
                        [s["combo_id"], s["spin_time_ms"], s.get("trial_number")]
                    )
                conn.commit()
                conn.close()
                print(f"[{datetime.now().isoformat()}] Imported {len(battles_list)} battles + {len(stamina_list)} stamina")
                self._send_json({"success": True, "imported_battles": len(battles_list), "imported_stamina": len(stamina_list)})
            except Exception as e:
                import traceback; traceback.print_exc()
                self._send_json({"error": str(e)}, 500)

        elif path == "/api/battles/clear-session" or path == "/battles/clear-session":
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
                sid = data.get("session_id")
                if not sid:
                    self._send_json({"error": "session_id required"}, 400)
                    return
                sys.path.insert(0, str(SCRIPTS_DIR))
                from db import get_battles_connection, init_battles_schema
                conn = get_battles_connection()
                init_battles_schema(conn)
                count = conn.execute("SELECT COUNT(*) FROM battles WHERE session_id = ?", [sid]).fetchone()[0]
                conn.execute("DELETE FROM battles WHERE session_id = ?", [sid])
                conn.commit()
                conn.close()
                print(f"[{datetime.now().isoformat()}] Cleared {count} battles from session {sid}")
                self._send_json({"success": True, "cleared": count})
            except Exception as e:
                self._send_json({"error": str(e)}, 500)

        elif path == "/api/battles/delete" or path == "/battles/delete":
            self._handle_battles_delete(body)

        elif path == "/api/stamina" or path == "/stamina":
            self._handle_stamina_post(body)

        else:
            self._send_json({"error": "Not found"}, 404)

    # ------------------------------------------------------------------
    # Battle tracker POST handlers
    # ------------------------------------------------------------------

    FINISH_POINTS = {"spin": 1, "over": 2, "xtreme": 3, "burst": 2}

    def _handle_battles_post(self, body):
        try:
            data = json.loads(body.decode("utf-8")) if body else {}
            combo_id = data.get("combo_id")
            opponent_id = data.get("opponent_id")
            finish_type = data.get("finish_type")
            result = data.get("result")
            if not all([combo_id, opponent_id, finish_type, result]):
                self._send_json({"error": "combo_id, opponent_id, finish_type, result required"}, 400)
                return
            if finish_type not in self.FINISH_POINTS:
                self._send_json({"error": f"finish_type must be spin/over/xtreme/burst"}, 400)
                return
            if result not in ("win", "loss"):
                self._send_json({"error": "result must be win or loss"}, 400)
                return

            points = self.FINISH_POINTS[finish_type]
            stadium = data.get("stadium")
            session_id = data.get("session_id")
            notes = data.get("notes")

            sys.path.insert(0, str(SCRIPTS_DIR))
            from db import get_battles_connection, init_battles_schema
            conn = get_battles_connection()
            init_battles_schema(conn)
            try:
                row = conn.execute("""
                    INSERT INTO battles (combo_id, opponent_id, stadium, finish_type,
                                        result, points, session_id, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id, created_at::VARCHAR
                """, [combo_id, opponent_id, stadium, finish_type,
                      result, points, session_id, notes]).fetchone()
                conn.commit()
                self._send_json({
                    "success": True,
                    "id": row[0],
                    "points": points,
                    "created_at": row[1],
                })
            finally:
                conn.close()
        except json.JSONDecodeError:
            self._send_json({"error": "invalid JSON"}, 400)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_battles_delete(self, body):
        try:
            data = json.loads(body.decode("utf-8")) if body else {}
            battle_id = data.get("id")
            if not battle_id:
                self._send_json({"error": "id required"}, 400)
                return
            sys.path.insert(0, str(SCRIPTS_DIR))
            from db import get_battles_connection, init_battles_schema
            conn = get_battles_connection()
            init_battles_schema(conn)
            try:
                conn.execute("DELETE FROM battles WHERE id = ?", [battle_id])
                conn.commit()
                self._send_json({"success": True})
            finally:
                conn.close()
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_stamina_post(self, body):
        try:
            data = json.loads(body.decode("utf-8")) if body else {}
            combo_id = data.get("combo_id")
            spin_time_ms = data.get("spin_time_ms")
            if not combo_id or spin_time_ms is None:
                self._send_json({"error": "combo_id and spin_time_ms required"}, 400)
                return

            sys.path.insert(0, str(SCRIPTS_DIR))
            from db import get_battles_connection, init_battles_schema
            conn = get_battles_connection()
            init_battles_schema(conn)
            try:
                # Auto-assign trial number
                existing = conn.execute(
                    "SELECT COUNT(*) FROM stamina_trials WHERE combo_id = ?",
                    [combo_id],
                ).fetchone()[0]
                trial_num = existing + 1

                row = conn.execute("""
                    INSERT INTO stamina_trials (combo_id, spin_time_ms, trial_number, notes)
                    VALUES (?, ?, ?, ?)
                    RETURNING id, created_at::VARCHAR
                """, [combo_id, spin_time_ms, trial_num, data.get("notes")]).fetchone()
                conn.commit()
                self._send_json({
                    "success": True,
                    "id": row[0],
                    "trial_number": trial_num,
                    "created_at": row[1],
                })
            finally:
                conn.close()
        except json.JSONDecodeError:
            self._send_json({"error": "invalid JSON"}, 400)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

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
