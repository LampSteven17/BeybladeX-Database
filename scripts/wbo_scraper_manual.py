"""
WBO Forum Scraper with Manual Cloudflare Bypass (WSL-friendly).

This script works in two modes:
1. COOKIE MODE: You visit WBO in your Windows browser, export cookies, paste them here
2. HEADLESS MODE: Uses those cookies to scrape without a browser window

Usage:
    python scripts/wbo_scraper_manual.py

The script will:
1. Ask you to visit WBO in your Windows browser
2. Ask you to copy cookies from browser dev tools
3. Use those cookies to scrape all pages headlessly
"""

import re
import sys
import time
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Import shared utilities from scraper.py and db.py
from db import get_connection, init_schema, parse_cx_blade, infer_region, normalize_data
from scraper import (
    parse_combo,
    parse_header_lines,
    is_beyblade_x_content,
    Combo,
    Placement,
    Tournament,
)


BASE_URL = "https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX"
COOKIES_FILE = Path(__file__).parent.parent / "data" / "wbo_cookies.json"


def get_total_pages(html: str) -> int:
    """Extract total page count from pagination."""
    soup = BeautifulSoup(html, "html.parser")

    # Look for pagination links like "page=51"
    pagination = soup.find("div", class_="pagination")
    if pagination:
        page_links = pagination.find_all("a", href=True)
        max_page = 1
        for link in page_links:
            match = re.search(r"page=(\d+)", link["href"])
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
        return max_page

    # Fallback: look anywhere in page for page= links
    matches = re.findall(r"page=(\d+)", html)
    if matches:
        return max(int(m) for m in matches)

    return 1


def parse_post(post_element) -> list[Tournament]:
    """
    Parse a forum post element and extract tournament data.
    Returns list of tournaments (a post may contain multiple).
    Only extracts Beyblade X content, filters out Metal Fight etc.
    """
    tournaments = []

    # Get post ID
    post_id = post_element.get("id", "")
    if not post_id.startswith("pid"):
        return tournaments

    # Get post body
    body = post_element.find("div", class_="post_body")
    if not body:
        return tournaments

    # Get text content preserving some structure
    text = body.get_text(separator="\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # Skip the first post (it's instructions)
    if "This thread is for Beyblade X combinations" in text:
        return tournaments

    # Filter out non-Beyblade X content
    if not is_beyblade_x_content(lines):
        return tournaments

    # Parse header info from first few lines
    header_info = parse_header_lines(lines)

    current_tournament = None
    current_placements = []
    current_place = None
    current_player = None
    current_combos = []

    # Track if we've created the initial tournament
    tournament_created = False
    tournament_index = 0

    for i, line in enumerate(lines):
        # Check for date pattern that might indicate a NEW tournament within same post
        has_date = re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", line) or re.search(
            r"[A-Z][a-z]+ \d{1,2},? \d{4}", line
        )

        # Only treat as new tournament if we already have one and this looks like a header
        if has_date and tournament_created and current_tournament:
            is_header_line = (
                line.strip().startswith("-")
                or re.match(r"^[A-Z][a-z]+ \d{1,2},? \d{4}$", line.strip())
                or re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", line.strip())
            )

            if is_header_line:
                # Save current tournament
                if current_place is not None and current_player and current_combos:
                    current_placements.append(
                        Placement(
                            place=current_place,
                            player_name=current_player,
                            player_wbo_id=None,
                            combos=current_combos,
                        )
                    )

                if current_placements:
                    current_tournament.placements = current_placements
                    tournaments.append(current_tournament)
                    tournament_index += 1

                # Start new tournament
                remaining_lines = lines[max(0, i - 1) : i + 5]
                header_info = parse_header_lines(remaining_lines)

                unique_post_id = f"{post_id}_{tournament_index}"
                current_tournament = Tournament(
                    wbo_post_id=unique_post_id,
                    name=header_info.get("name", f"Tournament {tournament_index}"),
                    date=header_info.get("date"),
                    city=header_info.get("city"),
                    state=header_info.get("state"),
                    country=header_info.get("country"),
                    format=header_info.get("format"),
                    ranked=header_info.get("ranked"),
                )
                current_placements = []
                current_place = None
                current_player = None
                current_combos = []
                continue

        # Check for placement markers (1st, 2nd, 3rd, etc.)
        place_match = re.match(
            r"^(1st|2nd|3rd|\d+(?:st|nd|rd|th))\s*(?:Place)?[:\s-]*(.*)$", line, re.I
        )
        if place_match:
            # Save previous placement if exists
            if current_place is not None and current_player and current_combos:
                current_placements.append(
                    Placement(
                        place=current_place,
                        player_name=current_player,
                        player_wbo_id=None,
                        combos=current_combos,
                    )
                )
                current_combos = []

            # Parse new placement
            place_str = place_match.group(1).lower()
            place_map = {"1st": 1, "2nd": 2, "3rd": 3}
            if place_str in place_map:
                current_place = place_map[place_str]
            else:
                num_match = re.search(r"\d+", place_str)
                current_place = int(num_match.group()) if num_match else 0

            rest = place_match.group(2).strip()

            # Check if player name is on same line
            name_match = re.match(
                r"^([A-Za-z0-9_\[\]]+(?:\s+[A-Za-z0-9_\[\]]+)?)\s*[-:]?\s*(.*)$", rest
            )
            if name_match:
                current_player = name_match.group(1).strip()
                combo_text = name_match.group(2).strip()
                if combo_text:
                    combo = parse_combo(combo_text)
                    if combo:
                        current_combos.append(combo)
            else:
                current_player = rest if rest else None

            # Create tournament on first placement if not yet created
            if not tournament_created:
                current_tournament = Tournament(
                    wbo_post_id=f"{post_id}_{tournament_index}",
                    name=header_info.get("name", "Unknown Tournament"),
                    date=header_info.get("date"),
                    city=header_info.get("city"),
                    state=header_info.get("state"),
                    country=header_info.get("country"),
                    format=header_info.get("format"),
                    ranked=header_info.get("ranked"),
                )
                tournament_created = True
            continue

        # Check if line is a player name (after place marker)
        if current_place is not None and current_player is None:
            if re.match(r"^[A-Za-z0-9_\[\]]+$", line) and len(line) <= 30:
                current_player = line
                continue

        # Try to parse as combo
        if current_place is not None:
            combo = parse_combo(line)
            if combo:
                current_combos.append(combo)

    # Save last placement
    if current_place is not None and current_player and current_combos:
        current_placements.append(
            Placement(
                place=current_place,
                player_name=current_player,
                player_wbo_id=None,
                combos=current_combos,
            )
        )

    # Save last tournament
    if current_tournament and current_placements:
        current_tournament.placements = current_placements
        tournaments.append(current_tournament)

    return tournaments


def scrape_page(html: str) -> list[Tournament]:
    """Scrape all tournaments from page HTML."""
    soup = BeautifulSoup(html, "html.parser")

    tournaments = []
    posts = soup.find_all("div", class_="post")

    for post in posts:
        try:
            page_tournaments = parse_post(post)
            tournaments.extend(page_tournaments)
        except Exception as e:
            post_id = post.get("id", "unknown")
            print(f"  Warning: Error parsing post {post_id}: {e}")

    return tournaments


def save_tournaments(tournaments: list[Tournament], conn):
    """Save tournaments to database."""
    saved = 0
    skipped = 0

    for tournament in tournaments:
        # Check if already exists
        existing = conn.execute(
            "SELECT 1 FROM tournaments WHERE wbo_post_id = ?", [tournament.wbo_post_id]
        ).fetchone()

        if existing:
            skipped += 1
            continue

        # Insert tournament
        region = infer_region(tournament.country)

        conn.execute(
            """
            INSERT INTO tournaments (wbo_post_id, name, date, city, state, country, region, format, ranked, wbo_url, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
                tournament.wbo_post_id,
                tournament.name,
                tournament.date.isoformat() if tournament.date else None,
                tournament.city,
                tournament.state,
                tournament.country,
                region,
                tournament.format,
                tournament.ranked,
                tournament.wbo_url,
                "wbo",
            ],
        )

        tournament_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Insert placements and combos
        for placement in tournament.placements:
            conn.execute(
                """
                INSERT INTO placements (tournament_id, place, player_name, player_wbo_id)
                VALUES (?, ?, ?, ?)
            """,
                [
                    tournament_id,
                    placement.place,
                    placement.player_name,
                    placement.player_wbo_id,
                ],
            )

            placement_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            for combo in placement.combos:
                conn.execute(
                    """
                    INSERT INTO combo_usage (placement_id, blade, lock_chip, ratchet, bit, assist, stage)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        placement_id,
                        combo.blade,
                        combo.lock_chip,
                        combo.ratchet,
                        combo.bit,
                        combo.assist,
                        combo.stage,
                    ],
                )

        saved += 1

    return saved, skipped


def parse_cookie_string(cookie_str: str) -> dict:
    """Parse cookie string from browser into dict."""
    cookies = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()
    return cookies


def load_saved_cookies() -> Optional[dict]:
    """Load cookies from file if they exist."""
    if COOKIES_FILE.exists():
        try:
            with open(COOKIES_FILE) as f:
                data = json.load(f)
                # Check if cookies are less than 24 hours old
                saved_time = datetime.fromisoformat(data.get("saved_at", "2000-01-01"))
                if datetime.now() - saved_time < timedelta(hours=24):
                    return data.get("cookies", {})
        except Exception:
            pass
    return None


def save_cookies(cookies: dict):
    """Save cookies to file."""
    COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COOKIES_FILE, "w") as f:
        json.dump({"cookies": cookies, "saved_at": datetime.now().isoformat()}, f)


def test_cookies(session: requests.Session) -> bool:
    """Test if current cookies work."""
    try:
        response = session.get(BASE_URL, timeout=15)
        if response.status_code == 200 and "Winning Combinations" in response.text:
            return True
    except Exception:
        pass
    return False


def get_cookies_interactive() -> dict:
    """Interactive prompt to get cookies from user."""
    print()
    print("=" * 70)
    print("STEP 1: Open this URL in your Windows browser (Firefox/Chrome):")
    print("=" * 70)
    print()
    print(f"  {BASE_URL}")
    print()
    print("Wait for the page to fully load (solve any Cloudflare challenge).")
    print()
    input("Press Enter once the WBO page has loaded...")

    print()
    print("=" * 70)
    print("STEP 2: Copy cookies from your browser")
    print("=" * 70)
    print()
    print("In your browser, open Developer Tools (F12), then:")
    print()
    print("  FIREFOX:")
    print("    1. Go to 'Storage' tab -> 'Cookies' -> worldbeyblade.org")
    print("    2. Right-click -> 'Copy All'")
    print()
    print("  CHROME:")
    print("    1. Go to 'Application' tab -> 'Cookies' -> worldbeyblade.org")
    print("    2. Or go to 'Network' tab, refresh, click first request,")
    print("       find 'Cookie:' in Request Headers, copy that value")
    print()
    print("  EASIEST METHOD (works in both):")
    print("    1. Go to Console tab")
    print("    2. Type: document.cookie")
    print("    3. Press Enter, copy the result (without quotes)")
    print()

    cookie_str = input("Paste the cookie string here: ").strip()

    if cookie_str.startswith('"') and cookie_str.endswith('"'):
        cookie_str = cookie_str[1:-1]
    if cookie_str.startswith("'") and cookie_str.endswith("'"):
        cookie_str = cookie_str[1:-1]

    return parse_cookie_string(cookie_str)


def main():
    print("=" * 70)
    print("WBO Scraper - WSL Compatible (Cookie-based)")
    print("=" * 70)

    # Create session with browser-like headers
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )

    # Try to load saved cookies first
    saved_cookies = load_saved_cookies()
    if saved_cookies:
        print("\nFound saved cookies, testing...")
        session.cookies.update(saved_cookies)
        if test_cookies(session):
            print("Saved cookies still work!")
        else:
            print("Saved cookies expired, need new ones.")
            saved_cookies = None
            session.cookies.clear()

    # Get new cookies if needed
    if not saved_cookies:
        cookies = get_cookies_interactive()
        session.cookies.update(cookies)

        print("\nTesting cookies...")
        if not test_cookies(session):
            print("\nERROR: Cookies don't seem to work.")
            print("Make sure you copied the full cookie string after the page loaded.")
            print("Try again? (y/n): ", end="")
            if input().strip().lower() == "y":
                cookies = get_cookies_interactive()
                session.cookies.update(cookies)
                if not test_cookies(session):
                    print("Still not working. Exiting.")
                    return
            else:
                return

        print("Cookies work! Saving for future use...")
        save_cookies(dict(session.cookies))

    # Get first page and total pages
    print("\nFetching first page...")
    response = session.get(BASE_URL, timeout=30)
    total_pages = get_total_pages(response.text)
    print(f"Found {total_pages} pages to scrape")

    # Initialize database
    conn = get_connection()
    init_schema(conn)

    total_saved = 0
    total_skipped = 0
    all_tournaments = []

    # Scrape all pages
    for page_num in tqdm(range(1, total_pages + 1), desc="Scraping pages"):
        try:
            if page_num == 1:
                html = response.text
            else:
                page_url = f"{BASE_URL}&page={page_num}"
                response = session.get(page_url, timeout=30)
                html = response.text
                time.sleep(0.5)  # Be nice to the server

            # Check if we got blocked
            if "Just a moment" in html or response.status_code == 403:
                print(f"\n  Blocked on page {page_num}! Cookies may have expired.")
                break

            tournaments = scrape_page(html)

            if tournaments:
                saved, skipped = save_tournaments(tournaments, conn)
                total_saved += saved
                total_skipped += skipped
                all_tournaments.extend(tournaments)

        except Exception as e:
            print(f"\n  Error on page {page_num}: {e}")

    # Run normalization to fix any typos
    if total_saved > 0:
        print("\nNormalizing data (fixing typos)...")
        fixed = normalize_data(conn)
        if fixed > 0:
            print(f"Fixed {fixed} records")

    conn.commit()
    conn.close()

    print()
    print("=" * 70)
    print("SCRAPING COMPLETE")
    print("=" * 70)
    print(f"Total tournaments found: {len(all_tournaments)}")
    print(f"New tournaments saved: {total_saved}")
    print(f"Duplicates skipped: {total_skipped}")
    print()
    print(f"Cookies saved to: {COOKIES_FILE}")
    print("(They'll be reused next time if still valid)")


if __name__ == "__main__":
    main()
