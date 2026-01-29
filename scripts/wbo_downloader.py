"""
WBO HTML Page Downloader for Windows.

This script ONLY downloads HTML pages and saves them to disk.
The actual parsing is done by scraper.py reading these files.

This separation allows:
- Windows (with browser cookies) to handle Cloudflare bypass
- Linux/WSL (with all the parsing code) to do the actual data extraction

Usage (on Windows):
    python wbo_downloader.py

Output:
    data/wbo_pages/page_001.html
    data/wbo_pages/page_002.html
    ...

Then on Linux/WSL:
    python scripts/scraper.py local
"""

import os
import re
import time
from pathlib import Path

import requests

# === CONFIGURATION ===
BASE_URL = "https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX"

# Output directory - saves directly to WSL-accessible path
# This writes to the shared BeybladeX-Database folder
OUTPUT_DIR = Path(r"C:\Users\RTX-MONSTER\BeybladeX-Database\data\wbo_pages")

# Delay between page requests (seconds)
DELAY = 1.0

# === PASTE YOUR COOKIES HERE ===
# Get these from your browser after visiting WBO:
# 1. Open WBO in Firefox/Chrome
# 2. Open DevTools (F12) -> Console
# 3. Type: document.cookie
# 4. Copy the result and paste below
COOKIES = """
mybb[lastvisit]=1769181208; mybb[lastactive]=1769186508; mybb[threadread]=a%3A3%3A%7Bi%3A110113%3Bi%3A1769186506%3Bi%3A122672%3Bi%3A1768938547%3Bi%3A123107%3Bi%3A1769122974%3B%7D; mybb[announcements]=0; cf_clearance=WRnzCsC09vPNqTDSQGjCcO_h_PMFL9pDYubIF9cOhAA-1769186101-1.2.1.1-pDFMaynMDzJpHTyvlojlChgcjAhrr5QvzsHbMDVJf713f0ys1AvouCTpxaME6kZmULvdSw0Jb1M8LvxOT2WrSqGkaYprf1KehyoEcyBEqgO6Av5OrKs1K982gmXwubZ7wS1GlIoQ9eLg33Bmp5MK7X5M6GIxO6tZVVvck_eoYSImL.vB2K9hUoHnFlK1vYg6YK5zSe630thW1oNJPMj2O2Za5mVyD76fNq1ldiDmraxp3r2X6W4B3jq_CiQ3KoyY
""".strip()

# User agent (match your browser)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0"
)


def parse_cookies(cookie_str: str) -> dict:
    """Parse cookie string into dict."""
    cookies = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()
    return cookies


def get_total_pages(html: str) -> int:
    """Extract total page count from HTML."""
    matches = re.findall(r"page=(\d+)", html)
    if matches:
        return max(int(m) for m in matches)
    return 1


def main():
    print("=" * 60)
    print("WBO HTML Downloader")
    print("=" * 60)

    # Setup output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")

    # Setup session
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )

    # Parse and set cookies
    if not COOKIES:
        print("\nERROR: No cookies configured!")
        print(
            "Edit this script and paste your browser cookies in the COOKIES variable."
        )
        print("\nTo get cookies:")
        print("1. Open WBO in your browser")
        print("2. Open DevTools (F12) -> Console")
        print("3. Type: document.cookie")
        print("4. Copy the result")
        return

    cookies = parse_cookies(COOKIES)
    session.cookies.update(cookies)
    print(f"Loaded {len(cookies)} cookies")

    # Fetch first page
    print("\nFetching first page...")
    try:
        response = session.get(BASE_URL, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"ERROR fetching first page: {e}")
        return

    # Check if blocked by Cloudflare
    if "Just a moment" in response.text or response.status_code == 403:
        print("\nBLOCKED by Cloudflare!")
        print("Your cookies may be expired. Get fresh ones from your browser.")
        return

    if "Winning Combinations" not in response.text:
        print("\nWARNING: Page content doesn't look right.")
        print("Check if you're logged in and cookies are valid.")

    # Get total pages
    total_pages = get_total_pages(response.text)
    print(f"Found {total_pages} pages to download")

    # Check what we already have
    existing = set()
    for f in OUTPUT_DIR.glob("page_*.html"):
        match = re.search(r"page_(\d+)\.html", f.name)
        if match:
            existing.add(int(match.group(1)))

    if existing:
        print(f"Already have {len(existing)} pages downloaded")

    # Download pages
    downloaded = 0
    skipped = 0
    errors = 0

    for page_num in range(1, total_pages + 1):
        output_file = OUTPUT_DIR / f"page_{page_num:03d}.html"

        # Skip if already exists
        if page_num in existing:
            skipped += 1
            continue

        try:
            if page_num == 1:
                html = response.text
            else:
                time.sleep(DELAY)
                url = f"{BASE_URL}?page={page_num}"
                response = session.get(url, timeout=30)
                response.raise_for_status()
                html = response.text

            # Check for Cloudflare block
            if "Just a moment" in html:
                print(f"\nBlocked on page {page_num}! Cookies expired.")
                break

            # Save HTML
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(html)

            downloaded += 1
            print(f"  Downloaded page {page_num}/{total_pages}")

        except Exception as e:
            print(f"  ERROR on page {page_num}: {e}")
            errors += 1

    print()
    print("=" * 60)
    print("DOWNLOAD COMPLETE")
    print("=" * 60)
    print(f"Downloaded: {downloaded} pages")
    print(f"Skipped (already had): {skipped} pages")
    print(f"Errors: {errors}")
    print(f"Files saved to: {OUTPUT_DIR}")
    print()
    print("Next step: Run the parser on Linux/WSL:")
    print("  cd /home/rtx-monster/BeybladeX-Database")
    print("  python scripts/scraper.py local")


if __name__ == "__main__":
    main()
