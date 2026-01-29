"""
WBO Scraper using Playwright - Handles Cloudflare automatically.

This uses a real browser (Chromium) that can solve Cloudflare challenges.
Run this on Windows for best results.

Install:
    pip install playwright beautifulsoup4 tqdm
    playwright install chromium

Usage:
    python wbo_playwright.py           # Download all pages
    python wbo_playwright.py --pages 5 # Download first 5 pages only
"""

import argparse
import re
import time
import json
from pathlib import Path
from datetime import datetime

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    from bs4 import BeautifulSoup
    from tqdm import tqdm
except ImportError as e:
    print(f"Missing package: {e}")
    print("\nInstall required packages:")
    print("  pip install playwright beautifulsoup4 tqdm")
    print("  playwright install chromium")
    exit(1)


BASE_URL = "https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX"

# Output directory - works on both Windows and WSL
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR.parent / "data" / "wbo_pages"


def get_total_pages(html: str) -> int:
    """Extract total page count from pagination."""
    matches = re.findall(r"page=(\d+)", html)
    return max(int(m) for m in matches) if matches else 1


def download_pages(max_pages: int | None = None, headless: bool = False):
    """
    Download all WBO pages using Playwright.

    Args:
        max_pages: Limit to this many pages (None for all)
        headless: Run browser without UI (may fail Cloudflare more often)
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("WBO Scraper - Playwright Edition")
    print("=" * 60)
    print(f"Output: {OUTPUT_DIR}")
    print(f"Headless: {headless}")
    print()

    with sync_playwright() as p:
        # Launch browser - NOT headless by default for better Cloudflare bypass
        print("Launching browser...")
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
            ]
        )

        # Create context with realistic settings
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        page = context.new_page()

        # Navigate to first page
        print("Loading WBO (this may take a moment for Cloudflare)...")
        try:
            page.goto(BASE_URL, timeout=60000, wait_until="networkidle")
        except PlaywrightTimeout:
            print("Timeout on initial load - Cloudflare may be challenging.")
            print("Waiting for manual resolution...")
            page.wait_for_load_state("networkidle", timeout=120000)

        # Check for Cloudflare challenge
        content = page.content()
        if "Just a moment" in content or "challenge" in content.lower():
            print("\nCloudflare challenge detected!")
            print("Please solve the challenge in the browser window...")
            print("(The script will continue automatically once resolved)")

            # Wait for the challenge to be solved
            try:
                page.wait_for_selector("div.post", timeout=120000)
                print("Challenge solved!")
            except PlaywrightTimeout:
                print("ERROR: Cloudflare challenge not solved in time.")
                browser.close()
                return

        # Verify we're on the right page
        content = page.content()
        if "Winning Combinations" not in content:
            print("ERROR: Not on the expected WBO page.")
            print("Page title:", page.title())
            browser.close()
            return

        print("SUCCESS: Connected to WBO!")

        # Get total pages
        total_pages = get_total_pages(content)
        if max_pages:
            total_pages = min(total_pages, max_pages)
        print(f"Found {total_pages} pages to download")

        # Check existing pages
        existing = set()
        for f in OUTPUT_DIR.glob("page_*.html"):
            match = re.search(r"page_(\d+)\.html", f.name)
            if match:
                existing.add(int(match.group(1)))

        if existing:
            print(f"Already have {len(existing)} pages")

        # Download pages
        downloaded = 0
        skipped = 0
        errors = 0

        for page_num in tqdm(range(1, total_pages + 1), desc="Downloading"):
            output_file = OUTPUT_DIR / f"page_{page_num:03d}.html"

            # Skip if exists
            if page_num in existing:
                skipped += 1
                continue

            try:
                if page_num == 1:
                    html = content
                else:
                    url = f"{BASE_URL}&page={page_num}"
                    page.goto(url, timeout=30000, wait_until="networkidle")
                    html = page.content()
                    time.sleep(0.5)  # Small delay to be nice

                # Check for Cloudflare block
                if "Just a moment" in html:
                    print(f"\nBlocked on page {page_num}! Waiting for challenge...")
                    try:
                        page.wait_for_selector("div.post", timeout=60000)
                        html = page.content()
                    except PlaywrightTimeout:
                        print("Challenge not solved - stopping.")
                        break

                # Save HTML
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(html)

                downloaded += 1

            except Exception as e:
                print(f"\nError on page {page_num}: {e}")
                errors += 1

        browser.close()

    print()
    print("=" * 60)
    print("DOWNLOAD COMPLETE")
    print("=" * 60)
    print(f"Downloaded: {downloaded}")
    print(f"Skipped (already had): {skipped}")
    print(f"Errors: {errors}")
    print(f"Files saved to: {OUTPUT_DIR}")
    print()
    print("Next step - parse the pages:")
    print("  python scripts/scraper.py local")


def main():
    parser = argparse.ArgumentParser(description="Download WBO pages using Playwright")
    parser.add_argument("--pages", type=int, help="Max pages to download")
    parser.add_argument("--headless", action="store_true", help="Run headless (may fail Cloudflare)")
    args = parser.parse_args()

    download_pages(max_pages=args.pages, headless=args.headless)


if __name__ == "__main__":
    main()
