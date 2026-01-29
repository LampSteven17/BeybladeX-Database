"""
Import WBO pages from browser console download.

This reads the wbo_pages.json file created by the browser console script
and parses it into the database.

Usage:
    python scripts/import_browser_json.py
    python scripts/import_browser_json.py fresh  # Clear existing WBO data first
"""

import sys
import json
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

from db import get_connection, init_schema, normalize_data
from scraper import parse_post, insert_tournament, get_processed_post_ids


DATA_FILE = Path(__file__).parent.parent / "data" / "wbo_pages.json"


def main():
    fresh = len(sys.argv) > 1 and sys.argv[1] == "fresh"

    if not DATA_FILE.exists():
        print(f"ERROR: {DATA_FILE} not found")
        print()
        print("Download it using the browser console script, then copy to data/")
        return

    print("=" * 60)
    print("WBO Browser JSON Importer")
    print("=" * 60)

    # Load JSON
    print(f"Loading {DATA_FILE}...")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        pages_data = json.load(f)

    print(f"Found {len(pages_data)} pages")

    # Connect to database
    conn = get_connection()
    init_schema(conn)

    if fresh:
        print("Fresh import - clearing existing WBO data...")
        conn.execute(
            "DELETE FROM placements WHERE tournament_id IN (SELECT id FROM tournaments WHERE wbo_post_id IS NOT NULL AND wbo_post_id NOT LIKE 'jp_%' AND wbo_post_id NOT LIKE 'de_%')"
        )
        conn.execute(
            "DELETE FROM tournaments WHERE wbo_post_id IS NOT NULL AND wbo_post_id NOT LIKE 'jp_%' AND wbo_post_id NOT LIKE 'de_%'"
        )
        conn.commit()

    # Get already processed IDs
    processed_ids = get_processed_post_ids(conn)
    print(f"Already processed {len(processed_ids)} posts")

    tournaments_added = 0
    tournaments_skipped = 0

    # Process each page
    # Keys are page numbers as strings
    page_nums = sorted([int(k) for k in pages_data.keys()])

    for page_num in tqdm(page_nums, desc="Processing pages"):
        html = pages_data[str(page_num)]

        # Skip Cloudflare challenge pages
        if "Just a moment" in html:
            print(f"\nPage {page_num} is a Cloudflare challenge page, skipping")
            continue

        soup = BeautifulSoup(html, "html.parser")
        posts = soup.find_all("div", class_="post")

        for post in posts:
            post_id = post.get("id", "")

            # Skip if already processed
            if post_id in processed_ids:
                tournaments_skipped += 1
                continue

            try:
                tournaments = parse_post(post)

                for tournament in tournaments:
                    if tournament.wbo_post_id in processed_ids:
                        tournaments_skipped += 1
                        continue

                    result = insert_tournament(conn, tournament)
                    if result:
                        tournaments_added += 1
                        processed_ids.add(tournament.wbo_post_id)
                    else:
                        tournaments_skipped += 1

            except Exception as e:
                print(f"\nError parsing post {post_id}: {e}")

        conn.commit()

    # Normalize data
    print("\nNormalizing data...")
    fixed = normalize_data(conn)
    if fixed > 0:
        print(f"Fixed {fixed} records")
        conn.commit()

    conn.close()

    print()
    print("=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"Tournaments added: {tournaments_added}")
    print(f"Skipped (duplicates/invalid): {tournaments_skipped}")


if __name__ == "__main__":
    main()
