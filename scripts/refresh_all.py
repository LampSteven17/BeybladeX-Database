#!/usr/bin/env python3
"""
Unified Data Pipeline for BeybladeX-Database.

Main entry point for refreshing all data sources:
- WBO (World Beyblade Organization forum)
- JP (Japanese tournaments from okuyama3093.com)
- DE (German tournaments from BLG Instagram)

Usage:
    python scripts/refresh_all.py                    # Full refresh all sources
    python scripts/refresh_all.py --sources wbo      # WBO only
    python scripts/refresh_all.py --sources wbo,jp   # Specific sources
    python scripts/refresh_all.py --incremental      # No clear, just add new
    python scripts/refresh_all.py --stats            # Show stats only
    python scripts/refresh_all.py --clear            # Clear database only
    python scripts/refresh_all.py -v                 # Verbose logging
"""

import argparse
import sys
from pathlib import Path

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection, init_schema, normalize_data, database_lock, DatabaseLockError
from scrapers import WBOScraper, JPScraper, DEScraper
from import_championships import (
    init_data_file as init_champ_data,
    import_championships,
    clear_championship_data,
    get_stats as get_champ_stats,
)


# =============================================================================
# Available Scrapers
# =============================================================================

SCRAPERS = {
    "wbo": WBOScraper,
    "jp": JPScraper,
    "de": DEScraper,
}

# Default order of scraping (champ is handled separately)
DEFAULT_ORDER = ["wbo", "jp", "de", "champ"]


# =============================================================================
# CLI Functions
# =============================================================================

def show_stats(conn, sources: list[str] = None):
    """Display database statistics."""
    print("\n" + "=" * 60)
    print("DATABASE STATISTICS")
    print("=" * 60)

    # Overall stats
    total_tournaments = conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0]
    total_placements = conn.execute("SELECT COUNT(*) FROM placements").fetchone()[0]

    print(f"\nTotal tournaments: {total_tournaments}")
    print(f"Total placements: {total_placements}")

    # Per-source stats
    print("\n" + "-" * 40)
    print("BY SOURCE:")
    print("-" * 40)

    source_list = sources or DEFAULT_ORDER
    for source_name in source_list:
        if source_name == "champ":
            # Special handling for championships
            stats = get_champ_stats(conn)
            print(f"  {stats['source']:12} {stats['tournaments']:5} tournaments, {stats['placements']:5} placements")
        elif source_name in SCRAPERS:
            scraper = SCRAPERS[source_name]()
            stats = scraper.get_stats(conn)
            print(f"  {stats['source']:12} {stats['tournaments']:5} tournaments, {stats['placements']:5} placements")

    # Top blades
    print("\n" + "-" * 40)
    print("TOP 10 BLADES:")
    print("-" * 40)
    for row in conn.execute("""
        SELECT part_name, total_placements, win_rate
        FROM part_stats
        WHERE part_type = 'blade'
        ORDER BY total_placements DESC
        LIMIT 10
    """).fetchall():
        print(f"  {row[0]:25} {row[1]:4} uses, {row[2]:.1%} win rate")

    # Top combos
    print("\n" + "-" * 40)
    print("TOP 10 COMBOS:")
    print("-" * 40)
    for row in conn.execute("""
        SELECT combo, total_placements, win_rate
        FROM combo_stats
        ORDER BY total_placements DESC
        LIMIT 10
    """).fetchall():
        print(f"  {row[0]:30} {row[1]:4} uses, {row[2]:.1%} win rate")

    # Regional breakdown
    print("\n" + "-" * 40)
    print("BY REGION:")
    print("-" * 40)
    for row in conn.execute("""
        SELECT COALESCE(region, 'Unknown') as region, COUNT(*) as count
        FROM tournaments
        GROUP BY region
        ORDER BY count DESC
    """).fetchall():
        print(f"  {row[0]:12} {row[1]:5} tournaments")

    print("\n" + "=" * 60)


def clear_database(conn, sources: list[str] = None):
    """Clear data from specified sources."""
    source_list = sources or DEFAULT_ORDER

    print("\nClearing database...")

    total_deleted = 0
    for source_name in source_list:
        if source_name == "champ":
            count = clear_championship_data(conn)
            total_deleted += count
            print(f"  Championships: deleted {count} tournaments")
        elif source_name in SCRAPERS:
            scraper = SCRAPERS[source_name]()
            count = scraper.clear_source_data(conn)
            total_deleted += count
            print(f"  {scraper.source_name}: deleted {count} tournaments")
        else:
            print(f"  Unknown source: {source_name}")

    conn.commit()
    print(f"\nTotal deleted: {total_deleted} tournaments")
    return total_deleted


def run_scrapers(conn, sources: list[str], incremental: bool = False, verbose: bool = False):
    """Run scrapers for specified sources."""
    results = {}

    for source_name in sources:
        # Special handling for championships
        if source_name == "champ":
            print(f"\n{'=' * 60}")
            print("IMPORTING: Championships")
            print("=" * 60)

            if not incremental:
                deleted = clear_championship_data(conn)
                if verbose:
                    print(f"Cleared {deleted} existing championship tournaments")

            data = init_champ_data()
            added, placements = import_championships(conn, data, verbose=verbose)
            results["champ"] = {"status": "OK", "added": added, "skipped": 0, "error": None}
            print(f"\nChampionships: Added {added} tournaments, {placements} placements")
            continue

        if source_name not in SCRAPERS:
            print(f"Unknown source: {source_name}")
            continue

        scraper = SCRAPERS[source_name]()
        print(f"\n{'=' * 60}")
        print(f"SCRAPING: {scraper.source_name}")
        print("=" * 60)

        # Clear source data unless incremental
        if not incremental:
            deleted = scraper.clear_source_data(conn)
            conn.commit()
            if verbose:
                print(f"Cleared {deleted} existing {scraper.source_name} tournaments")

        # Run scraper
        try:
            added, skipped = scraper.scrape(conn, verbose=verbose)
            results[source_name] = {"added": added, "skipped": skipped, "error": None}
            print(f"\n{scraper.source_name}: Added {added}, Skipped {skipped}")
        except Exception as e:
            results[source_name] = {"added": 0, "skipped": 0, "error": str(e)}
            print(f"\n{scraper.source_name}: ERROR - {e}")

        conn.commit()

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Unified data pipeline for BeybladeX-Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/refresh_all.py                    # Full refresh all sources
  python scripts/refresh_all.py --sources wbo      # WBO only
  python scripts/refresh_all.py --sources wbo,jp   # Specific sources
  python scripts/refresh_all.py --incremental      # No clear, just add new
  python scripts/refresh_all.py --stats            # Show stats only
  python scripts/refresh_all.py --clear            # Clear database only
  python scripts/refresh_all.py -v                 # Verbose logging
        """
    )

    all_sources = list(SCRAPERS.keys()) + ["champ"]
    parser.add_argument(
        "--sources",
        type=str,
        help=f"Comma-separated list of sources to process. Available: {', '.join(all_sources)}",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Don't clear existing data, only add new entries",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics only (no scraping)",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear database only (no scraping)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output with progress bars",
    )
    parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Skip data normalization after import",
    )

    args = parser.parse_args()

    # Parse sources
    if args.sources:
        sources = [s.strip().lower() for s in args.sources.split(",")]
        # Validate sources
        valid_sources = set(SCRAPERS.keys()) | {"champ"}
        invalid = [s for s in sources if s not in valid_sources]
        if invalid:
            print(f"Unknown sources: {', '.join(invalid)}")
            print(f"Available: {', '.join(valid_sources)}")
            sys.exit(1)
    else:
        sources = DEFAULT_ORDER

    # Stats-only mode: use read-only connection, no lock needed
    if args.stats:
        conn = get_connection(read_only=True)
        try:
            show_stats(conn, sources)
        finally:
            conn.close()
        return

    # Write operations: acquire exclusive lock to prevent concurrent scrapes
    try:
        lock_context = database_lock()
        lock_context.__enter__()
    except DatabaseLockError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    try:
        # Connect to database (write mode)
        conn = get_connection()
        init_schema(conn)

        # Clear only mode
        if args.clear:
            clear_database(conn, sources)
            show_stats(conn, sources)
            return

        # Full pipeline
        print("\n" + "=" * 60)
        print("BEYBLADEX DATABASE REFRESH")
        print("=" * 60)
        print(f"Sources: {', '.join(sources)}")
        print(f"Mode: {'Incremental' if args.incremental else 'Full refresh'}")

        # Run scrapers
        results = run_scrapers(conn, sources, incremental=args.incremental, verbose=args.verbose)

        # Normalize data
        if not args.no_normalize:
            print("\n" + "-" * 40)
            print("Normalizing data...")
            fixed_count = normalize_data(conn)
            if fixed_count > 0:
                print(f"Fixed {fixed_count} typos/inconsistencies")
                conn.commit()

        # Show summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)

        total_added = 0
        total_skipped = 0
        errors = []

        for source_name, result in results.items():
            status = "ERROR" if result["error"] else "OK"
            print(f"  {source_name:6} {status:6} Added: {result['added']:4}, Skipped: {result['skipped']:4}")
            total_added += result["added"]
            total_skipped += result["skipped"]
            if result["error"]:
                errors.append(f"{source_name}: {result['error']}")

        print("-" * 40)
        print(f"  TOTAL        Added: {total_added:4}, Skipped: {total_skipped:4}")

        if errors:
            print("\nErrors:")
            for error in errors:
                print(f"  - {error}")

        # Show final stats
        show_stats(conn, sources)

    finally:
        conn.close()
        # Release database lock
        lock_context.__exit__(None, None, None)


if __name__ == "__main__":
    main()
