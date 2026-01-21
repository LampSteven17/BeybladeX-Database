"""
Analysis tools for Beyblade X tournament data.

Scoring system:
- Recency weighting: Recent results weighted higher (exponential decay)
- Placement scoring: 1st = 3 pts, 2nd = 2 pts, 3rd = 1 pt
- Combined score = sum(placement_points * recency_weight)
"""

import argparse
import math
from datetime import datetime, timedelta
from typing import Optional

from db import get_connection


# Scoring configuration
PLACEMENT_POINTS = {1: 3, 2: 2, 3: 1}  # Points per placement
RECENCY_HALF_LIFE_DAYS = 90  # Results lose half their weight every 90 days


def calculate_recency_weight(tournament_date: datetime, reference_date: datetime = None) -> float:
    """
    Calculate recency weight using exponential decay.

    More recent = higher weight (max 1.0)
    Weight halves every RECENCY_HALF_LIFE_DAYS days.
    """
    if reference_date is None:
        reference_date = datetime.now()

    if tournament_date is None:
        return 0.5  # Default for unknown dates

    # Handle date objects
    if hasattr(tournament_date, 'date'):
        tournament_date = tournament_date

    days_ago = (reference_date.date() - tournament_date).days if hasattr(tournament_date, 'days') == False else (reference_date - tournament_date).days

    # Clamp to non-negative
    days_ago = max(0, days_ago)

    # Exponential decay: weight = 0.5^(days/half_life)
    weight = math.pow(0.5, days_ago / RECENCY_HALF_LIFE_DAYS)

    return weight


def calculate_placement_score(place: int) -> int:
    """Get points for a placement."""
    return PLACEMENT_POINTS.get(place, 0)


def ranked_combos(limit: int = 20, min_uses: int = 2) -> list[dict]:
    """
    Get combos ranked by weighted score.

    Score = sum(placement_points * recency_weight) for each appearance

    Returns combos with:
    - raw_score: Total weighted score
    - uses: Number of times used
    - avg_score: Average score per use
    - placements: Breakdown by 1st/2nd/3rd
    - recent_trend: Score from last 30 days vs prior
    """
    conn = get_connection()

    # Get all combo placements with dates
    results = conn.execute("""
        SELECT
            blade,
            ratchet,
            bit,
            place,
            tournament_date
        FROM combo_usage
        ORDER BY tournament_date DESC
    """).fetchall()
    conn.close()

    # Calculate scores per combo
    combo_scores = {}
    reference_date = datetime.now()
    thirty_days_ago = reference_date - timedelta(days=30)

    for blade, ratchet, bit, place, tournament_date in results:
        combo_key = (blade, ratchet, bit)

        if combo_key not in combo_scores:
            combo_scores[combo_key] = {
                "blade": blade,
                "ratchet": ratchet,
                "bit": bit,
                "raw_score": 0.0,
                "recent_score": 0.0,  # Last 30 days
                "older_score": 0.0,   # Before last 30 days
                "uses": 0,
                "first": 0,
                "second": 0,
                "third": 0,
            }

        # Calculate weighted score for this placement
        recency_weight = calculate_recency_weight(tournament_date, reference_date)
        placement_points = calculate_placement_score(place)
        weighted_score = placement_points * recency_weight

        combo_scores[combo_key]["raw_score"] += weighted_score
        combo_scores[combo_key]["uses"] += 1

        # Track placement counts
        if place == 1:
            combo_scores[combo_key]["first"] += 1
        elif place == 2:
            combo_scores[combo_key]["second"] += 1
        elif place == 3:
            combo_scores[combo_key]["third"] += 1

        # Track recent vs older
        if tournament_date and tournament_date >= thirty_days_ago.date():
            combo_scores[combo_key]["recent_score"] += weighted_score
        else:
            combo_scores[combo_key]["older_score"] += weighted_score

    # Convert to list and calculate derived metrics
    combo_list = []
    for key, data in combo_scores.items():
        if data["uses"] < min_uses:
            continue

        data["combo"] = f"{data['blade']} {data['ratchet']}{data['bit']}"
        data["avg_score"] = data["raw_score"] / data["uses"]

        # Trend: positive = gaining popularity, negative = declining
        # Compare recent weighted score to what we'd expect if evenly distributed
        if data["older_score"] > 0:
            data["trend"] = (data["recent_score"] / max(data["older_score"], 0.1)) - 1.0
        else:
            data["trend"] = 1.0 if data["recent_score"] > 0 else 0.0

        combo_list.append(data)

    # Sort by raw_score (recency-weighted total)
    combo_list.sort(key=lambda x: x["raw_score"], reverse=True)

    return combo_list[:limit]


def ranked_blades(limit: int = 20, min_uses: int = 3) -> list[dict]:
    """
    Get blades ranked by weighted score.
    """
    conn = get_connection()

    results = conn.execute("""
        SELECT
            blade,
            place,
            tournament_date
        FROM combo_usage
        ORDER BY tournament_date DESC
    """).fetchall()
    conn.close()

    blade_scores = {}
    reference_date = datetime.now()
    thirty_days_ago = reference_date - timedelta(days=30)

    for blade, place, tournament_date in results:
        if blade not in blade_scores:
            blade_scores[blade] = {
                "blade": blade,
                "raw_score": 0.0,
                "recent_score": 0.0,
                "older_score": 0.0,
                "uses": 0,
                "first": 0,
                "second": 0,
                "third": 0,
            }

        recency_weight = calculate_recency_weight(tournament_date, reference_date)
        placement_points = calculate_placement_score(place)
        weighted_score = placement_points * recency_weight

        blade_scores[blade]["raw_score"] += weighted_score
        blade_scores[blade]["uses"] += 1

        if place == 1:
            blade_scores[blade]["first"] += 1
        elif place == 2:
            blade_scores[blade]["second"] += 1
        elif place == 3:
            blade_scores[blade]["third"] += 1

        if tournament_date and tournament_date >= thirty_days_ago.date():
            blade_scores[blade]["recent_score"] += weighted_score
        else:
            blade_scores[blade]["older_score"] += weighted_score

    blade_list = []
    for blade, data in blade_scores.items():
        if data["uses"] < min_uses:
            continue

        data["avg_score"] = data["raw_score"] / data["uses"]

        if data["older_score"] > 0:
            data["trend"] = (data["recent_score"] / max(data["older_score"], 0.1)) - 1.0
        else:
            data["trend"] = 1.0 if data["recent_score"] > 0 else 0.0

        blade_list.append(data)

    blade_list.sort(key=lambda x: x["raw_score"], reverse=True)

    return blade_list[:limit]


def ranked_ratchets(limit: int = 15, min_uses: int = 3) -> list[dict]:
    """Get ratchets ranked by weighted score."""
    conn = get_connection()

    results = conn.execute("""
        SELECT ratchet, place, tournament_date
        FROM combo_usage
    """).fetchall()
    conn.close()

    return _rank_parts(results, limit, min_uses, "ratchet")


def ranked_bits(limit: int = 15, min_uses: int = 3) -> list[dict]:
    """Get bits ranked by weighted score."""
    conn = get_connection()

    results = conn.execute("""
        SELECT bit, place, tournament_date
        FROM combo_usage
    """).fetchall()
    conn.close()

    return _rank_parts(results, limit, min_uses, "bit")


def _rank_parts(results, limit: int, min_uses: int, part_name: str) -> list[dict]:
    """Generic ranking for parts (ratchets, bits)."""
    part_scores = {}
    reference_date = datetime.now()
    thirty_days_ago = reference_date - timedelta(days=30)

    for part, place, tournament_date in results:
        if part not in part_scores:
            part_scores[part] = {
                part_name: part,
                "raw_score": 0.0,
                "recent_score": 0.0,
                "older_score": 0.0,
                "uses": 0,
                "first": 0,
                "second": 0,
                "third": 0,
            }

        recency_weight = calculate_recency_weight(tournament_date, reference_date)
        placement_points = calculate_placement_score(place)
        weighted_score = placement_points * recency_weight

        part_scores[part]["raw_score"] += weighted_score
        part_scores[part]["uses"] += 1

        if place == 1:
            part_scores[part]["first"] += 1
        elif place == 2:
            part_scores[part]["second"] += 1
        elif place == 3:
            part_scores[part]["third"] += 1

        if tournament_date and tournament_date >= thirty_days_ago.date():
            part_scores[part]["recent_score"] += weighted_score
        else:
            part_scores[part]["older_score"] += weighted_score

    part_list = []
    for part, data in part_scores.items():
        if data["uses"] < min_uses:
            continue

        data["avg_score"] = data["raw_score"] / data["uses"]

        if data["older_score"] > 0:
            data["trend"] = (data["recent_score"] / max(data["older_score"], 0.1)) - 1.0
        else:
            data["trend"] = 1.0 if data["recent_score"] > 0 else 0.0

        part_list.append(data)

    part_list.sort(key=lambda x: x["raw_score"], reverse=True)

    return part_list[:limit]


def best_combos_for_blade(blade: str, limit: int = 10) -> list[dict]:
    """
    Get best ratchet+bit combinations for a specific blade.
    Ranked by weighted score.
    """
    conn = get_connection()

    results = conn.execute("""
        SELECT ratchet, bit, place, tournament_date
        FROM combo_usage
        WHERE LOWER(blade) = LOWER(?)
    """, [blade]).fetchall()
    conn.close()

    combo_scores = {}
    reference_date = datetime.now()

    for ratchet, bit, place, tournament_date in results:
        key = (ratchet, bit)

        if key not in combo_scores:
            combo_scores[key] = {
                "ratchet": ratchet,
                "bit": bit,
                "combo": f"{ratchet}{bit}",
                "raw_score": 0.0,
                "uses": 0,
                "first": 0,
                "second": 0,
                "third": 0,
            }

        recency_weight = calculate_recency_weight(tournament_date, reference_date)
        placement_points = calculate_placement_score(place)

        combo_scores[key]["raw_score"] += placement_points * recency_weight
        combo_scores[key]["uses"] += 1

        if place == 1:
            combo_scores[key]["first"] += 1
        elif place == 2:
            combo_scores[key]["second"] += 1
        elif place == 3:
            combo_scores[key]["third"] += 1

    combo_list = list(combo_scores.values())
    combo_list.sort(key=lambda x: x["raw_score"], reverse=True)

    return combo_list[:limit]


def compare_blades(blade1: str, blade2: str) -> dict:
    """
    Compare two blades head-to-head.

    Returns comparative stats including:
    - Overall scores
    - Placement distributions
    - Recent performance
    - Tournaments where both appeared
    """
    conn = get_connection()

    # Get stats for both blades
    blade1_data = conn.execute("""
        SELECT place, tournament_date, tournament_id
        FROM combo_usage
        WHERE LOWER(blade) = LOWER(?)
    """, [blade1]).fetchall()

    blade2_data = conn.execute("""
        SELECT place, tournament_date, tournament_id
        FROM combo_usage
        WHERE LOWER(blade) = LOWER(?)
    """, [blade2]).fetchall()

    conn.close()

    reference_date = datetime.now()

    def calc_blade_stats(data):
        stats = {"raw_score": 0, "uses": 0, "first": 0, "second": 0, "third": 0, "tournaments": set()}
        for place, date, tid in data:
            weight = calculate_recency_weight(date, reference_date)
            points = calculate_placement_score(place)
            stats["raw_score"] += points * weight
            stats["uses"] += 1
            stats["tournaments"].add(tid)
            if place == 1:
                stats["first"] += 1
            elif place == 2:
                stats["second"] += 1
            elif place == 3:
                stats["third"] += 1
        return stats

    stats1 = calc_blade_stats(blade1_data)
    stats2 = calc_blade_stats(blade2_data)

    # Find tournaments where both appeared
    common_tournaments = stats1["tournaments"] & stats2["tournaments"]

    # In common tournaments, who placed higher?
    blade1_higher = 0
    blade2_higher = 0
    ties = 0

    blade1_by_tournament = {}
    for place, date, tid in blade1_data:
        if tid not in blade1_by_tournament or place < blade1_by_tournament[tid]:
            blade1_by_tournament[tid] = place

    blade2_by_tournament = {}
    for place, date, tid in blade2_data:
        if tid not in blade2_by_tournament or place < blade2_by_tournament[tid]:
            blade2_by_tournament[tid] = place

    for tid in common_tournaments:
        p1 = blade1_by_tournament.get(tid, 99)
        p2 = blade2_by_tournament.get(tid, 99)
        if p1 < p2:
            blade1_higher += 1
        elif p2 < p1:
            blade2_higher += 1
        else:
            ties += 1

    return {
        "blade1": {
            "name": blade1,
            "score": round(stats1["raw_score"], 2),
            "uses": stats1["uses"],
            "first": stats1["first"],
            "second": stats1["second"],
            "third": stats1["third"],
            "win_rate": stats1["first"] / stats1["uses"] if stats1["uses"] > 0 else 0,
        },
        "blade2": {
            "name": blade2,
            "score": round(stats2["raw_score"], 2),
            "uses": stats2["uses"],
            "first": stats2["first"],
            "second": stats2["second"],
            "third": stats2["third"],
            "win_rate": stats2["first"] / stats2["uses"] if stats2["uses"] > 0 else 0,
        },
        "head_to_head": {
            "common_tournaments": len(common_tournaments),
            "blade1_placed_higher": blade1_higher,
            "blade2_placed_higher": blade2_higher,
            "ties": ties,
        }
    }


def meta_snapshot(days: int = 30) -> dict:
    """
    Get a snapshot of the current meta (last N days).

    Returns top blades, combos, ratchets, bits for the period.
    """
    conn = get_connection()
    cutoff = datetime.now() - timedelta(days=days)

    # Top blades in period
    blades = conn.execute("""
        SELECT blade, COUNT(*) as uses,
               SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
        FROM combo_usage
        WHERE tournament_date >= ?
        GROUP BY blade
        ORDER BY uses DESC
        LIMIT 10
    """, [cutoff.date()]).fetchall()

    # Top combos in period
    combos = conn.execute("""
        SELECT blade || ' ' || ratchet || bit as combo, COUNT(*) as uses,
               SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
        FROM combo_usage
        WHERE tournament_date >= ?
        GROUP BY blade, ratchet, bit
        ORDER BY uses DESC
        LIMIT 10
    """, [cutoff.date()]).fetchall()

    # Tournament count
    tournaments = conn.execute("""
        SELECT COUNT(*) FROM tournaments WHERE date >= ?
    """, [cutoff.date()]).fetchone()[0]

    conn.close()

    return {
        "period_days": days,
        "tournaments": tournaments,
        "top_blades": [{"blade": r[0], "uses": r[1], "wins": r[2]} for r in blades],
        "top_combos": [{"combo": r[0], "uses": r[1], "wins": r[2]} for r in combos],
    }


def database_summary() -> dict:
    """Get overall database summary."""
    conn = get_connection()

    tournaments = conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0]
    placements = conn.execute("SELECT COUNT(*) FROM placements").fetchone()[0]
    unique_players = conn.execute("SELECT COUNT(DISTINCT player_name) FROM placements").fetchone()[0]
    unique_blades = conn.execute("SELECT COUNT(DISTINCT blade) FROM combo_usage").fetchone()[0]

    date_range = conn.execute("""
        SELECT MIN(date), MAX(date) FROM tournaments
    """).fetchone()

    conn.close()

    return {
        "tournaments": tournaments,
        "placements": placements,
        "unique_players": unique_players,
        "unique_blades": unique_blades,
        "earliest_tournament": date_range[0].strftime('%Y-%m-%d') if date_range[0] else None,
        "latest_tournament": date_range[1].strftime('%Y-%m-%d') if date_range[1] else None,
    }


# =============================================================================
# CLI Interface
# =============================================================================

def print_ranked_table(data: list[dict], title: str, key_field: str):
    """Print a ranked table with scores."""
    print(f"\n=== {title} ===")
    print(f"{'Rank':<5} {key_field:<25} {'Score':<10} {'Uses':<6} {'1st':<5} {'2nd':<5} {'3rd':<5} {'Trend':<8}")
    print("-" * 75)

    for i, row in enumerate(data, 1):
        name = row.get(key_field.lower(), row.get("combo", "?"))
        trend_str = f"{row.get('trend', 0):+.0%}" if row.get('trend') is not None else "N/A"
        print(f"{i:<5} {name:<25} {row['raw_score']:<10.1f} {row['uses']:<6} {row['first']:<5} {row['second']:<5} {row['third']:<5} {trend_str:<8}")


def cli():
    """Command-line interface for analysis."""
    parser = argparse.ArgumentParser(description='Beyblade X Meta Analysis (Weighted Scoring)')
    subparsers = parser.add_subparsers(dest='command', help='Analysis command')

    # Summary
    subparsers.add_parser('summary', help='Database summary')

    # Ranked blades
    blades_parser = subparsers.add_parser('blades', help='Ranked blades by weighted score')
    blades_parser.add_argument('-n', '--limit', type=int, default=15)

    # Ranked combos
    combos_parser = subparsers.add_parser('combos', help='Ranked combos by weighted score')
    combos_parser.add_argument('-n', '--limit', type=int, default=15)

    # Ranked ratchets
    ratchets_parser = subparsers.add_parser('ratchets', help='Ranked ratchets')
    ratchets_parser.add_argument('-n', '--limit', type=int, default=10)

    # Ranked bits
    bits_parser = subparsers.add_parser('bits', help='Ranked bits')
    bits_parser.add_argument('-n', '--limit', type=int, default=10)

    # Best combos for blade
    blade_parser = subparsers.add_parser('blade', help='Best combos for a blade')
    blade_parser.add_argument('name', help='Blade name')

    # Compare blades
    compare_parser = subparsers.add_parser('compare', help='Compare two blades')
    compare_parser.add_argument('blade1', help='First blade')
    compare_parser.add_argument('blade2', help='Second blade')

    # Meta snapshot
    meta_parser = subparsers.add_parser('meta', help='Recent meta snapshot')
    meta_parser.add_argument('-d', '--days', type=int, default=30, help='Days to look back')

    args = parser.parse_args()

    if args.command == 'summary':
        summary = database_summary()
        print("\n=== DATABASE SUMMARY ===")
        for k, v in summary.items():
            print(f"  {k}: {v}")

    elif args.command == 'blades':
        data = ranked_blades(args.limit)
        print_ranked_table(data, "RANKED BLADES (Recency-Weighted)", "Blade")

    elif args.command == 'combos':
        data = ranked_combos(args.limit)
        print_ranked_table(data, "RANKED COMBOS (Recency-Weighted)", "Combo")

    elif args.command == 'ratchets':
        data = ranked_ratchets(args.limit)
        print_ranked_table(data, "RANKED RATCHETS", "Ratchet")

    elif args.command == 'bits':
        data = ranked_bits(args.limit)
        print_ranked_table(data, "RANKED BITS", "Bit")

    elif args.command == 'blade':
        data = best_combos_for_blade(args.name)
        print(f"\n=== BEST COMBOS FOR {args.name.upper()} ===")
        print(f"{'Rank':<5} {'Combo':<15} {'Score':<10} {'Uses':<6} {'1st':<5} {'2nd':<5} {'3rd':<5}")
        print("-" * 55)
        for i, row in enumerate(data, 1):
            print(f"{i:<5} {row['combo']:<15} {row['raw_score']:<10.1f} {row['uses']:<6} {row['first']:<5} {row['second']:<5} {row['third']:<5}")

    elif args.command == 'compare':
        data = compare_blades(args.blade1, args.blade2)
        print(f"\n=== {data['blade1']['name'].upper()} vs {data['blade2']['name'].upper()} ===")

        print(f"\n{data['blade1']['name']}:")
        print(f"  Score: {data['blade1']['score']} | Uses: {data['blade1']['uses']}")
        print(f"  1st: {data['blade1']['first']} | 2nd: {data['blade1']['second']} | 3rd: {data['blade1']['third']}")
        print(f"  Win Rate: {data['blade1']['win_rate']:.1%}")

        print(f"\n{data['blade2']['name']}:")
        print(f"  Score: {data['blade2']['score']} | Uses: {data['blade2']['uses']}")
        print(f"  1st: {data['blade2']['first']} | 2nd: {data['blade2']['second']} | 3rd: {data['blade2']['third']}")
        print(f"  Win Rate: {data['blade2']['win_rate']:.1%}")

        h2h = data['head_to_head']
        print(f"\nHead-to-Head ({h2h['common_tournaments']} tournaments):")
        print(f"  {data['blade1']['name']} placed higher: {h2h['blade1_placed_higher']}")
        print(f"  {data['blade2']['name']} placed higher: {h2h['blade2_placed_higher']}")
        print(f"  Ties: {h2h['ties']}")

    elif args.command == 'meta':
        data = meta_snapshot(args.days)
        print(f"\n=== META SNAPSHOT (Last {data['period_days']} days) ===")
        print(f"Tournaments: {data['tournaments']}")

        print("\nTop Blades:")
        for b in data['top_blades'][:5]:
            print(f"  {b['blade']}: {b['uses']} uses, {b['wins']} wins")

        print("\nTop Combos:")
        for c in data['top_combos'][:5]:
            print(f"  {c['combo']}: {c['uses']} uses, {c['wins']} wins")

    else:
        parser.print_help()


if __name__ == "__main__":
    cli()
