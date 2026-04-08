"""
Parser smoke tests for the wiki-driven catalog refactor.

Run with: python3 scripts/test_parser.py

These cover:
- The new Eva lock chip and Vertical/Assault assist (added by the wiki, picked
  up automatically via the catalog with no code edits)
- Over blade parsing (Break/Flow/Guard) — the original missing feature
- Standard CX combo parsing
- BX/UX standalone blades

No assertion framework — fails loudly with assert errors so the failing case
is obvious in CI logs.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from scraper import parse_combo, split_blade_over_assist
from db import parse_cx_blade, is_incomplete_cx_blade, is_invalid_two_main_blades
from catalog import PartsCatalog


def assert_combo(combo_str, **expected):
    actual = parse_combo(combo_str)
    assert actual is not None, f"parse failed: {combo_str!r}"
    for field, want in expected.items():
        got = getattr(actual, field)
        assert got == want, f"{combo_str!r} {field}: expected {want!r}, got {got!r}"
    print(f"  OK: {combo_str}")


def main():
    cat = PartsCatalog.get()

    # Sanity-check the catalog itself before testing the parser
    assert "Eva" in cat.lock_chips, "Eva lock chip missing from catalog — run fandom scrape"
    assert "Vertical" in cat.assists, "Vertical assist missing — run fandom scrape"
    assert "Break" in cat.over_blades
    assert "Flow" in cat.over_blades
    assert "Guard" in cat.over_blades
    assert "Hornet" not in cat.main_blades, "Hornet should be a lock chip, not a main blade"
    print(f"Catalog OK: {len(cat.lock_chips)} lock chips, {len(cat.main_blades)} mains, "
          f"{len(cat.over_blades)} over blades, {len(cat.assists)} assists, "
          f"{len(cat.bits)} bits, {len(cat.ratchets)} ratchets")
    print()

    print("=== CX combos ===")
    assert_combo("Pegasus Blast 3-60F",
                 blade="Blast", lock_chip="Pegasus", ratchet="3-60", bit="Flat",
                 assist=None, over_blade=None)

    assert_combo("Pegasus Blast Wheel 3-60 Low Flat",
                 blade="Blast", lock_chip="Pegasus", assist="Wheel",
                 ratchet="3-60", bit="Low Flat", over_blade=None)

    print()
    print("=== NEW catalog entries (Eva, Vertical, Assault) ===")
    assert_combo("Eva Blast 4-60 Ball",
                 blade="Blast", lock_chip="Eva", bit="Ball")

    assert_combo("Eva Blast Vertical 4-60 Flat",
                 blade="Blast", lock_chip="Eva", assist="Vertical")

    print()
    print("=== Over blade parsing (the original missing feature) ===")
    assert_combo("Pegasus Blast Guard Wheel 3-60 Low Flat",
                 blade="Blast", lock_chip="Pegasus",
                 over_blade="Guard", assist="Wheel",
                 ratchet="3-60", bit="Low Flat")

    assert_combo("Pegasus Blast Break Slash 3-60F",
                 blade="Blast", lock_chip="Pegasus",
                 over_blade="Break", assist="Slash",
                 ratchet="3-60", bit="Flat")

    assert_combo("Eva Blast Flow 4-60 Ball",
                 blade="Blast", lock_chip="Eva",
                 over_blade="Flow", assist=None,
                 ratchet="4-60", bit="Ball")

    print()
    print("=== BX/UX standalone blades (no over blade, no assist) ===")
    assert_combo("Cobalt Dragoon 9-60 Rush",
                 blade="Cobalt Dragoon", lock_chip=None,
                 over_blade=None, assist=None)

    assert_combo("Wizard Rod 1-60 Ball",
                 blade="Wizard Rod", lock_chip=None,
                 over_blade=None, assist=None)

    print()
    print("=== Validation helpers ===")
    assert parse_cx_blade("Pegasus Blast") == ("Pegasus", "Blast")
    assert parse_cx_blade("Eva Blast") == ("Eva", "Blast")
    assert parse_cx_blade("PegasusBlast") == ("Pegasus", "Blast")
    assert parse_cx_blade("Cobalt Dragoon") == (None, "Cobalt Dragoon")
    assert is_incomplete_cx_blade("Blast", None) is True
    assert is_incomplete_cx_blade("Pegasus Blast", "Pegasus") is False
    assert is_invalid_two_main_blades("Might Blast") is True
    assert is_invalid_two_main_blades("Pegasus Blast") is False
    print("  OK: parse_cx_blade, is_incomplete_cx_blade, is_invalid_two_main_blades")

    print()
    print("=== split_blade_over_assist edge cases ===")
    cases = [
        ("Pegasus Blast", ("Pegasus Blast", None, None)),
        ("Pegasus Blast Wheel", ("Pegasus Blast", None, "Wheel")),
        ("Pegasus Blast Guard", ("Pegasus Blast", "Guard", None)),
        ("Pegasus Blast Guard Wheel", ("Pegasus Blast", "Guard", "Wheel")),
        ("Eva Blast Break Vertical", ("Eva Blast", "Break", "Vertical")),
    ]
    for inp, want in cases:
        got = split_blade_over_assist(inp)
        assert got == want, f"{inp!r}: expected {want!r}, got {got!r}"
        print(f"  OK: {inp}")

    print()
    print("All tests passed.")


if __name__ == "__main__":
    main()
