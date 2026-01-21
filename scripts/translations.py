"""
Japanese to English translations for BeybladeX part names.

This module provides comprehensive translation dictionaries for converting
Japanese (katakana) part names to their English equivalents.
"""

# =============================================================================
# Blade Translations (カタカナ → English)
# =============================================================================
# Main blades including BX, UX, and CX series

BLADE_TRANSLATIONS: dict[str, str] = {
    # BX Series (Basic Line)
    "ドランソード": "Dran Sword",
    "ヘルズサイズ": "Hells Scythe",
    "ウィザードアロー": "Wizard Arrow",
    "ナイトシールド": "Knight Shield",
    "ナイトランス": "Knight Lance",
    "シャークエッジ": "Shark Edge",
    "バイパーテイル": "Viper Tail",
    "ドランダガー": "Dran Dagger",
    "レオンクロー": "Leon Claw",
    "ライノホーン": "Rhino Horn",
    "フェニックスウイング": "Phoenix Wing",
    "ヘルズチェイン": "Hells Chain",
    "ユニコーンスティング": "Unicorn Sting",
    "ブラックシェル": "Black Shell",
    "ティラノビート": "Tyranno Beat",
    "ヴァイスタイガー": "Weiss Tiger",
    "コバルトドラグーン": "Cobalt Dragoon",
    "コバルトドレイク": "Cobalt Drake",
    "クリムゾンガルーダ": "Crimson Garuda",
    "タロンプテラ": "Talon Ptera",
    "ロアータイラノ": "Roar Tyranno",
    "スフィンクスカウル": "Sphinx Cowl",
    "ワイバーンゲイル": "Wyvern Gale",
    "シェルタードレイク": "Shelter Drake",
    "トリケラプレス": "Tricera Press",
    "サムライキャリバー": "Samurai Calibur",
    "ベアスクラッチ": "Bear Scratch",
    "ゼノエクスカリバー": "Xeno Xcalibur",
    "チェインインセンディオ": "Chain Incendio",
    "サイズインセンディオ": "Scythe Incendio",
    "スティールサムライ": "Steel Samurai",
    "オプティマスプライマル": "Optimus Primal",
    "バイトクロック": "Bite Croc",
    "ナイフシノビ": "Knife Shinobi",
    "ヴェノム": "Venom",
    "キールシャーク": "Keel Shark",
    "ホエールウェーブ": "Whale Wave",
    "ギルシャーク": "Gill Shark",
    "ドライガースラッシュ": "Driger Slash",
    "ドラグーンストーム": "Dragoon Storm",

    # UX Series (Unique Line)
    "ドランバスター": "Dran Buster",
    "ヘルズハンマー": "Hells Hammer",
    "ウィザードロッド": "Wizard Rod",
    "ソアーフェニックス": "Soar Phoenix",
    "レオンクレスト": "Leon Crest",
    "ナイトメイル": "Knight Mail",
    "シルバーウルフ": "Silver Wolf",
    "サムライセイバー": "Samurai Saber",
    "フェニックスフェザー": "Phoenix Feather",
    "インパクトドレイク": "Impact Drake",
    "タスクマンモス": "Tusk Mammoth",
    "フェニックスラダー": "Phoenix Rudder",
    "ゴーストサークル": "Ghost Circle",
    "ゴーレムロック": "Golem Rock",
    "スコルピオスピア": "Scorpio Spear",
    "シノビシャドー": "Shinobi Shadow",
    "クロックミラージュ": "Clock Mirage",
    "メテオドラグーン": "Meteor Dragoon",
    "マミーカース": "Mummy Curse",
    "ドランザースパイラル": "Dranzer Spiral",
    "シャークスケイル": "Shark Scale",
    "ホバーワイバーン": "Hover Wyvern",
    "エアロペガサス": "Aero Pegasus",
    "ワンドウィザード": "Wand Wizard",

    # CX Series (Custom Line) - Full names with lock chips
    "ドランブレイブ": "Dran Brave",
    "エンペラーブレイブ": "Emperor Brave",
    "ウィザードアーク": "Wizard Arc",
    "ペルセウスダーク": "Perseus Dark",
    "ヘルズリーパー": "Hells Reaper",
    "フォックスブラッシュ": "Fox Brush",
    "ペガサスブラスト": "Pegasus Blast",
    "ケルベロスブラスト": "Cerberus Blast",
    "ヘルズブラスト": "Hells Blast",
    "ソルエクリプス": "Sol Eclipse",
    "ウルフハント": "Wolf Hunt",
    "エンペラーマイト": "Emperor Might",
    "フェニックスフレア": "Phoenix Flare",
    "ヴァルキリーボルト": "Valkyrie Volt",
    "ワルキューレボルト": "Valkyrie Volt",
    "ワルキューレボルトA": "Valkyrie Volt A",
    "ワルキューレブラスト": "Valkyrie Blast",
    "ワルキューレブラストS": "Valkyrie Blast S",
    "ワルキューレブラストW": "Valkyrie Blast W",

    # Alternate spellings
    "スコーピオスピア": "Scorpio Spear",
    "ワイバーンホバー": "Hover Wyvern",  # Same as ホバーワイバーン

    # CX Main Blades (without lock chip prefix)
    "ブレイブ": "Brave",
    "アーク": "Arc",
    "ダーク": "Dark",
    "リーパー": "Reaper",
    "ブラッシュ": "Brush",
    "ブラスト": "Blast",
    "エクリプス": "Eclipse",
    "ハント": "Hunt",
    "マイト": "Might",
    "フレア": "Flare",
    "ボルト": "Volt",
    "ストーム": "Storm",
    "エンペラー": "Emperor",

    # Additional variations and common alternate spellings
    "ペガサスブレスト": "Pegasus Blast",  # Alternate romanization
    "ウィザードロット": "Wizard Rod",      # Alternate romanization
    "サムライセーバー": "Samurai Saber",   # Alternate romanization

    # Additional blades found in Japanese tournament data
    "ワイバーンホバー": "Hover Wyvern",
    "ワルキューレブラスト": "Valkyrie Blast",
    "バルキューレボルト": "Valkyrie Volt",
    "ヴァルキリーボルト": "Valkyrie Volt",
    "セルベロスブラスト": "Cerberus Blast",
    "ケルベロスブラスト": "Cerberus Blast",
    "シノビシャドー": "Shinobi Shadow",
    "クロックミラージュ": "Clock Mirage",
    "ウルフハント": "Wolf Hunt",
    "フェニックスフレア": "Phoenix Flare",
    "エンペラーマイト": "Emperor Might",
    "ソルエクリプス": "Sol Eclipse",
    "フォックスブラッシュ": "Fox Brush",
    "ペルセウスダーク": "Perseus Dark",
    "ドランブレイブ": "Dran Brave",
    "エンペラーブレイブ": "Emperor Brave",
    "フェニックスラダー": "Phoenix Rudder",
    "マミーカース": "Mummy Curse",
    "スコルピオスピア": "Scorpio Spear",
    "ドランザースパイラル": "Dranzer Spiral",
}


# =============================================================================
# Ratchet Translations
# =============================================================================
# Ratchets follow X-XX format (e.g., 3-60, 4-80)
# Most are numeric but some have Japanese annotations

RATCHET_TRANSLATIONS: dict[str, str] = {
    # Standard ratchets are numeric and don't need translation
    # But capture any with Japanese annotations
    "1-60": "1-60",
    "2-60": "2-60",
    "3-60": "3-60",
    "4-60": "4-60",
    "5-60": "5-60",
    "6-60": "6-60",
    "7-60": "7-60",
    "9-60": "9-60",
    "1-70": "1-70",
    "3-70": "3-70",
    "4-70": "4-70",
    "5-70": "5-70",
    "9-70": "9-70",
    "1-80": "1-80",
    "3-80": "3-80",
    "4-80": "4-80",
    "5-80": "5-80",
    "9-80": "9-80",
}


# =============================================================================
# Bit Translations (カタカナ → English)
# =============================================================================
# Tips/drivers that go at the bottom of the Beyblade

BIT_TRANSLATIONS: dict[str, str] = {
    # Single letter bits (full katakana names)
    "フラット": "Flat",
    "ボール": "Ball",
    "ニードル": "Needle",
    "ポイント": "Point",
    "テーパー": "Taper",
    "スパイク": "Spike",
    "オーブ": "Orb",
    "ドット": "Dot",
    "アクセル": "Accel",
    "ラッシュ": "Rush",
    "ヘキサ": "Hexa",
    "サイクロン": "Cyclone",
    "ユナイト": "Unite",
    "レベル": "Level",
    "エレベート": "Elevate",
    "グライド": "Glide",
    "クエイク": "Quake",
    "キック": "Kick",
    "ヴァンガード": "Vanguard",

    # Two-letter/compound bits
    "ハイニードル": "High Needle",
    "ローフラット": "Low Flat",
    "ローラッシュ": "Low Rush",
    "ローニードル": "Low Needle",
    "ギアフラット": "GearFlat",
    "ギアボール": "GearBall",
    "ギアニードル": "GearNeedle",
    "ギアポイント": "GearPoint",
    "メタルニードル": "Metal Needle",
    "ハイテーパー": "High Taper",
    "ハイアクセル": "High Accel",
    "ディスクボール": "Disc Ball",
    "ハイソード": "High Sword",
    "スパイラルニードル": "Spiral Needle",
    "ブレーキ": "Brake",
    "バウンド": "Bound",

    # Abbreviation variants that might appear
    "F": "Flat",
    "B": "Ball",
    "N": "Needle",
    "P": "Point",
    "T": "Taper",
    "S": "Spike",
    "O": "Orb",
    "D": "Dot",
    "A": "Accel",
    "R": "Rush",
    "H": "Hexa",
    "C": "Cyclone",
    "U": "Unite",
    "L": "Level",
    "E": "Elevate",
    "G": "Glide",
    "Q": "Quake",
    "K": "Kick",
    "V": "Vanguard",
    "HN": "High Needle",
    "LF": "Low Flat",
    "LR": "Low Rush",
    "LN": "Low Needle",
    "GF": "GearFlat",
    "GB": "GearBall",
    "GN": "GearNeedle",
    "GP": "GearPoint",
    "MN": "Metal Needle",
    "HT": "High Taper",
    "HA": "High Accel",
    "DB": "Disc Ball",
    "HS": "High Sword",
    "SN": "Spiral Needle",
    "Br": "Brake",
    "Bd": "Bound",
    "Lv": "Level",
    "El": "Elevate",
    "Un": "Unite",
    "Gl": "Glide",
}


# =============================================================================
# Lock Chip Translations (for CX blades)
# =============================================================================
# Lock chips that combine with main blades in the CX series

LOCK_CHIP_TRANSLATIONS: dict[str, str] = {
    "ペガサス": "Pegasus",
    "ドラン": "Dran",
    "ウィザード": "Wizard",
    "ペルセウス": "Perseus",
    "ヘルズ": "Hells",
    "フォックス": "Fox",
    "ソル": "Sol",
    "ウルフ": "Wolf",
    "エンペラー": "Emperor",
    "フェニックス": "Phoenix",
    "ヴァルキリー": "Valkyrie",
    "ケルベロス": "Cerberus",
}


# =============================================================================
# Assist Blade Translations
# =============================================================================
# Assist blades that attach to CX main blades

ASSIST_TRANSLATIONS: dict[str, str] = {
    "ジャギー": "Jaggy",
    "スラッシュ": "Slash",
    "ホイール": "Wheel",
    "バンパー": "Bumper",
    "ヘビー": "Heavy",
    "アサルト": "Assault",
    "ラッシュ": "Rush",
    "ロー": "Low",

    # Compound assists
    "ラッシュアサルト": "Rush Assault",
    "ローオービット": "Low",
    "バンパースラッシュ": "Bumper Slash",
    "アッパーフラット": "Upper Flat",
    "ギアラッシュ": "Gear Rush",
    "ホイールバンパー": "Wheel Bumper",

    # Single letter abbreviations
    "W": "Wheel",
    "H": "Heavy",
    "S": "Slash",
    "J": "Jaggy",
}


# =============================================================================
# Tournament Name Translations
# =============================================================================
# Common tournament names and prefixes

TOURNAMENT_TRANSLATIONS: dict[str, str] = {
    # Tournament types
    "G1大会": "G1 Tournament",
    "日本選手権": "Japan Championship",
    "ワールドチャンピオンシップ": "World Championship",
    "アジアチャンピオンシップ": "Asia Championship",
    "エクストリームカップGP": "Extreme Cup GP",
    "ランダムブースターカップ": "Random Booster Cup",

    # Regional prefixes
    "東京": "Tokyo",
    "大阪": "Osaka",
    "名古屋": "Nagoya",
    "福岡": "Fukuoka",
    "札幌": "Sapporo",
    "仙台": "Sendai",
    "横浜": "Yokohama",
    "神戸": "Kobe",
    "京都": "Kyoto",
    "広島": "Hiroshima",

    # Other common terms
    "レギュラークラス": "Regular Class",
    "オープンクラス": "Open Class",
    "決勝": "Finals",
    "準決勝": "Semi-Finals",
    "予選": "Preliminaries",
}


# =============================================================================
# Placement Translations
# =============================================================================

PLACEMENT_TRANSLATIONS: dict[str, int] = {
    "1位": 1,
    "2位": 2,
    "3位": 3,
    "優勝": 1,
    "準優勝": 2,
    "3位入賞": 3,
}


# =============================================================================
# Utility Functions
# =============================================================================

def translate_blade(jp_name: str) -> str:
    """Translate a Japanese blade name to English."""
    # First check direct translation
    if jp_name in BLADE_TRANSLATIONS:
        return BLADE_TRANSLATIONS[jp_name]

    # Try removing spaces and checking again
    no_space = jp_name.replace(" ", "").replace("　", "")
    if no_space in BLADE_TRANSLATIONS:
        return BLADE_TRANSLATIONS[no_space]

    # If no translation found, return original
    return jp_name


def translate_bit(jp_name: str) -> str:
    """Translate a Japanese bit name to English."""
    if jp_name in BIT_TRANSLATIONS:
        return BIT_TRANSLATIONS[jp_name]
    return jp_name


def translate_lock_chip(jp_name: str) -> str:
    """Translate a Japanese lock chip name to English."""
    if jp_name in LOCK_CHIP_TRANSLATIONS:
        return LOCK_CHIP_TRANSLATIONS[jp_name]
    return jp_name


def translate_assist(jp_name: str) -> str:
    """Translate a Japanese assist blade name to English."""
    if jp_name in ASSIST_TRANSLATIONS:
        return ASSIST_TRANSLATIONS[jp_name]
    return jp_name


def translate_combo(jp_combo: str) -> tuple[str, str, str]:
    """
    Translate a full Japanese combo string to English parts.

    Args:
        jp_combo: Japanese combo string like "ドランソード 3-60F"

    Returns:
        Tuple of (blade, ratchet, bit) in English
    """
    import re

    # Try to match pattern: [Blade] [Ratchet][Bit]
    # Ratchet is X-XX format
    match = re.match(r'^(.+?)\s+(\d{1,2}-\d{2,3})([A-Za-zァ-ヶー]+)$', jp_combo.strip())
    if match:
        blade_jp = match.group(1).strip()
        ratchet = match.group(2)
        bit_jp = match.group(3).strip()

        blade_en = translate_blade(blade_jp)
        bit_en = translate_bit(bit_jp)

        return (blade_en, ratchet, bit_en)

    # If pattern doesn't match, return components as-is
    parts = jp_combo.strip().split()
    if len(parts) >= 3:
        return (translate_blade(parts[0]), parts[1], translate_bit(parts[2]))
    elif len(parts) == 2:
        # Might be blade + ratchet+bit combined
        return (translate_blade(parts[0]), parts[1], "")
    else:
        return (jp_combo, "", "")


def is_japanese(text: str) -> bool:
    """Check if text contains Japanese characters (hiragana, katakana, or kanji)."""
    import re
    # Match hiragana, katakana, or kanji
    return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', text))


def get_all_blade_translations() -> dict[str, str]:
    """Get the full blade translation dictionary."""
    return BLADE_TRANSLATIONS.copy()


def get_all_bit_translations() -> dict[str, str]:
    """Get the full bit translation dictionary."""
    return BIT_TRANSLATIONS.copy()


def add_blade_translation(jp_name: str, en_name: str) -> None:
    """Add a new blade translation (for runtime updates)."""
    BLADE_TRANSLATIONS[jp_name] = en_name


def add_bit_translation(jp_name: str, en_name: str) -> None:
    """Add a new bit translation (for runtime updates)."""
    BIT_TRANSLATIONS[jp_name] = en_name


if __name__ == "__main__":
    # Test translations
    test_cases = [
        "ドランソード",
        "ヘルズサイズ",
        "ペガサスブラスト",
        "フラット",
        "ハイニードル",
    ]

    print("Testing blade translations:")
    for jp in test_cases[:3]:
        print(f"  {jp} -> {translate_blade(jp)}")

    print("\nTesting bit translations:")
    for jp in test_cases[3:]:
        print(f"  {jp} -> {translate_bit(jp)}")

    print("\nTesting combo translation:")
    test_combo = "ドランソード 3-60F"
    result = translate_combo(test_combo)
    print(f"  {test_combo} -> {result}")

    print(f"\nTotal blade translations: {len(BLADE_TRANSLATIONS)}")
    print(f"Total bit translations: {len(BIT_TRANSLATIONS)}")
    print(f"Total lock chip translations: {len(LOCK_CHIP_TRANSLATIONS)}")
    print(f"Total assist translations: {len(ASSIST_TRANSLATIONS)}")
