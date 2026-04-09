#!/usr/bin/env python3
"""
Ingest the Beyblade X Fandom wiki into a RAGFlow dataset.

For every part page (lock chips, main blades, over blades, assist blades,
metal blades, bits, ratchets, BX/UX standalone blades) discovered via the
existing fandom scraper, this script:

  1. Fetches the page via cloudscraper (CF bypass — same code path as the
     catalog scraper at scripts/scrapers/fandom.py)
  2. Extracts the title, infobox key-values, and article body paragraphs
  3. Converts to clean markdown
  4. Uploads to the configured RAGFlow dataset
  5. Triggers parsing/embedding so the chunks become searchable

Idempotent: documents that already exist in the dataset (matched by
filename) are skipped. To force a re-upload, run with --force.

Run from your dev box (NOT the prod box, which is Cloudflare-blocked):

    export RAGFLOW_BASE_URL=http://192.168.88.38
    export RAGFLOW_API_KEY=ragflow-...
    export RAGFLOW_DATASET_ID=e68a86ac344311f1ba10124e782f30a9
    python scripts/ragflow_ingest.py

Add --dry-run to print discovered URLs and the markdown for one sample
without actually uploading.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Make scripts/ + scripts/scrapers/ importable when run directly
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(SCRIPTS_DIR / "scrapers"))

from bs4 import BeautifulSoup, NavigableString  # noqa: E402

from fandom import (  # noqa: E402
    CATEGORY_PAGES,
    DISCOVERY_PAGES,
    BX_FALLBACK_PARTS,
    _fetch_page,
    _discover_from_category,
    _discover_parts_from_list_page,
)
from ragflow import RAGFlowClient  # noqa: E402


RATE_LIMIT = 1.0  # seconds between wiki fetches — be polite to the wiki + cloudscraper
UPLOAD_BATCH = 25  # how many docs to upload before triggering parse
DISCOVERY_CACHE = SCRIPTS_DIR.parent / "data" / "ragflow_ingest_urls.json"


# ----------------------------------------------------------------------
# HTML → markdown extraction
# ----------------------------------------------------------------------

def _strip_unwanted(soup_el):
    """Remove navigation, edit links, references, gallery, etc. from a
    BS4 element in place."""
    selectors_to_remove = [
        "div.toc",
        "div.references",
        "ol.references",
        "div.reference",
        "div.printfooter",
        "div#catlinks",
        "table.navbox",
        "table.metadata",
        "div.thumb",
        "div.gallery",
        "div.gallerybox",
        "span.mw-editsection",
        "span.mw-cite-backlink",
        "sup.reference",
        "div.notice",
        "div.hatnote",
    ]
    for sel in selectors_to_remove:
        for el in soup_el.select(sel):
            el.decompose()


def _infobox_to_markdown(infobox) -> str:
    """Extract key-value pairs from a Fandom portable-infobox.

    Output format:
        ## Stats
        - **Attack**: 5
        - **Defense**: 3
        - **Weight**: 6.4 g
    """
    lines: list[str] = []
    seen_section = False

    # 1. pi-data items: simple key/value (Weight, Spin Direction, etc.)
    for item in infobox.find_all("div", class_="pi-data"):
        label_el = item.find("h3", class_="pi-data-label")
        value_el = item.find("div", class_="pi-data-value")
        if not label_el or not value_el:
            continue
        label = label_el.get_text(" ", strip=True)
        value = value_el.get_text(" ", strip=True)
        if label and value:
            if not seen_section:
                lines.append("## Properties")
                seen_section = True
            lines.append(f"- **{label}**: {value}")

    # 2. pi-horizontal-group tables (the stat columns: Attack/Defense/Stamina, Dash/Burst Resistance)
    for table in infobox.find_all("table", class_="pi-horizontal-group"):
        thead = table.find("thead")
        tbody = table.find("tbody")
        if not thead or not tbody:
            continue
        headers = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
        values = [td.get_text(" ", strip=True) for td in tbody.find_all("td")]
        if not headers or not values:
            continue
        if not lines or not lines[-1].startswith("##"):
            lines.append("")
            lines.append("## Stats")
        for h, v in zip(headers, values):
            if h and v:
                lines.append(f"- **{h}**: {v}")

    if lines:
        lines.append("")
    return "\n".join(lines)


def _body_to_markdown(content_el, max_chars: int = 4000) -> str:
    """Convert the wiki article body to a simple markdown representation.

    Keeps headings (h2-h4), paragraphs, and lists. Drops everything else.
    Truncates to max_chars to keep RAGFlow chunks lean and focused on the
    most semantically dense content (top of the article).
    """
    lines: list[str] = []
    char_count = 0

    for el in content_el.children:
        if isinstance(el, NavigableString):
            continue
        tag = getattr(el, "name", None)
        if not tag:
            continue

        if tag in ("h2", "h3", "h4"):
            text = el.get_text(" ", strip=True)
            if not text or text.lower() in {"references", "navigation", "see also", "gallery", "trivia"}:
                # Skip and stop processing — these sections are noisy
                if text.lower() in {"references", "see also", "gallery"}:
                    break
                continue
            level = "##" if tag == "h2" else ("###" if tag == "h3" else "####")
            lines.append("")
            lines.append(f"{level} {text}")
            char_count += len(text)

        elif tag == "p":
            text = el.get_text(" ", strip=True)
            if text:
                lines.append("")
                lines.append(text)
                char_count += len(text)

        elif tag in ("ul", "ol"):
            for li in el.find_all("li", recursive=False):
                text = li.get_text(" ", strip=True)
                if text:
                    lines.append(f"- {text}")
                    char_count += len(text)

        if char_count >= max_chars:
            break

    return "\n".join(lines).strip() + "\n"


def page_to_markdown(soup: BeautifulSoup, part_name: str, part_type: str, wiki_url: str) -> str:
    """Convert a Fandom wiki page (BeautifulSoup) to clean markdown."""
    # Find the article body
    content = soup.find("div", class_="mw-parser-output") or soup
    _strip_unwanted(content)

    # Title
    title = part_name
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(" ", strip=True) or title

    parts = [
        f"# {title}",
        "",
        f"**Part type**: {part_type.replace('_', ' ').title()}",
        f"**Wiki URL**: {wiki_url}",
        "",
    ]

    infobox = soup.find("aside", class_="portable-infobox")
    if infobox:
        parts.append(_infobox_to_markdown(infobox))

    parts.append(_body_to_markdown(content))
    return "\n".join(parts)


# ----------------------------------------------------------------------
# Discovery — every wiki page we want to ingest
# ----------------------------------------------------------------------

def discover_all_pages(verbose: bool = False, use_cache: bool = True) -> list[dict]:
    """Return [{name, part_type, system, wiki_url}, ...] for every part page
    on the Beyblade X wiki we want to ingest into RAGFlow.

    Reuses the existing fandom scraper's category + list-page discovery so
    we don't reinvent it. Caches the result to data/ragflow_ingest_urls.json
    so partial Cloudflare blocks on subsequent runs don't shrink our work
    set — pass use_cache=False to force a full re-discovery.
    """
    import json

    if use_cache and DISCOVERY_CACHE.exists():
        try:
            with open(DISCOVERY_CACHE) as f:
                cached = json.load(f)
            if isinstance(cached, list) and cached:
                if verbose:
                    print(f"  Using cached {len(cached)} URLs from {DISCOVERY_CACHE}")
                return cached
        except Exception as e:
            if verbose:
                print(f"  Cache read failed ({e}); re-discovering")

    seen_urls: set[str] = set()
    pages: list[dict] = []

    # 1. Category pages — authoritative source for CX parts
    for part_type, (cat_url, system) in CATEGORY_PAGES.items():
        if verbose:
            print(f"  Discovering from {cat_url}")
        for p in _discover_from_category(cat_url, part_type, system):
            url = p["wiki_url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)
            pages.append({
                "name": p["name"],
                "part_type": part_type,
                "system": system,
                "wiki_url": url,
            })
        time.sleep(RATE_LIMIT)

    # 2. BX/UX standalone blades from list pages
    for system, list_url in [("BX", DISCOVERY_PAGES["BX"]), ("UX", DISCOVERY_PAGES["UX"])]:
        if verbose:
            print(f"  Discovering from {list_url}")
        items = _discover_parts_from_list_page(list_url, system)
        if not items and system == "BX":
            if verbose:
                print(f"    BX list page blocked, using fallback")
            items = list(BX_FALLBACK_PARTS)
        for it in items:
            if it.get("part_type") != "blade":
                continue
            url = it.get("url") or it.get("wiki_url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            pages.append({
                "name": it["name"],
                "part_type": "blade",
                "system": system,
                "wiki_url": url,
            })
        time.sleep(RATE_LIMIT)

    # Cache to disk so subsequent runs survive partial Cloudflare blocks.
    # Only write the cache if we got a "good" discovery (at least 200 URLs;
    # full set is ~290, a partial Cloudflare-blocked run is ~150).
    try:
        DISCOVERY_CACHE.parent.mkdir(parents=True, exist_ok=True)
        if len(pages) >= 200 or not DISCOVERY_CACHE.exists():
            import json
            with open(DISCOVERY_CACHE, "w") as f:
                json.dump(pages, f, indent=2)
            if verbose:
                print(f"  Wrote discovery cache: {len(pages)} URLs → {DISCOVERY_CACHE}")
    except Exception as e:
        if verbose:
            print(f"  Cache write failed: {e}")

    return pages


# ----------------------------------------------------------------------
# Filename convention — round-trippable to canonical part name
# ----------------------------------------------------------------------

def filename_for(page: dict) -> str:
    """Build the upload filename. The leading slug is round-trippable to a
    canonical part name when we get retrieval results back."""
    slug = page["wiki_url"].rsplit("/wiki/", 1)[-1]
    # RAGFlow strips path-unsafe chars; sanitize ourselves to be deterministic
    slug = slug.replace("/", "_")
    return f"{slug}.md"


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print discovered URLs and one sample markdown without uploading")
    ap.add_argument("--force", action="store_true",
                    help="Re-upload pages even if their filename already exists in the dataset")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after this many uploads (for testing)")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--rediscover", action="store_true",
                    help="Ignore the cached URL list at data/ragflow_ingest_urls.json and re-scrape the wiki")
    args = ap.parse_args()

    client = RAGFlowClient()
    if not client.is_configured():
        print("ERROR: RAGFLOW_BASE_URL and RAGFLOW_API_KEY env vars not set")
        sys.exit(1)
    if not client.default_dataset_id:
        print("ERROR: RAGFLOW_DATASET_ID env var not set")
        sys.exit(1)

    print(f"RAGFlow target: {client.base_url}  dataset {client.default_dataset_id}")
    print()

    print("Phase 1: discovering wiki pages...")
    pages = discover_all_pages(verbose=args.verbose, use_cache=not args.rediscover)
    print(f"  Discovered {len(pages)} pages")
    print()

    if args.dry_run:
        print("=== DRY RUN — first 5 page URLs ===")
        for p in pages[:5]:
            print(f"  [{p['part_type']:11}] {p['name']:30} {p['wiki_url']}")
        print()
        if pages:
            print("=== SAMPLE MARKDOWN (first page) ===")
            soup = _fetch_page(pages[0]["wiki_url"])
            if soup:
                md = page_to_markdown(soup, pages[0]["name"], pages[0]["part_type"], pages[0]["wiki_url"])
                print(md)
        return

    print("Phase 2: listing existing dataset documents (for idempotency)...")
    existing_docs = client.list_documents()
    existing_names = {d.get("name") for d in existing_docs}
    print(f"  {len(existing_names)} docs already in dataset")
    print()

    print("Phase 3: fetching, converting, uploading...")
    uploaded_ids: list[str] = []
    skipped = 0
    failed = 0
    parsed_so_far = 0

    for i, page in enumerate(pages, start=1):
        if args.limit and (len(uploaded_ids) + parsed_so_far) >= args.limit:
            print(f"  Hit --limit ({args.limit}), stopping")
            break

        fname = filename_for(page)
        if not args.force and fname in existing_names:
            skipped += 1
            continue

        if args.verbose:
            print(f"  [{i:3}/{len(pages)}] {fname}")

        soup = _fetch_page(page["wiki_url"])
        if soup is None:
            print(f"    fetch failed: {page['wiki_url']}")
            failed += 1
            time.sleep(RATE_LIMIT)
            continue

        try:
            md = page_to_markdown(soup, page["name"], page["part_type"], page["wiki_url"])
        except Exception as e:
            print(f"    convert failed for {fname}: {e}")
            failed += 1
            time.sleep(RATE_LIMIT)
            continue

        doc_id = client.upload_document(fname, md.encode("utf-8"))
        if doc_id:
            uploaded_ids.append(doc_id)
        else:
            failed += 1

        # Trigger parsing in batches so the embedding pipeline starts working
        # while we keep uploading.
        if len(uploaded_ids) >= UPLOAD_BATCH:
            if args.verbose:
                print(f"    Triggering parse for batch of {len(uploaded_ids)} docs...")
            client.parse_document(uploaded_ids)
            parsed_so_far += len(uploaded_ids)
            uploaded_ids = []

        time.sleep(RATE_LIMIT)

    # Parse the remaining tail
    if uploaded_ids:
        if args.verbose:
            print(f"  Triggering parse for final batch of {len(uploaded_ids)} docs...")
        client.parse_document(uploaded_ids)
        parsed_so_far += len(uploaded_ids)

    print()
    print(f"Done: {parsed_so_far} uploaded, {skipped} skipped, {failed} failed")


if __name__ == "__main__":
    main()
