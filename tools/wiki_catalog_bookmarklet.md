# Wiki Catalog Bookmarklet

Same idea as the WBO scraping bookmarklet — runs in your browser (residential
IP, not Cloudflare-blocked), fetches the Fandom wiki Category pages, POSTs
the HTML bundle to the prod API, the server parses it in-memory and refreshes
`parts_catalog`.

**When to run it**: any time Takara Tomy releases new Beyblade X parts and
the wiki gets updated. Maybe a few times a year. No schedule, you just run
it on demand.

## Install

1. Open your browser bookmarks bar.
2. Right-click → Add bookmark.
3. **Name**: `Beyblade Wiki → BeybladeX`
4. **URL**: paste the entire one-liner below.
5. Save.

## Run (same UX as the WBO bookmarklet)

1. Click the bookmark **from anywhere**. If you're not already on
   `beyblade.fandom.com`, it opens the wiki in a new tab and exits.
2. Switch to the new tab and **click the bookmark again**. A small green
   status overlay appears in the top-right.
3. The script fetches 9 wiki pages (7 Category pages + 2 list pages, ~2.5
   MB total) directly from `beyblade.fandom.com` using same-origin
   `fetch()` calls (no CORS issues).
4. POSTs the bundle to `https://beybladex-database.thelightlab.net/api/upload/wiki-catalog`.
5. Server parses everything, refreshes the catalog table, runs drift
   detection + the smart drift resolver, copies the DB to dist.
6. Alert pops up with the catalog summary.

Total wall time: ~10-30 seconds depending on your network.

> **Why two clicks**: cross-origin `fetch()` against `beyblade.fandom.com`
> from any other site (e.g. your bank, Twitter, etc.) gets blocked by CORS
> because Fandom doesn't whitelist arbitrary origins. Being *on* the wiki
> makes the fetches same-origin and they sail through. The WBO bookmarklet
> does the same thing for the same reason.

## Bookmarklet (one-liner — paste into the bookmark URL field)

```javascript
javascript:(async()=>{const S="https://beybladex-database.thelightlab.net/api/upload/wiki-catalog",B="https://beyblade.fandom.com/wiki/Category:Lock_Chips",U=["https://beyblade.fandom.com/wiki/Category:Lock_Chips","https://beyblade.fandom.com/wiki/Category:Main_Blades","https://beyblade.fandom.com/wiki/Category:Over_Blades","https://beyblade.fandom.com/wiki/Category:Assist_Blades","https://beyblade.fandom.com/wiki/Category:Metal_Blades","https://beyblade.fandom.com/wiki/Category:Bits","https://beyblade.fandom.com/wiki/Category:Ratchets","https://beyblade.fandom.com/wiki/List_of_Basic_Line_parts","https://beyblade.fandom.com/wiki/List_of_Unique_Line_parts"];if(!location.hostname.includes("beyblade.fandom.com")){window.open(B);return}const d=document.createElement("div");d.style.cssText="position:fixed;top:10px;right:10px;background:#222;color:#0f0;padding:15px;border-radius:8px;font-family:monospace;font-size:14px;z-index:999999;min-width:280px;white-space:pre-line;";document.body.appendChild(d);const log=m=>{d.textContent=m;console.log(m)};log("Starting wiki catalog refresh...");try{const pages={};for(let i=0;i<U.length;i++){const u=U[i];log("Fetching "+(i+1)+"/"+U.length+"\n"+u.split("/wiki/")[1]);const r=await fetch(u);if(!r.ok){throw new Error("Fetch "+u+" → "+r.status)}pages[u]=await r.text();await new Promise(r=>setTimeout(r,200))}log("Uploading bundle to BeybladeX server...");const resp=await fetch(S,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({pages})});const j=await resp.json();if(!resp.ok){throw new Error(j.error||resp.statusText)}log("Done!\n"+j.message);alert("Wiki catalog refreshed!\n\n"+j.message+"\n\nCounts: "+JSON.stringify(j.catalog_counts,null,2))}catch(e){log("Error: "+e.message);alert("Failed: "+e.message)}})();
```

## Readable source (for editing — re-minify before pasting into a bookmark)

```javascript
javascript:(async () => {
  const SERVER = "https://beybladex-database.thelightlab.net/api/upload/wiki-catalog";
  const BOOTSTRAP = "https://beyblade.fandom.com/wiki/Category:Lock_Chips";
  const URLS = [
    "https://beyblade.fandom.com/wiki/Category:Lock_Chips",
    "https://beyblade.fandom.com/wiki/Category:Main_Blades",
    "https://beyblade.fandom.com/wiki/Category:Over_Blades",
    "https://beyblade.fandom.com/wiki/Category:Assist_Blades",
    "https://beyblade.fandom.com/wiki/Category:Metal_Blades",
    "https://beyblade.fandom.com/wiki/Category:Bits",
    "https://beyblade.fandom.com/wiki/Category:Ratchets",
    "https://beyblade.fandom.com/wiki/List_of_Basic_Line_parts",
    "https://beyblade.fandom.com/wiki/List_of_Unique_Line_parts",
  ];

  // First click: not on the wiki yet → open it and exit. Click again on the
  // new tab to actually run the scrape. Same UX as the WBO bookmarklet.
  if (!location.hostname.includes("beyblade.fandom.com")) {
    window.open(BOOTSTRAP);
    return;
  }

  const overlay = document.createElement("div");
  overlay.style.cssText =
    "position:fixed;top:10px;right:10px;background:#222;color:#0f0;" +
    "padding:15px;border-radius:8px;font-family:monospace;font-size:14px;" +
    "z-index:999999;min-width:280px;white-space:pre-line;";
  document.body.appendChild(overlay);
  const log = (msg) => { overlay.textContent = msg; console.log(msg); };

  log("Starting wiki catalog refresh...");
  try {
    const pages = {};
    for (let i = 0; i < URLS.length; i++) {
      const u = URLS[i];
      log(`Fetching ${i + 1}/${URLS.length}\n${u.split("/wiki/")[1]}`);
      // Same-origin fetch — no `credentials: omit`. The browser will send
      // any wiki cookies (harmless) and CORS isn't a factor.
      const r = await fetch(u);
      if (!r.ok) throw new Error(`Fetch ${u} → ${r.status}`);
      pages[u] = await r.text();
      await new Promise((res) => setTimeout(res, 200));  // be polite
    }

    log("Uploading bundle to BeybladeX server...");
    const resp = await fetch(SERVER, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pages }),
    });
    const j = await resp.json();
    if (!resp.ok) throw new Error(j.error || resp.statusText);

    log("Done!\n" + j.message);
    alert(
      "Wiki catalog refreshed!\n\n" +
      j.message + "\n\n" +
      "Counts: " + JSON.stringify(j.catalog_counts, null, 2)
    );
  } catch (e) {
    log("Error: " + e.message);
    alert("Failed: " + e.message);
  }
})();
```

## Notes

- The first click opens `Category:Lock_Chips` on the wiki in a new tab if
  you're not already there. The second click (on the wiki tab) actually
  runs the scrape. Same flow as the WBO bookmarklet.
- The `~200 ms` delay between fetches keeps us well under any rate limits.
- Total payload is ~2.5 MB → JSON. The API server parses it in one
  synchronous request and responds within a few seconds.
- After the run, check `https://beybladex-database.thelightlab.net/admin/new-parts`
  to see anything the smart drift resolver flagged for review.
