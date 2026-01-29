/**
 * WBO Scraper Bookmarklet - Homelab Edition
 *
 * This script runs in your browser while on the WBO website.
 * It scrapes all pages and uploads directly to your homelab BeybladeX instance.
 *
 * USAGE:
 * 1. Navigate to: https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX
 * 2. Click the bookmarklet
 * 3. Wait for completion alert (~30-60 seconds)
 * 4. Your homelab database will automatically update
 *
 * CREATING THE BOOKMARKLET:
 * 1. In Firefox, right-click the bookmarks toolbar → "Add Bookmark..."
 * 2. Name: "Scrape WBO"
 * 3. URL: Paste the minified code below (starting with "javascript:")
 */

// ============================================================================
// CONFIGURATION
// ============================================================================
const SERVER_URL = "https://beybladexdatabase.thelightlab.net/api";

// ============================================================================
// FULL SCRIPT (for reference/debugging)
// ============================================================================
(async () => {
  const baseUrl = "https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX";

  console.log("🔄 Starting WBO scrape...");

  // Fetch first page and determine total page count
  const firstPage = await (await fetch(baseUrl)).text();
  const pageMatches = [...firstPage.matchAll(/page=(\d+)/g)].map(m => parseInt(m[1]));
  const maxPage = Math.max(...pageMatches);

  console.log(`📄 Found ${maxPage} pages to scrape`);

  // Scrape all pages
  const allHtml = { 1: firstPage };

  for (let i = 2; i <= maxPage; i++) {
    const progress = ((i / maxPage) * 100).toFixed(0);
    console.log(`📥 Fetching page ${i}/${maxPage} (${progress}%)`);

    const url = `${baseUrl}?page=${i}`;
    const response = await fetch(url);
    allHtml[i] = await response.text();

    // Rate limit: 300ms between requests
    await new Promise(resolve => setTimeout(resolve, 300));
  }

  console.log("✅ Scraping complete! Uploading to homelab...");

  // Upload to homelab server
  try {
    const response = await fetch(`${SERVER_URL}/upload/wbo`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(allHtml)
    });

    if (response.ok) {
      const result = await response.json();
      console.log("🚀 Upload successful!", result);
      alert(`Done! Uploaded ${maxPage} pages.\n\n${result.message}`);
    } else {
      const error = await response.json();
      console.error("❌ Upload failed:", error);
      alert(`Upload failed: ${error.error}\n\nCheck console for details.`);
    }
  } catch (e) {
    console.error("❌ Connection failed:", e);
    alert(`Connection failed: ${e.message}\n\nMake sure your homelab server is running.`);
  }
})();

// ============================================================================
// MINIFIED BOOKMARKLET - COPY THIS ENTIRE LINE:
// ============================================================================
// javascript:(async()=>{const S="https://beybladexdatabase.thelightlab.net/api",B="https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX";console.log("Starting WBO scrape...");const f=await(await fetch(B)).text(),M=Math.max(...[...f.matchAll(/page=(\d+)/g)].map(m=>+m[1]));console.log(`Found ${M} pages`);const H={1:f};for(let i=2;i<=M;i++){console.log(`Page ${i}/${M}`);H[i]=await(await fetch(`${B}?page=${i}`)).text();await new Promise(r=>setTimeout(r,300))}console.log("Uploading...");try{const r=await fetch(`${S}/upload/wbo`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(H)});if(r.ok){const j=await r.json();alert(`Done! ${M} pages uploaded.\n\n${j.message}`)}else{alert("Upload failed: "+(await r.json()).error)}}catch(e){alert("Connection failed: "+e.message)}})();
