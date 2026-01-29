/**
 * WBO Scraper Bookmarklet
 *
 * This script runs in your browser console while on the WBO website.
 * It scrapes all pages and uploads directly to GitHub, triggering a rebuild.
 *
 * SETUP:
 * 1. Create a GitHub Personal Access Token (Fine-grained):
 *    - Go to: GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens
 *    - Repository access: Select "BeybladeX-Database" only
 *    - Permissions: Contents → Read and write
 *    - Generate and copy the token
 *
 * 2. Replace YOUR_GITHUB_TOKEN and YOUR_USERNAME below
 *
 * 3. Create bookmarklet:
 *    - Copy the minified version at the bottom of this file
 *    - Create a new bookmark in your browser
 *    - Paste the minified code as the URL
 *
 * 4. Usage:
 *    - Navigate to: https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX
 *    - Click the bookmarklet
 *    - Wait for "Done!" alert (~30-60 seconds)
 *    - GitHub Actions will automatically rebuild the database
 */

// ============================================================================
// CONFIGURATION - Update these values!
// ============================================================================
const TOKEN = "YOUR_GITHUB_TOKEN";  // Your fine-grained PAT
const REPO = "YOUR_USERNAME/BeybladeX-Database";  // Your GitHub username/repo

// ============================================================================
// SCRIPT - Don't modify below unless you know what you're doing
// ============================================================================
(async () => {
  const PATH = "data/wbo_pages.json";
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

  console.log("✅ Scraping complete! Uploading to GitHub...");

  // Get current file SHA (required for updating existing files)
  let sha = null;
  try {
    const metaResponse = await fetch(`https://api.github.com/repos/${REPO}/contents/${PATH}`, {
      headers: { Authorization: `Bearer ${TOKEN}` }
    });
    if (metaResponse.ok) {
      const meta = await metaResponse.json();
      sha = meta.sha;
    }
  } catch (e) {
    console.log("📝 Creating new file (no existing file found)");
  }

  // Encode content as base64
  const jsonContent = JSON.stringify(allHtml);
  const base64Content = btoa(unescape(encodeURIComponent(jsonContent)));

  // Upload to GitHub
  const uploadResponse = await fetch(`https://api.github.com/repos/${REPO}/contents/${PATH}`, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      message: `Update WBO data (${maxPage} pages)`,
      content: base64Content,
      sha: sha
    })
  });

  if (uploadResponse.ok) {
    console.log("🚀 Upload successful! GitHub Actions will rebuild the database.");
    alert(`Done! Uploaded ${maxPage} pages to GitHub.\n\nGitHub Actions will now rebuild the database automatically.`);
  } else {
    const error = await uploadResponse.json();
    console.error("❌ Upload failed:", error);
    alert(`Upload failed: ${error.message}\n\nCheck console for details.`);
  }
})();

// ============================================================================
// MINIFIED BOOKMARKLET VERSION
// ============================================================================
// Replace YOUR_GITHUB_TOKEN and YOUR_USERNAME, then use as bookmarklet URL:
//
// javascript:(async()=>{const TOKEN="YOUR_GITHUB_TOKEN",REPO="YOUR_USERNAME/BeybladeX-Database",PATH="data/wbo_pages.json",baseUrl="https://worldbeyblade.org/Thread-Winning-Combinations-at-WBO-Organized-Events-Beyblade-X-BBX";console.log("Starting WBO scrape...");const firstPage=await(await fetch(baseUrl)).text(),maxPage=Math.max(...[...firstPage.matchAll(/page=(\d+)/g)].map(m=>+m[1]));console.log(`Found ${maxPage} pages`);const allHtml={1:firstPage};for(let i=2;i<=maxPage;i++){console.log(`Page ${i}/${maxPage}`);allHtml[i]=await(await fetch(`${baseUrl}?page=${i}`)).text();await new Promise(r=>setTimeout(r,300))}console.log("Uploading to GitHub...");let sha=null;try{const meta=await(await fetch(`https://api.github.com/repos/${REPO}/contents/${PATH}`,{headers:{Authorization:`Bearer ${TOKEN}`}})).json();sha=meta.sha}catch(e){}const resp=await fetch(`https://api.github.com/repos/${REPO}/contents/${PATH}`,{method:"PUT",headers:{Authorization:`Bearer ${TOKEN}`,"Content-Type":"application/json"},body:JSON.stringify({message:`Update WBO data (${maxPage} pages)`,content:btoa(unescape(encodeURIComponent(JSON.stringify(allHtml)))),sha})});resp.ok?alert(`Done! Uploaded ${maxPage} pages.`):alert("Upload failed: "+(await resp.json()).message)})();
