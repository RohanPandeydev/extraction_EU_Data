require("dotenv").config();
const { createTableWHOAdverseEvents } = require("../../data/who-queries");

// WHO VigiAccess uses anti-scraping measures (invisible Unicode characters,
// session-based access, Cloudflare protection). Programmatic extraction
// requires a headless browser (Puppeteer/Playwright).
// For now, this is a placeholder.

async function run() {
  console.log("=== WHO VigiAccess Adverse Events ===");
  await createTableWHOAdverseEvents();
  console.log("SKIPPED: VigiAccess requires headless browser due to anti-scraping measures.");
  console.log("Table created — data can be imported manually or via Puppeteer in the future.");
  console.log("=== WHO VigiAccess extraction skipped ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
