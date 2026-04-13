require("dotenv").config();
// NOTE: EUDAMED clinical investigations data is NOT available via the public API.
// No public endpoint exists for this data.
// This scraper is a placeholder.

async function run() {
  console.log("=== EUDAMED Clinical Investigations ===");
  console.log("SKIPPED: Clinical investigation data is not available via the public EUDAMED API.");
  console.log("=== Clinical Investigations extraction skipped ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
