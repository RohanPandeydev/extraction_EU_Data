require("dotenv").config();
// NOTE: EUDAMED FSCA data is NOT available via the public API.
// The vigilance/FSCA subsystem runs on a separate internal backend (vig-data-ws)
// that is only accessible within the EU network infrastructure.
// This scraper is a placeholder.

async function run() {
  console.log("=== EUDAMED Field Safety Corrective Actions ===");
  console.log("SKIPPED: FSCA data is not available via the public EUDAMED API.");
  console.log("Use national regulator scrapers for recall/safety action data instead.");
  console.log("=== FSCA extraction skipped ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
