require("dotenv").config();
// NOTE: EUDAMED vigilance/adverse event data is NOT available via the public API.
// The vigilance subsystem runs on a separate internal backend (vig-data-ws)
// that is only accessible within the EU network infrastructure.
// This scraper is a placeholder — data must come from other sources
// (e.g. BfArM, ANSM, WHO VigiAccess, or manual EUDAMED portal access).

async function run() {
  console.log("=== EUDAMED Vigilance/Adverse Events ===");
  console.log("SKIPPED: Vigilance data is not available via the public EUDAMED API.");
  console.log("Use national regulator scrapers (BfArM, ANSM, etc.) or WHO VigiAccess instead.");
  console.log("=== Vigilance extraction skipped ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
