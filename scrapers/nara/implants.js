require("dotenv").config();
const { createTableNARA } = require("../../data/nara-queries");

// NARA (Nordic Arthroplasty Register Association) website at nara.medicor.se
// is currently unreachable. nara.nu is an unrelated Swedish company.
// Nordic FSNs come from individual country regulators:
// - Sweden: lakemedelsverket.se
// - Norway: dmp.no/en/medical-devices
// - Denmark: laegemiddelstyrelsen.dk
// - Finland: fimea.fi

async function run() {
  console.log("=== NARA Nordic Arthroplasty Registry ===");
  await createTableNARA();
  console.log("SKIPPED: NARA website (nara.medicor.se) is currently unreachable.");
  console.log("Nordic device data available from individual country regulators.");
  console.log("=== NARA extraction skipped ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
