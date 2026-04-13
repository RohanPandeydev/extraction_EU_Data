require("dotenv").config();

const scrapers = [
  { name: "EUDAMED Devices", module: "../script" },
  { name: "EUDAMED Vigilance", module: "../scrapers/eudamed/vigilance" },
  { name: "EUDAMED Clinical Investigations", module: "../scrapers/eudamed/clinical-investigations" },
  { name: "EUDAMED Certificates", module: "../scrapers/eudamed/certificates" },
  { name: "EUDAMED FSCA", module: "../scrapers/eudamed/fsca" },
  { name: "EMA Combination Products", module: "../scrapers/ema/combination-products" },
  { name: "WHO VigiAccess", module: "../scrapers/who/vigibase" },
  { name: "Cochrane Reviews", module: "../scrapers/cochrane/reviews" },
  { name: "BfArM Recalls (Germany)", module: "../scrapers/bfarm/recalls" },
  { name: "ANSM Decisions (France)", module: "../scrapers/ansm/recalls" },
  { name: "ISS Safety (Italy)", module: "../scrapers/iss/safety" },
  { name: "IGJ Recalls (Netherlands)", module: "../scrapers/igj/recalls" },
  { name: "AEMPS Alerts (Spain)", module: "../scrapers/aemps/alerts" },
  { name: "NARA Implant Registry", module: "../scrapers/nara/implants" },
  { name: "SCENIHR/SCHEER Opinions", module: "../scrapers/scenihr/opinions" },
];

async function runAll() {
  console.log("========================================");
  console.log("  Medical Device Data Extraction Suite  ");
  console.log("========================================\n");
  console.log(`Starting ${scrapers.length} scrapers...\n`);

  const results = [];

  for (const scraper of scrapers) {
    console.log(`\n>>> Starting: ${scraper.name}`);
    const startTime = Date.now();
    try {
      const mod = require(scraper.module);
      if (mod.run) {
        await mod.run();
      }
      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      console.log(`<<< ${scraper.name} completed in ${duration}s`);
      results.push({ name: scraper.name, status: "SUCCESS", duration });
    } catch (error) {
      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      console.error(`<<< ${scraper.name} FAILED: ${error.message}`);
      results.push({ name: scraper.name, status: "FAILED", duration, error: error.message });
    }
  }

  console.log("\n========================================");
  console.log("  EXTRACTION SUMMARY");
  console.log("========================================");
  for (const r of results) {
    const icon = r.status === "SUCCESS" ? "[OK]" : "[FAIL]";
    console.log(`${icon} ${r.name} (${r.duration}s) ${r.error ? "- " + r.error : ""}`);
  }
  console.log("========================================\n");

  process.exit(0);
}

runAll().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
