require("dotenv").config();

async function runEudamed() {
  console.log("=== Running all EUDAMED scrapers ===\n");

  const scrapers = [
    { name: "Devices", run: require("../script").run || require("../scrapers/eudamed/vigilance").run },
    { name: "Vigilance", run: require("../scrapers/eudamed/vigilance").run },
    { name: "Clinical Investigations", run: require("../scrapers/eudamed/clinical-investigations").run },
    { name: "Certificates", run: require("../scrapers/eudamed/certificates").run },
    { name: "FSCA", run: require("../scrapers/eudamed/fsca").run },
  ];

  for (const scraper of scrapers) {
    console.log(`\n>>> ${scraper.name}`);
    try {
      await scraper.run();
    } catch (error) {
      console.error(`Failed: ${scraper.name} - ${error.message}`);
    }
  }

  console.log("\n=== All EUDAMED scrapers complete ===");
  process.exit(0);
}

runEudamed().catch(console.error);
