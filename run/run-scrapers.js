require("dotenv").config();

async function runScrapers() {
  console.log("=== Running HTML scrapers (national regulators) ===\n");

  const scrapers = [
    { name: "BfArM (Germany)", run: require("../scrapers/bfarm/recalls").run },
    { name: "ANSM (France)", run: require("../scrapers/ansm/recalls").run },
    { name: "ISS (Italy)", run: require("../scrapers/iss/safety").run },
    { name: "IGJ (Netherlands)", run: require("../scrapers/igj/recalls").run },
    { name: "AEMPS (Spain)", run: require("../scrapers/aemps/alerts").run },
    { name: "NARA (Nordic)", run: require("../scrapers/nara/implants").run },
    { name: "SCENIHR/SCHEER", run: require("../scrapers/scenihr/opinions").run },
  ];

  for (const scraper of scrapers) {
    console.log(`\n>>> ${scraper.name}`);
    try {
      await scraper.run();
    } catch (error) {
      console.error(`Failed: ${scraper.name} - ${error.message}`);
    }
  }

  console.log("\n=== All scrapers complete ===");
  process.exit(0);
}

runScrapers().catch(console.error);
