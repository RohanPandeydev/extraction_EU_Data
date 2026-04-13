require("dotenv").config();
const fs = require("fs");
const path = require("path");

const { extractBasicUdiData } = require("../scrapers/eudamed/devices-api");
const { extractAllRecalls, searchRecalls } = require("../scrapers/openfda/device-recalls");
const { processCoreMdCsv, ensureCsvDownloaded, getDatasetInfo } = require("../scrapers/core-md/loader");
const { processIcijCsv, checkLocalData: checkIcijData } = require("../scrapers/icij/loader");
const {
  matchAdverseEventsToDevice,
  buildUnifiedRecords,
  saveUnifiedRecords,
} = require("../scrapers/unified-merger");

// OUTPUT_DIR: ../output/unified (currently using default output paths)

// ============ Collectors ============

async function collectDevices(maxPages = null) {
  console.log("\n" + "=".repeat(70));
  console.log("PHASE 1: Extracting devices from EUDAMED (live)");
  console.log("=".repeat(70));

  const devices = [];
  await extractBasicUdiData(async (device) => {
    if (device) devices.push(device);
  }, maxPages);

  console.log(`\n✅ Devices collected: ${devices.length.toLocaleString()}`);
  return devices;
}

async function collectCoreMd(maxRecords = null) {
  console.log("\n" + "=".repeat(70));
  console.log("PHASE 2: Loading CORE-MD dataset (up to Feb 2024)");
  console.log("=".repeat(70));

  // Show dataset info
  const info = await getDatasetInfo().catch(() => null);
  if (info) {
    console.log(`Dataset: ${info.title}`);
    console.log(`Published: ${info.publicationDate}`);
    console.log(`File size: ${info.fileSize ? (info.fileSize / 1024 / 1024).toFixed(1) + " MB" : "unknown"}`);
  }

  // Ensure CSV is downloaded
  await ensureCsvDownloaded();

  const records = [];
  await processCoreMdCsv(async (record) => {
    if (record) records.push(record);
  }, maxRecords);

  console.log(`\n✅ CORE-MD records loaded: ${records.length.toLocaleString()}`);
  return records;
}

async function collectIcij(maxRecords = null) {
  console.log("\n" + "=".repeat(70));
  console.log("PHASE 3: Loading ICIJ dataset (up to Nov 2019)");
  console.log("=".repeat(70));

  const check = checkIcijData();
  if (!check.available) {
    console.log(`⚠️  ICIJ data not available: ${check.reason}`);
    console.log("Skipping ICIJ loading. Download from https://medicaldevices.icij.org/p/download");
    return [];
  }

  const records = [];
  await processIcijCsv(async (record) => {
    if (record) records.push(record);
  }, maxRecords);

  console.log(`\n✅ ICIJ records loaded: ${records.length.toLocaleString()}`);
  return records;
}

async function collectOpenFda(maxPages = null) {
  console.log("\n" + "=".repeat(70));
  console.log("PHASE 4: Extracting device recalls from openFDA (live, weekly updated)");
  console.log("=".repeat(70));

  const records = [];
  await extractAllRecalls(async (record) => {
    if (record) records.push(record);
  }, maxPages);

  console.log(`\n✅ openFDA recalls loaded: ${records.length.toLocaleString()}`);
  return records;
}

// ============ Merge ============

async function mergeData(devices, adverseEvents) {
  console.log("\n" + "=".repeat(70));
  console.log("PHASE 5: Merging devices with adverse events");
  console.log("=".repeat(70));

  // Match adverse events to devices
  const matchResult = matchAdverseEventsToDevice(devices, adverseEvents);

  // Build unified records
  console.log("\nBuilding unified records...");
  const records = buildUnifiedRecords(devices, matchResult.matched);

  // Save
  await saveUnifiedRecords(records);

  return { records, matchResult };
}

// ============ Commands ============

async function cmdFull(options = {}) {
  const {
    maxDevicePages = null,
    maxCoreMd = null,
    maxIcij = null,
    maxFdaPages = null,
    skipCoreMd = false,
    skipIcij = false,
    skipFda = false,
    skipMerge = false,
  } = options;

  const startTime = Date.now();

  // Phase 1: EUDAMED devices
  const devices = await collectDevices(maxDevicePages);

  // Collect adverse events in parallel
  const adverseEvents = [];

  if (!skipCoreMd) {
    const coreMd = await collectCoreMd(maxCoreMd);
    adverseEvents.push(...coreMd);
  }

  if (!skipIcij) {
    const icij = await collectIcij(maxIcij);
    adverseEvents.push(...icij);
  }

  if (!skipFda) {
    const fda = await collectOpenFda(maxFdaPages);
    adverseEvents.push(...fda);
  }

  console.log(`\n✅ Total adverse events loaded: ${adverseEvents.length.toLocaleString()}`);

  if (!skipMerge && devices.length > 0 && adverseEvents.length > 0) {
    await mergeData(devices, adverseEvents);
  } else if (skipMerge) {
    console.log("\n⏭️  Merge skipped (--no-merge)");
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n⏱️  Total time: ${elapsed} minutes`);
}

async function cmdDevicesOnly(options = {}) {
  const { maxPages = null, outputFile = null } = options;

  const devices = [];
  await extractBasicUdiData(async (device) => {
    if (device) devices.push(device);
  }, maxPages);

  if (outputFile) {
    fs.mkdirSync(path.dirname(outputFile), { recursive: true });
    fs.writeFileSync(outputFile, JSON.stringify(devices, null, 2));
    console.log(`\n✅ Saved ${devices.length.toLocaleString()} devices to: ${outputFile}`);
  }

  return devices;
}

async function cmdSearchOpenFda(query, limit = 50) {
  const results = await searchRecalls(query, limit);
  console.log(`\n=== Search Results: "${query}" ===`);
  console.log(`Found: ${results.length} recalls`);
  for (const r of results.slice(0, 10)) {
    console.log(`\n  ${r.datePosted || "N/A"} | ${r.recallingFirm || "N/A"}`);
    console.log(`  Product: ${(r.productDescription || "").substring(0, 100)}`);
    console.log(`  Reason: ${(r.reasonForRecall || "").substring(0, 100)}`);
  }
  return results;
}

async function cmdMergeOnly(options = {}) {
  const { devicesFile, adverseEventsFile, outputFile = null } = options;

  console.log("Loading devices from file...");
  const devices = JSON.parse(fs.readFileSync(devicesFile, "utf-8"));

  console.log("Loading adverse events from file...");
  const adverseEvents = JSON.parse(fs.readFileSync(adverseEventsFile, "utf-8"));

  console.log(`Devices: ${devices.length.toLocaleString()}, Adverse events: ${adverseEvents.length.toLocaleString()}`);

  const { records } = await mergeData(devices, adverseEvents);

  if (outputFile) {
    const outputPath = path.resolve(outputFile);
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    const output = {
      metadata: { generatedAt: new Date().toISOString() },
      records,
    };
    fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
    console.log(`\n✅ Saved merged data to: ${outputPath}`);
  }

  return records;
}

async function cmdInfo() {
  console.log("=== Data Source Information ===\n");

  // EUDAMED
  console.log("EUDAMED (devices + manufacturers):");
  console.log("  Status: ✅ Live (real-time)");
  console.log("  Records: ~1.5M devices, ~490K basic UDI records");
  console.log("  Endpoint: https://ec.europa.eu/tools/eudamed/api/devices/basicUdiData");
  console.log();

  // CORE-MD
  console.log("CORE-MD (EU adverse events):");
  const coreMdInfo = await getDatasetInfo().catch(() => null);
  if (coreMdInfo) {
    console.log(`  Status: ✅ Available (frozen at ${coreMdInfo.publicationDate})`);
    console.log(`  Records: ~137,720 FSNs from 16 EU countries`);
    console.log(`  File: ${coreMdInfo.fileSize ? (coreMdInfo.fileSize / 1024 / 1024).toFixed(1) + " MB" : "unknown"}`);
  }
  console.log("  Download: https://zenodo.org/records/10864069");
  console.log();

  // ICIJ
  console.log("ICIJ (global recalls):");
  const icijCheck = checkIcijData();
  console.log(`  Status: ${icijCheck.available ? "✅ Available locally" : "⚠️  Not downloaded"}`);
  console.log("  Records: ~120,000 recalls from 36 countries");
  console.log("  Frozen: Nov 2019 (no longer updated)");
  console.log("  Download: https://medicaldevices.icij.org/p/download");
  console.log();

  // openFDA
  console.log("openFDA (US device recalls, includes EU companies):");
  console.log("  Status: ✅ Live (updated weekly)");
  console.log("  Records: ~57,823 recalls");
  console.log("  Endpoint: https://api.fda.gov/device/recall.json");
}

// ============ CLI ============

function printUsage() {
  console.log(`
Usage: node run/unified-extraction.js <command> [options]

Commands:
  full          Run full extraction (EUDAMED + CORE-MD + ICIJ + openFDA → merge)
  devices       Extract EUDAMED devices only
  search-fda    Search openFDA device recalls
  merge-only    Merge pre-extracted device and adverse event files
  info          Show data source information

Options (full):
  --max-device-pages <N>   Limit EUDAMED device pages (each = 300 devices)
  --max-core-md <N>        Limit CORE-MD records to process
  --max-icij <N>           Limit ICIJ records to process
  --max-fda-pages <N>      Limit openFDA pages
  --skip-core-md           Skip CORE-MD loading
  --skip-icij              Skip ICIJ loading
  --skip-fda               Skip openFDA loading
  --no-merge               Don't merge after extraction

Options (devices):
  --max-pages <N>          Limit device pages
  --output <file>          Save devices to file

Options (search-fda):
  --query <text>           Search query (required)
  --limit <N>              Result limit (default: 50)

Options (merge-only):
  --devices-file <file>    Path to devices JSON file (required)
  --adverse-events-file <file>  Path to adverse events JSON file (required)
  --output <file>          Output file path

Examples:
  # Full extraction (limited for testing)
  node run/unified-extraction.js full --max-device-pages 5 --max-core-md 10000 --max-fda-pages 5

  # EUDAMED devices only
  node run/unified-extraction.js devices --max-pages 10 --output output/devices.json

  # Search FDA recalls
  node run/unified-extraction.js search-fda --query "Medtronic" --limit 20

  # Merge pre-extracted data
  node run/unified-extraction.js merge-only --devices-file output/devices.json --adverse-events-file output/adverse-events.json --output output/merged.json
`);
}

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  if (!command || command === "--help" || command === "-h") {
    printUsage();
    return;
  }

  // Parse options
  const options = {};
  for (let i = 1; i < args.length; i += 2) {
    const key = args[i].replace(/^--?/, "");
    const value = args[i + 1];
    if (value !== undefined && !value.startsWith("--")) {
      // Convert to number if possible
      options[key] = /^\d+$/.test(value) ? parseInt(value, 10) : value;
    } else {
      options[key] = true;
      i--; // Don't skip next
    }
  }

  switch (command) {
    case "full":
      await cmdFull(options);
      break;

    case "devices":
      await cmdDevicesOnly(options);
      break;

    case "search-fda":
      if (!options.query) {
        console.error("Error: --query is required for search-fda");
        process.exit(1);
      }
      await cmdSearchOpenFda(options.query, options.limit || 50);
      break;

    case "merge-only":
      if (!options.devicesFile || !options.adverseEventsFile) {
        console.error("Error: --devices-file and --adverse-events-file are required for merge-only");
        process.exit(1);
      }
      await cmdMergeOnly(options);
      break;

    case "info":
      await cmdInfo();
      break;

    default:
      console.error(`Unknown command: ${command}`);
      printUsage();
      process.exit(1);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
