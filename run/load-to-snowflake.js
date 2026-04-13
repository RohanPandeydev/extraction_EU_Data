require("dotenv").config();
const fs = require("fs");
const path = require("path");
const {
  setupDatabase,
  insertDeviceComplete,
  insertNotifiedBody,
  insertRefusedApplication,
  insertEMAMedicine,
  insertCochraneReview,
  insertSafetyNotice,
  getTableCount,
  closeConnection,
} = require("../data/snowflake");

const OUTPUT_DIR = path.join(__dirname, "..", "output");
const DEVICES_DIR = path.join(OUTPUT_DIR, "devices");
const BULK_DIR = path.join(OUTPUT_DIR, "bulk");

function log(source, message) {
  const timestamp = new Date().toISOString().replace("T", " ").substring(0, 19);
  console.log(`[${timestamp}] [${source}] ${message}`);
}

function loadJSON(filePath) {
  if (!fs.existsSync(filePath)) return [];
  return JSON.parse(fs.readFileSync(filePath, "utf-8"));
}

async function loadBulkData() {
  // 1. Notified Bodies
  log("SNOWFLAKE", "Loading notified bodies...");
  const nbData = loadJSON(path.join(BULK_DIR, "eudamed_notified_bodies.json"));
  for (const nb of nbData) {
    await insertNotifiedBody(nb);
  }
  log("SNOWFLAKE", `Notified bodies: ${nbData.length} loaded`);

  // 2. Refused Applications
  log("SNOWFLAKE", "Loading refused applications...");
  const apps = loadJSON(path.join(BULK_DIR, "eudamed_refused_applications.json"));
  for (const app of apps) {
    await insertRefusedApplication(app);
  }
  log("SNOWFLAKE", `Refused applications: ${apps.length} loaded`);

  // 3. EMA Medicines
  log("SNOWFLAKE", "Loading EMA medicines...");
  const ema = loadJSON(path.join(BULK_DIR, "ema_medicines.json"));
  let emaCount = 0;
  for (const med of ema) {
    if (med.category === "Human") {
      await insertEMAMedicine(med);
      emaCount++;
    }
  }
  log("SNOWFLAKE", `EMA medicines: ${emaCount} loaded`);

  // 4. Cochrane/PubMed Reviews
  log("SNOWFLAKE", "Loading Cochrane/PubMed reviews...");
  const reviews = loadJSON(path.join(BULK_DIR, "cochrane_pubmed_reviews.json"));
  for (const review of reviews) {
    await insertCochraneReview(review);
  }
  log("SNOWFLAKE", `Cochrane reviews: ${reviews.length} loaded`);

  // 5. ANSM Safety
  log("SNOWFLAKE", "Loading ANSM safety data...");
  const ansm = loadJSON(path.join(BULK_DIR, "ansm_safety.json"));
  for (const record of ansm) {
    await insertSafetyNotice("ANSM", record);
  }
  log("SNOWFLAKE", `ANSM records: ${ansm.length} loaded`);

  // 6. SCHEER Opinions
  log("SNOWFLAKE", "Loading SCHEER opinions...");
  const scheer = loadJSON(path.join(BULK_DIR, "scheer_opinions.json"));
  for (const record of scheer) {
    await insertSafetyNotice("SCHEER", { ...record, deviceName: record.title });
  }
  log("SNOWFLAKE", `SCHEER opinions: ${scheer.length} loaded`);
}

async function loadDevices() {
  if (!fs.existsSync(DEVICES_DIR)) {
    log("SNOWFLAKE", "No devices directory found");
    return;
  }

  const files = fs.readdirSync(DEVICES_DIR).filter((f) => f.endsWith(".json"));
  log("SNOWFLAKE", `Loading ${files.length} devices...`);

  let loaded = 0;
  let errors = 0;

  for (const file of files) {
    try {
      const deviceJSON = JSON.parse(fs.readFileSync(path.join(DEVICES_DIR, file), "utf-8"));
      await insertDeviceComplete(deviceJSON);
      loaded++;
      if (loaded % 50 === 0) {
        log("SNOWFLAKE", `Devices progress: ${loaded}/${files.length} (${((loaded / files.length) * 100).toFixed(1)}%)`);
      }
    } catch (error) {
      errors++;
      if (errors <= 5) log("SNOWFLAKE", `Error loading ${file}: ${error.message}`);
    }
  }

  log("SNOWFLAKE", `Devices loaded: ${loaded} | Errors: ${errors}`);
}

async function main() {
  console.log("");
  console.log("========================================================");
  console.log("       LOAD JSON DATA INTO SNOWFLAKE");
  console.log("========================================================");
  log("MAIN", `Reading from: ${OUTPUT_DIR}`);
  log("MAIN", `Started at: ${new Date().toISOString()}`);
  console.log("========================================================\n");

  await setupDatabase();
  console.log("");

  // Load bulk data
  log("MAIN", "====== Loading Bulk Data ======");
  await loadBulkData();
  console.log("");

  // Load devices
  log("MAIN", "====== Loading Devices ======");
  await loadDevices();
  console.log("");

  // Stats
  log("MAIN", "====== Final Stats ======");
  const tables = ["DEVICE_COMPLETE", "NOTIFIED_BODIES", "REFUSED_APPLICATIONS", "EMA_MEDICINES", "COCHRANE_REVIEWS", "SAFETY_NOTICES"];
  for (const table of tables) {
    const count = await getTableCount(table);
    log("MAIN", `  ${table}: ${count} rows`);
  }

  console.log("");
  log("MAIN", `Finished at: ${new Date().toISOString()}`);
  console.log("========================================================\n");

  closeConnection();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  closeConnection();
  process.exit(1);
});
