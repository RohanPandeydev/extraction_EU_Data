require("dotenv").config();
const fs = require("fs");
const path = require("path");
const {
  setupDatabase,
  insertDevicesBatch,
  insertNotifiedBodiesBatch,
  insertRefusedApplicationsBatch,
  insertSafetyNoticesBatch,
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
  await insertNotifiedBodiesBatch(nbData);
  log("SNOWFLAKE", `Notified bodies: ${nbData.length} loaded`);

  // 2. Refused Applications
  log("SNOWFLAKE", "Loading refused applications...");
  const apps = loadJSON(path.join(BULK_DIR, "eudamed_refused_applications.json"));
  await insertRefusedApplicationsBatch(apps);
  log("SNOWFLAKE", `Refused applications: ${apps.length} loaded`);

  // 3. ANSM Safety
  log("SNOWFLAKE", "Loading ANSM safety data...");
  const ansm = loadJSON(path.join(BULK_DIR, "ansm_safety.json"));
  await insertSafetyNoticesBatch("ANSM", ansm);
  log("SNOWFLAKE", `ANSM records: ${ansm.length} loaded`);

  // 4. SCHEER Opinions
  log("SNOWFLAKE", "Loading SCHEER opinions...");
  const scheer = loadJSON(path.join(BULK_DIR, "scheer_opinions.json"));
  await insertSafetyNoticesBatch("SCHEER", scheer.map(r => ({ ...r, deviceName: r.title })));
  log("SNOWFLAKE", `SCHEER opinions: ${scheer.length} loaded`);
}

async function loadDevices() {
  if (!fs.existsSync(DEVICES_DIR)) {
    log("SNOWFLAKE", "No devices directory found");
    return;
  }

  const files = fs.readdirSync(DEVICES_DIR).filter((f) => f.endsWith(".json"));
  log("SNOWFLAKE", `Loading ${files.length} devices...`);

  const BATCH_SIZE = 100;
  let loaded = 0;
  let errors = 0;
  let buffer = [];

  const flush = async () => {
    if (buffer.length === 0) return;
    try {
      await insertDevicesBatch(buffer);
      loaded += buffer.length;
      log("SNOWFLAKE", `Devices progress: ${loaded}/${files.length} (${((loaded / files.length) * 100).toFixed(1)}%)`);
    } catch (error) {
      errors += buffer.length;
      if (errors <= 5) log("SNOWFLAKE", `Batch error: ${error.message}`);
    }
    buffer = [];
  };

  for (const file of files) {
    try {
      const deviceJSON = JSON.parse(fs.readFileSync(path.join(DEVICES_DIR, file), "utf-8"));
      buffer.push(deviceJSON);
      if (buffer.length >= BATCH_SIZE) await flush();
    } catch (error) {
      errors++;
      if (errors <= 5) log("SNOWFLAKE", `Error loading ${file}: ${error.message}`);
    }
  }
  await flush();

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
  const tables = ["DEVICES", "NOTIFIED_BODIES", "REFUSED_APPLICATIONS", "SAFETY_NOTICES"];
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
