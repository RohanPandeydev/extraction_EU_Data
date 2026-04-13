require("dotenv").config();
const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const LOCAL_DATA_DIR = path.join(__dirname, "../../data/icij");
const ZIP_PATH = path.join(LOCAL_DATA_DIR, "icij-medical-devices.zip");
const EXTRACT_DIR = path.join(LOCAL_DATA_DIR, "extracted");

/**
 * Check if ICIJ data is available locally
 * Users must manually download from https://medicaldevices.icij.org/p/download
 * This loader processes the downloaded files
 */
function checkLocalData() {
  if (!fs.existsSync(LOCAL_DATA_DIR)) {
    return { available: false, reason: "data/icij directory does not exist" };
  }

  const hasZip = fs.existsSync(ZIP_PATH);
  const hasExtracted = fs.existsSync(EXTRACT_DIR);

  if (hasExtracted) {
    const files = fs.readdirSync(EXTRACT_DIR).filter((f) => f.endsWith(".csv"));
    return { available: true, type: "extracted", files };
  }

  if (hasZip) {
    return { available: true, type: "zip" };
  }

  return {
    available: false,
    reason: "No ICIJ ZIP or CSV found. Download from https://medicaldevices.icij.org/p/download",
  };
}

/**
 * Extract ZIP if present and not yet extracted
 */
function ensureExtracted() {
  const check = checkLocalData();

  if (!check.available) {
    throw new Error(`ICIJ data not available: ${check.reason}`);
  }

  if (check.type === "zip") {
    console.log(`Extracting ICIJ ZIP: ${ZIP_PATH}`);
    fs.mkdirSync(EXTRACT_DIR, { recursive: true });
    execSync(`unzip -o "${ZIP_PATH}" -d "${EXTRACT_DIR}"`);
    const files = fs.readdirSync(EXTRACT_DIR).filter((f) => f.endsWith(".csv"));
    console.log(`Extracted ${files.length} CSV files: ${files.join(", ")}`);
    return files;
  }

  return check.files;
}

/**
 * Parse a single CSV row handling quoted fields
 */
function parseCsvRow(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 2;
        continue;
      }
      inQuotes = !inQuotes;
      i++;
      continue;
    }

    if (char === "," && !inQuotes) {
      fields.push(current.trim());
      current = "";
      i++;
      continue;
    }

    current += char;
    i++;
  }

  fields.push(current.trim());
  return fields.length === 0 ? null : fields;
}

/**
 * Normalize an ICIJ record
 * ICIJ CSV columns typically include:
 * - country: Country of the recall
 * - recall_number: Recall/reference number
 * - product: Product name
 * - manufacturer: Manufacturer name
 * - date: Date of the recall
 * - type: Type of action (recall, FSN, safety alert)
 * - description: Description of the issue
 * - reason: Reason for recall
 * - action: Corrective action
 * - url: Source URL
 */
function normalizeIcijRecord(row, headers, sourceFile = null) {
  const record = {};

  for (let i = 0; i < headers.length; i++) {
    const header = (headers[i] || "").toLowerCase().trim().replace(/\s+/g, "_");
    const value = (row[i] || "").trim() || null;
    record[header] = value;
  }

  // Map common field variations
  return {
    source: "ICIJ",
    sourceFile: sourceFile ? path.basename(sourceFile) : null,

    // Device info
    deviceName:
      record.product || record.product_name || record.device || record.device_name || record.title || null,
    manufacturerName: record.manufacturer || record.company || record.firm || record.brand || null,
    modelName: record.model || record.model_name || null,
    catalogNumber: record.catalog_number || record.reference || record.ref_number || record.recall_number || null,
    lotNumber: record.lot || record.lot_number || record.batch || null,

    // Recall info
    country: record.country || record.countries || record.source_country || null,
    date: record.date || record.recall_date || record.date_published || record.published || null,
    recallType: record.type || record.recall_type || record.action_type || record.classification || null,
    reason: record.reason || record.description || record.summary || null,
    correctiveAction: record.action || record.corrective_action || record.action_taken || null,
    riskLevel: record.risk || record.risk_level || record.hazard || record.classification || null,

    // Distribution
    distributionInfo: record.distribution || record.distribution_pattern || record.affected_countries || null,

    // Metadata
    sourceUrl: record.url || record.source_url || record.link || null,
    sourceAgency: record.agency || record.authority || record.source || record.regulator || null,
    language: record.language || null,
    additionalNotes: record.notes || record.additional_info || record.comments || null,

    // Full raw for anything we missed
    raw: record,
  };
}

/**
 * Process all ICIJ CSV files
 */
async function processIcijCsv(outputCb, maxRecords = null) {
  console.log("=== ICIJ: Processing Medical Devices Database ===");

  const check = checkLocalData();
  if (!check.available) {
    console.log("ICIJ data not found locally.");
    console.log("Download from: https://medicaldevices.icij.org/p/download");
    console.log(`Place the ZIP file in: ${ZIP_PATH}`);
    return 0;
  }

  const csvFiles = ensureExtracted();
  let totalProcessed = 0;

  for (const csvFile of csvFiles) {
    const filePath = path.join(EXTRACT_DIR, csvFile);
    console.log(`\nProcessing: ${csvFile}`);

    const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
    // File size: fs.statSync(filePath).size (logged if needed)

    let chunk = "";
    let headers = null;
    let fileProcessed = 0;
    let hasMore = true;

    await new Promise((resolve, reject) => {
      fileStream.on("data", (data) => {
        if (!hasMore) return;

        chunk += data;
        const lines = chunk.split("\n");
        chunk = lines.pop();

        for (const line of lines) {
          if (!hasMore) break;

          if (!headers) {
            headers = parseCsvRow(line);
            if (headers) {
              console.log(`  Headers: ${headers.length} columns`);
              console.log(`  ${headers.slice(0, 10).join(" | ")}${headers.length > 10 ? "..." : ""}`);
            }
            continue;
          }

          const row = parseCsvRow(line);
          if (row && row.length > 0 && row.some((f) => f !== "")) {
            const normalized = normalizeIcijRecord(row, headers, csvFile);
            totalProcessed++;
            fileProcessed++;

            if (outputCb) {
              outputCb(normalized).catch((err) =>
                console.error(`Error processing record ${totalProcessed}:`, err.message)
              );
            }

            if (maxRecords && totalProcessed >= maxRecords) {
              hasMore = false;
              break;
            }
          }
        }
      });

      fileStream.on("end", () => {
        // Process remaining
        if (chunk && headers) {
          const row = parseCsvRow(chunk);
          if (row && row.length > 0 && row.some((f) => f !== "")) {
            normalizeIcijRecord(row, headers, csvFile);
            totalProcessed++;
            fileProcessed++;
          }
        }
        console.log(`  Processed ${fileProcessed.toLocaleString()} records from ${csvFile}`);
        resolve();
      });

      fileStream.on("error", reject);
    });

    if (maxRecords && totalProcessed >= maxRecords) break;
  }

  console.log(`\nTotal ICIJ records processed: ${totalProcessed.toLocaleString()}`);
  return totalProcessed;
}

/**
 * Get summary statistics from ICIJ data
 */
async function getIcijSummary(maxSample = 10000) {
  const check = checkLocalData();
  if (!check.available) {
    return { available: false, reason: check.reason };
  }

  // Ensure CSV files are extracted before processing
  ensureExtracted();
  const countries = new Set();
  const types = new Set();
  const manufacturers = new Map();

  let sampleCount = 0;

  await processIcijCsv((record) => {
    sampleCount++;
    if (record.country) countries.add(record.country);
    if (record.recallType) types.add(record.recallType);
    if (record.manufacturerName) {
      manufacturers.set(
        record.manufacturerName,
        (manufacturers.get(record.manufacturerName) || 0) + 1
      );
    }
  }, maxSample);

  // Top manufacturers
  const topMfr = [...manufacturers.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([name, count]) => ({ name, count }));

  return {
    available: true,
    sampleSize: sampleCount,
    uniqueCountries: [...countries].sort(),
    recallTypes: [...types].sort(),
    topManufacturers: topMfr,
    note: "Data frozen at Nov 2019 — not updated since",
  };
}

module.exports = {
  processIcijCsv,
  getIcijSummary,
  checkLocalData,
  ensureExtracted,
  normalizeIcijRecord,
  parseCsvRow,
  LOCAL_DATA_DIR,
  ZIP_PATH,
  EXTRACT_DIR,
};

if (require.main === module) {
  (async () => {
    const check = checkLocalData();
    if (!check.available) {
      console.log("ICIJ data not available locally.");
      console.log(`Download from: https://medicaldevices.icij.org/p/download`);
      console.log(`Place ZIP at: ${ZIP_PATH}`);
    } else {
      const summary = await getIcijSummary(5000);
      console.log("\n=== ICIJ Data Summary ===");
      console.log(`Sample size: ${summary.sampleSize.toLocaleString()}`);
      console.log(`Countries: ${summary.uniqueCountries.length}`);
      console.log(`Recall types: ${summary.recallTypes.size}`);
      console.log("\nTop manufacturers:");
      summary.topManufacturers.forEach((m, i) => {
        console.log(`  ${i + 1}. ${m.name} (${m.count} records)`);
      });
    }
  })().catch(console.error);
}
