require("dotenv").config();
const fs = require("fs");
const path = require("path");
const { fetchHTML } = require("../../utils/httpClient");

const ZENODO_API_URL = "https://zenodo.org/api/records/10864069";
const LOCAL_DATA_DIR = path.join(__dirname, "../../data/core-md");
const CSV_FILENAME = "data_upload_Mar2024_v1.csv";
const CSV_PATH = path.join(LOCAL_DATA_DIR, CSV_FILENAME);

/**
 * Get download URL for CORE-MD dataset from Zenodo API
 */
async function getDownloadUrl() {
  const raw = await fetchHTML(ZENODO_API_URL, {
    headers: { Accept: "application/json" },
  });
  const data = JSON.parse(raw);
  const files = data.files || [];
  const csvFile = files.find((f) => f.key?.endsWith(".csv"));
  if (!csvFile) {
    throw new Error("No CSV file found in Zenodo record");
  }
  return csvFile.links?.self || null;
}

/**
 * Download CORE-MD CSV from Zenodo if not already present locally
 */
async function ensureCsvDownloaded() {
  if (fs.existsSync(CSV_PATH)) {
    const stats = fs.statSync(CSV_PATH);
    console.log(
      `CORE-MD CSV already exists: ${CSV_PATH} (${(stats.size / 1024 / 1024).toFixed(1)} MB)`
    );
    return true;
  }

  fs.mkdirSync(LOCAL_DATA_DIR, { recursive: true });

  const downloadUrl = await getDownloadUrl();
  if (!downloadUrl) {
    throw new Error("Could not find CORE-MD download URL");
  }

  console.log(`Downloading CORE-MD CSV from Zenodo: ${downloadUrl}`);
  console.log("This may take a while (186 MB)...");

  const response = await fetch(downloadUrl, {
    headers: {
      "User-Agent":
        "eudamed-data-extraction/1.0 (https://github.com/openregulatory/eudamed-api)",
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status} downloading ${downloadUrl}`);
  }

  const contentLength = parseInt(response.headers.get("content-length") || "0", 10);
  let downloaded = 0;
  const startTime = Date.now();

  const fileStream = fs.createWriteStream(CSV_PATH);
  const reader = response.body.getReader();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    fileStream.write(Buffer.from(value));
    downloaded += value.length;

    if (contentLength > 0) {
      const pct = ((downloaded / contentLength) * 100).toFixed(1);
      const mb = (downloaded / 1024 / 1024).toFixed(1);
      const totalMb = (contentLength / 1024 / 1024).toFixed(1);
      const elapsed = (Date.now() - startTime) / 1000;
      const speed = (downloaded / 1024 / 1024 / elapsed).toFixed(2);
      process.stdout.write(`\r  ${pct}% — ${mb} MB / ${totalMb} MB (${speed} MB/s)`);
    }
  }

  fileStream.end();
  console.log("\nDownload complete.");

  if (!contentLength) {
    const stats = fs.statSync(CSV_PATH);
    console.log(`File size: ${(stats.size / 1024 / 1024).toFixed(1)} MB`);
  }

  return true;
}

/**
 * Parse a single CORE-MD FSN record into a flat object
 * Fields vary but common ones include:
 * - country: Source country of the notice
 * - date_issued: Publication date
 * - manufacturer: Manufacturer name
 * - device_name: Device name/description
 * - recall_type: Type of action (recall, FSN, etc.)
 * - risk_class: Risk classification
 * - corrective_action: Description of corrective action
 * - reason: Reason for the notice
 * - affected_products: Products affected
 * - url: Source URL
 * - language: Language of the notice
 */
function normalizeCoreMdRecord(row, headers) {
  const record = {};

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    const value = row[i]?.trim() || null;
    record[header] = value;
  }

  return {
    source: "CORE-MD",
    sourceUrl: record.url || record.source_url || null,

    // Device info
    deviceName: record.device_name || record.product_name || record.product || null,
    manufacturerName: record.manufacturer || record.company || record.firm || null,
    modelName: record.model || record.model_name || null,
    catalogNumber: record.catalog_number || record.reference || null,
    udiDi: record.udi || record.udi_di || null,

    // Recall info
    country: record.country || record.countries || null,
    dateIssued: record.date_issued || record.date || record.published || null,
    recallType: record.recall_type || record.type || record.action_type || null,
    riskClass: record.risk_class || record.risk || record.classification || null,
    reason: record.reason || record.description || null,
    correctiveAction: record.corrective_action || record.action || null,
    affectedProducts: record.affected_products || record.products || null,
    lotBatchNumbers: record.lot || record.batch || null,

    // Metadata
    language: record.language || null,
    sourceAgency: record.agency || record.authority || record.source || null,
    recordId: record.id || record.record_id || null,

    // Full raw for anything we missed
    raw: record,
  };
}

/**
 * Parse CSV string into rows (handles quoted fields, newlines in values, etc.)
 * Returns { headers, rows }
 */
function parseCsv(csvString) {
  const rows = [];
  let inQuotes = false;
  let rowBuffer = [];
  let fieldBuffer = "";

  const len = csvString.length;
  let i = 0;

  while (i < len) {
    const char = csvString[i];
    const next = csvString[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        fieldBuffer += '"';
        i += 2;
        continue;
      }
      inQuotes = !inQuotes;
      i++;
      continue;
    }

    if ((char === "," || char === ";" || char === "\t") && !inQuotes) {
      rowBuffer.push(fieldBuffer.trim());
      fieldBuffer = "";
      i++;
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      rowBuffer.push(fieldBuffer.trim());
      if (rowBuffer.length > 0 && rowBuffer.some((f) => f !== "")) {
        rows.push([...rowBuffer]);
      }
      rowBuffer = [];
      fieldBuffer = "";

      // Skip \r\n
      if (char === "\r" && next === "\n") i++;
      i++;
      continue;
    }

    fieldBuffer += char;
    i++;
  }

  // Handle last field/row
  if (fieldBuffer.trim() || rowBuffer.length > 0) {
    rowBuffer.push(fieldBuffer.trim());
    if (rowBuffer.some((f) => f !== "")) {
      rows.push([...rowBuffer]);
    }
  }

  if (rows.length === 0) {
    return { headers: [], rows: [] };
  }

  return {
    headers: rows[0],
    rows: rows.slice(1),
  };
}

/**
 * Stream-parse the CORE-MD CSV file and process each record
 * Uses streaming to avoid loading 186MB file entirely into memory
 */
async function processCoreMdCsv(outputCb, maxRecords = null) {
  console.log("=== CORE-MD: Processing Post-Market Surveillance CSV ===");

  if (!fs.existsSync(CSV_PATH)) {
    console.log("CSV not found locally. Downloading...");
    await ensureCsvDownloaded();
  }

  return new Promise((resolve, reject) => {
    const fileSize = fs.statSync(CSV_PATH).size;
    console.log(`File size: ${(fileSize / 1024 / 1024).toFixed(1)} MB`);

    const stream = fs.createReadStream(CSV_PATH, { encoding: "utf-8" });

    let chunk = "";
    let headers = null;
    // recordCount: tracked via processedCount
    let processedCount = 0;
    let hasMore = true;

    stream.on("data", (data) => {
      if (!hasMore) return;

      chunk += data;
      const lines = chunk.split("\n");
      chunk = lines.pop(); // Keep incomplete line for next chunk

      for (const line of lines) {
        if (!hasMore) break;

        if (!headers) {
          headers = parseCsv(line + "\n").headers;
          console.log(`CSV headers found: ${headers.length} columns`);
          console.log(`Headers: ${headers.slice(0, 15).join(", ")}${headers.length > 15 ? "..." : ""}`);
          continue;
        }

        // Parse this row
        const row = parseCsvRow(line);
        if (row && row.length > 0) {
          const normalized = normalizeCoreMdRecord(row, headers);
          processedCount++;

          if (outputCb) {
            outputCb(normalized).catch((err) =>
              console.error(`Error processing record ${processedCount}:`, err.message)
            );
          }

          if (processedCount % 10000 === 0) {
            const pct = ((stream.bytesRead / fileSize) * 100).toFixed(1);
            console.log(
              `Processed ${processedCount.toLocaleString()} records (${pct}% of file)`
            );
          }

          if (maxRecords && processedCount >= maxRecords) {
            hasMore = false;
            break;
          }
        }
      }
    });

    stream.on("end", () => {
      // Process remaining chunk
      if (chunk && headers) {
        const row = parseCsvRow(chunk);
        if (row && row.length > 0) {
          const normalized = normalizeCoreMdRecord(row, headers);
          if (outputCb) outputCb(normalized).catch(console.error);
          processedCount++;
        }
      }

      console.log(`Total records processed: ${processedCount.toLocaleString()}`);
      resolve(processedCount);
    });

    stream.on("error", (err) => {
      reject(err);
    });
  });
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

    if ((char === "," || char === ";" || char === "\t") && !inQuotes) {
      fields.push(current.trim());
      current = "";
      i++;
      continue;
    }

    current += char;
    i++;
  }

  fields.push(current.trim());

  if (fields.length === 0 || (fields.length === 1 && fields[0] === "")) {
    return null;
  }

  return fields;
}

/**
 * Get dataset metadata info
 */
async function getDatasetInfo() {
  const raw = await fetchHTML(ZENODO_API_URL, {
    headers: { Accept: "application/json" },
  });
  const data = JSON.parse(raw);

  return {
    title: data.metadata?.title,
    publicationDate: data.metadata?.publication_date,
    lastUpdated: data.updated,
    version: data.metadata?.version,
    description: data.metadata?.description?.replace(/<[^>]*>/g, "").substring(0, 500),
    fileSize: data.files?.[0]?.size,
    downloadUrl: data.files?.[0]?.links?.self,
  };
}

module.exports = {
  ensureCsvDownloaded,
  processCoreMdCsv,
  getDatasetInfo,
  parseCsv,
  parseCsvRow,
  normalizeCoreMdRecord,
  CSV_PATH,
};

if (require.main === module) {
  (async () => {
    const info = await getDatasetInfo();
    console.log("=== CORE-MD Dataset Info ===");
    console.log(`Title: ${info.title}`);
    console.log(`Published: ${info.publicationDate}`);
    console.log(`Updated: ${info.lastUpdated}`);
    console.log(`File size: ${info.fileSize ? (info.fileSize / 1024 / 1024).toFixed(1) + " MB" : "unknown"}`);
  })().catch(console.error);
}
