require("dotenv").config();
const fs = require("fs");
const path = require("path");

const OUTPUT_DIR = path.join(__dirname, "../output/unified");

/**
 * Normalize a manufacturer name for matching
 * Removes legal suffixes, punctuation, normalizes case
 */
function normalizeManufacturerName(name) {
  if (!name) return "";
  return name
    .toLowerCase()
    .replace(/\b(inc|ltd|llc|corp|gmbh|sa|nv|bv|ag|srl|spa|ab|oy|as)\b\.?/g, "")
    .replace(/[™®©]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Normalize a device/product name for matching
 */
function normalizeDeviceName(name) {
  if (!name) return "";
  return name
    .toLowerCase()
    .replace(/®|™|©/g, "")
    .replace(/(model|no\.?|cat\.?|ref|#)\s*/gi, "")
    .replace(/[^a-z0-9\s\-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Generate a match key from manufacturer + device name
 */
function createMatchKey(manufacturerName, deviceName) {
  const mfr = normalizeManufacturerName(manufacturerName);
  const dev = normalizeDeviceName(deviceName);
  if (!mfr && !dev) return null;
  return `${mfr}||${dev}`;
}

/**
 * Generate a manufacturer-only match key
 */
function createMfrMatchKey(manufacturerName) {
  const mfr = normalizeManufacturerName(manufacturerName);
  return mfr || null;
}

/**
 * Build a unified dataset from multiple sources
 *
 * Structure:
 * {
 *   device: { ...eudamed device info... },
 *   manufacturer: { ...eudamed manufacturer info... },
 *   adverseEvents: [ ...recalls/FSNs from CORE-MD, ICIJ, openFDA... ],
 *   certificates: [ ...NB certificates... ],
 *   clinicalEvidence: { ...clinical investigation flags... },
 *   dataFreshness: { ...when each source was last updated... }
 * }
 */
function createUnifiedRecord(device, adverseEvents = []) {
  return {
    // Device identity
    deviceInfo: {
      name: device.deviceName || device.tradeName || null,
      basicUdi: device.basicUdi || null,
      primaryDi: device.primaryDi || null,
      riskClass: device.riskClass || null,
      legislation: device.legislation || null,
      isLegacyDevice: device.isLegacyDevice || null,
      status: device.deviceStatus || null,
    },

    // Manufacturer identity
    manufacturerInfo: {
      name: device.manufacturerName || null,
      srn: device.manufacturerSrn || null,
      country: device.manufacturerCountry || device.manufacturerCountryName || null,
      address: device.manufacturerAddress || null,
      email: device.manufacturerEmail || null,
      status: device.manufacturerStatus || null,
    },

    // Clinical evidence
    clinicalEvidence: {
      clinicalInvestigationApplicable: device.clinicalInvestigationApplicable || null,
      clinicalInvestigationLinks: device.clinicalInvestigationLinks || [],
      linkedSscp: device.linkedSscp || null,
    },

    // Certificates
    certificates: device.certificates || [],

    // Adverse events from all sources
    adverseEvents: adverseEvents.map((ae) => ({
      source: ae.source || null,
      date: ae.date || ae.dateInitiated || ae.dateIssued || ae.datePosted || null,
      type: ae.recallType || ae.classification || null,
      deviceName: ae.deviceName || ae.productDescription || null,
      manufacturerName: ae.manufacturerName || ae.recallingFirm || null,
      country: ae.country || ae.city || ae.state || null,
      reason: ae.reason || ae.reasonForRecall || null,
      correctiveAction: ae.correctiveAction || ae.action || null,
      riskLevel: ae.riskLevel || ae.riskClass || null,
      sourceUrl: ae.sourceUrl || null,
    })),

    // Data freshness metadata
    dataFreshness: {
      deviceDataAsOf: device.lastUpdated || new Date().toISOString().split("T")[0],
      adverseEventDataAsOf: {
        coreMd: "2024-03-23",
        icij: "2019-11-25",
        openfda: "weekly (live)",
      },
    },
  };
}

/**
 * Match adverse events to devices by manufacturer name
 * Fuzzy matching — normalized comparison
 */
function matchAdverseEventsToDevice(devices, adverseEvents) {
  console.log(`\n=== Matching ${adverseEvents.length.toLocaleString()} adverse events to ${devices.length.toLocaleString()} devices ===`);

  // Build manufacturer lookup from devices
  const mfrToDeviceNames = new Map();
  for (const device of devices) {
    const mfrKey = createMfrMatchKey(device.manufacturerName);
    if (!mfrKey) continue;

    if (!mfrToDeviceNames.has(mfrKey)) {
      mfrToDeviceNames.set(mfrKey, {
        devices: [],
        manufacturerName: device.manufacturerName,
        srn: device.manufacturerSrn,
      });
    }
    const entry = mfrToDeviceNames.get(mfrKey);
    if (!entry.devices.some((d) => d.basicUdi === device.basicUdi)) {
      entry.devices.push(device);
    }
  }

  console.log(`Unique manufacturers in device dataset: ${mfrToDeviceNames.size.toLocaleString()}`);

  // Match adverse events
  const matched = [];
  const unmatched = [];
  const sourceCounts = {};

  for (const ae of adverseEvents) {
    const aeMfrKey = createMfrMatchKey(ae.manufacturerName || ae.recallingFirm || null);
    if (!aeMfrKey) {
      unmatched.push({ ae, reason: "no manufacturer name" });
      continue;
    }

    const deviceEntry = mfrToDeviceNames.get(aeMfrKey);
    if (deviceEntry) {
      matched.push({
        ae,
        devices: deviceEntry.devices,
        manufacturerName: deviceEntry.manufacturerName,
        srn: deviceEntry.srn,
      });
    } else {
      unmatched.push({ ae, reason: "manufacturer not found in EUDAMED" });
    }

    // Count by source
    const source = ae.source || "unknown";
    sourceCounts[source] = (sourceCounts[source] || 0) + 1;
  }

  console.log(`\nMatch results:`);
  console.log(`  ✅ Matched: ${matched.length.toLocaleString()}`);
  console.log(`  ❌ Unmatched: ${unmatched.length.toLocaleString()}`);
  console.log(`  Match rate: ${((matched.length / (matched.length + unmatched.length)) * 100).toFixed(1)}%`);
  console.log(`\nBy source:`);
  for (const [source, count] of Object.entries(sourceCounts)) {
    console.log(`  ${source}: ${count.toLocaleString()}`);
  }

  return { matched, unmatched, sourceCounts };
}

/**
 * Build unified records grouped by device
 */
function buildUnifiedRecords(devices, matchedAdverseEvents) {
  const deviceMap = new Map();

  // Index devices
  for (const device of devices) {
    const key = device.basicUdi || device.uuid || device.primaryDi;
    if (!key) continue;
    if (!deviceMap.has(key)) {
      deviceMap.set(key, {
        device,
        adverseEvents: [],
      });
    }
  }

  // Attach adverse events
  for (const match of matchedAdverseEvents) {
    for (const dev of match.devices) {
      const key = dev.basicUdi || dev.uuid || dev.primaryDi;
      if (deviceMap.has(key)) {
        deviceMap.get(key).adverseEvents.push(match.ae);
      }
    }
  }

  // Convert to unified records
  const records = [];
  for (const entry of Object.values(deviceMap)) {
    const record = createUnifiedRecord(entry.device, entry.adverseEvents);
    records.push(record);
  }

  // Sort by adverse event count (devices with most issues first)
  records.sort((a, b) => b.adverseEvents.length - a.adverseEvents.length);

  return records;
}

/**
 * Save unified records to JSON file
 */
async function saveUnifiedRecords(records, filename = "unified-dataset.json") {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const outputPath = path.join(OUTPUT_DIR, filename);

  const summary = generateSummary(records);

  // Save with summary header
  const output = {
    metadata: {
      generatedAt: new Date().toISOString(),
      totalDevices: summary.totalDevices,
      totalAdverseEvents: summary.totalAdverseEvents,
      devicesWithAdverseEvents: summary.devicesWithAdverseEvents,
      devicesWithoutAdverseEvents: summary.devicesWithoutAdverseEvents,
      topAffectedDevices: summary.topAffectedDevices,
      adverseEventsBySource: summary.adverseEventsBySource,
      adverseEventsByCountry: summary.adverseEventsByCountry,
      dataFreshness: {
        eudamed: "live (2026-04-13)",
        coreMd: "2024-03-23 (frozen)",
        icij: "2019-11-25 (frozen)",
        openfda: "live (weekly)",
      },
    },
    records: records,
  };

  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
  console.log(`\n✅ Saved unified dataset to: ${outputPath}`);
  console.log(`   File size: ${(fs.statSync(outputPath).size / 1024 / 1024).toFixed(1)} MB`);
}

/**
 * Generate summary statistics
 */
function generateSummary(records) {
  const topAffectedDevices = [];
  const adverseEventsBySource = {};
  const adverseEventsByCountry = {};
  let totalAdverseEvents = 0;
  let devicesWithAdverseEvents = 0;

  for (const record of records) {
    if (record.adverseEvents.length > 0) {
      devicesWithAdverseEvents++;
      topAffectedDevices.push({
        deviceName: record.deviceInfo.name,
        manufacturer: record.manufacturerInfo.name,
        srn: record.manufacturerInfo.srn,
        adverseEventCount: record.adverseEvents.length,
      });
    }

    totalAdverseEvents += record.adverseEvents.length;

    for (const ae of record.adverseEvents) {
      adverseEventsBySource[ae.source] = (adverseEventsBySource[ae.source] || 0) + 1;
      if (ae.country) {
        adverseEventsByCountry[ae.country] = (adverseEventsByCountry[ae.country] || 0) + 1;
      }
    }
  }

  // Sort top affected
  topAffectedDevices.sort((a, b) => b.adverseEventCount - a.adverseEventCount);

  // Sort countries
  const sortedCountries = Object.entries(adverseEventsByCountry)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([country, count]) => ({ country, count }));

  return {
    totalDevices: records.length,
    totalAdverseEvents,
    devicesWithAdverseEvents,
    devicesWithoutAdverseEvents: records.length - devicesWithAdverseEvents,
    topAffectedDevices: topAffectedDevices.slice(0, 20),
    adverseEventsBySource,
    adverseEventsByCountry: sortedCountries,
  };
}

module.exports = {
  normalizeManufacturerName,
  normalizeDeviceName,
  createMatchKey,
  createMfrMatchKey,
  createUnifiedRecord,
  matchAdverseEventsToDevice,
  buildUnifiedRecords,
  saveUnifiedRecords,
  generateSummary,
  OUTPUT_DIR,
};

if (require.main === module) {
  // Test matching logic with sample data
  const testDevices = [
    { manufacturerName: "Medtronic Inc", basicUdi: "ABC123", tradeName: "Pacemaker X", riskClass: "class-iii" },
    { manufacturerName: "B. Braun Melsungen AG", basicUdi: "DEF456", tradeName: "Infusion Pump", riskClass: "class-iia" },
  ];

  const testAdverseEvents = [
    { manufacturerName: "Medtronic", deviceName: "Pacemaker", source: "CORE-MD", reason: "Battery failure" },
    { manufacturerName: "MEDTRONIC INC.", source: "ICIJ", reason: "Software bug" },
    { manufacturerName: "Unknown Company", source: "openFDA", reason: "Contamination" },
  ];

  const result = matchAdverseEventsToDevice(testDevices, testAdverseEvents);
  console.log("\nMatch details:");
  for (const m of result.matched) {
    console.log(`  ✅ "${m.ae.source}" → ${m.manufacturerName} (${m.devices.length} devices)`);
  }
  for (const u of result.unmatched) {
    console.log(`  ❌ "${u.ae.source}" (${u.ae.manufacturerName || u.ae.recallingFirm}) → ${u.reason}`);
  }
}
