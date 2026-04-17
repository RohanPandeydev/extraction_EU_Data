require("dotenv").config();
const fs = require("fs");
const snowflake = require("snowflake-sdk");
snowflake.configure({ logLevel: "ERROR" });

const DB_NAME = process.env.SNOWFLAKE_DB || "GS_EUDAMED";

function getConnection() {
  return new Promise((resolve, reject) => {
    const conn = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USER,
      password: process.env.SNOWFLAKE_PASSWORD,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      role: process.env.SNOWFLAKE_ROLE,
    });
    conn.connect((err, c) => { if (err) return reject(err); resolve(c); });
  });
}

function query(conn, sql, binds = []) {
  return new Promise((resolve, reject) => {
    conn.execute({ sqlText: sql, binds, complete: (err, _, rows) => { if (err) return reject(err); resolve(rows); } });
  });
}

async function main() {
  const conn = await getConnection();
  await query(conn, `USE DATABASE ${DB_NAME}`);
  await query(conn, `USE SCHEMA MEDICAL_DEVICES`);

  // Find 10 devices with the richest data (most adverse events + clinical evidence + certs)
  const topDevices = await query(conn, `
    SELECT
      d.UUID, d.TRADE_NAME, d.DEVICE_NAME, d.RISK_CLASS, d.LEGISLATION,
      COALESCE((SELECT COUNT(*) FROM DEVICE_ADVERSE_EVENTS WHERE DEVICE_UUID = d.UUID), 0) AS AE_CNT,
      COALESCE((SELECT COUNT(*) FROM DEVICE_CLINICAL_EVIDENCE WHERE DEVICE_UUID = d.UUID), 0) AS CE_CNT,
      COALESCE((SELECT COUNT(*) FROM DEVICE_CERTIFICATES WHERE DEVICE_UUID = d.UUID), 0) AS CERT_CNT
    FROM DEVICES d
    ORDER BY (AE_CNT + CE_CNT + CERT_CNT) DESC
    LIMIT 10
  `);

  console.log("\n=== TOP 10 DEVICES (by data richness) ===\n");
  const exportData = [];

  for (const d of topDevices) {
    const uuid = d.UUID;
    const [deviceRow, adverseEvents, clinicalEvidence, certificates, authRep, relatedMeds] = await Promise.all([
      query(conn, `SELECT * FROM DEVICES WHERE UUID = ?`, [uuid]),
      query(conn, `SELECT * FROM DEVICE_ADVERSE_EVENTS WHERE DEVICE_UUID = ?`, [uuid]),
      query(conn, `SELECT * FROM DEVICE_CLINICAL_EVIDENCE WHERE DEVICE_UUID = ?`, [uuid]),
      query(conn, `SELECT * FROM DEVICE_CERTIFICATES WHERE DEVICE_UUID = ?`, [uuid]),
      query(conn, `SELECT * FROM AUTHORISED_REPRESENTATIVES WHERE DEVICE_UUID = ?`, [uuid]),
      query(conn, `SELECT * FROM DEVICE_RELATED_MEDICINES WHERE DEVICE_UUID = ?`, [uuid]),
    ]);

    const device = deviceRow[0] || {};
    const rawData = device.RAW_DATA ? JSON.parse(device.RAW_DATA) : {};

    // Per-device aggregates from Snowflake
    const aeSources = await query(conn, `SELECT SOURCE, COUNT(*) AS CNT FROM DEVICE_ADVERSE_EVENTS WHERE DEVICE_UUID = ? GROUP BY SOURCE`, [uuid]);
    const ceSources = await query(conn, `SELECT SOURCE, COUNT(*) AS CNT FROM DEVICE_CLINICAL_EVIDENCE WHERE DEVICE_UUID = ? GROUP BY SOURCE`, [uuid]);

    const record = {
      deviceInfo: {
        uuid, tradeName: device.TRADE_NAME, deviceName: device.DEVICE_NAME,
        deviceModel: device.DEVICE_MODEL, basicUdi: device.BASIC_UDI,
        primaryDi: device.PRIMARY_DI, reference: device.REFERENCE,
      },
      classification: {
        riskClass: device.RISK_CLASS, legislation: device.LEGISLATION,
        legacyDirective: device.LEGACY_DIRECTIVE, specialDeviceType: device.SPECIAL_DEVICE_TYPE,
        issuingAgency: device.ISSUING_AGENCY,
      },
      characteristics: {
        active: device.IS_ACTIVE, implantable: device.IS_IMPLANTABLE,
        reusable: device.IS_REUSABLE, sterile: device.IS_STERILE,
        measuringFunction: device.HAS_MEASURING_FUNCTION,
        administersMedicine: device.ADMINISTERS_MEDICINE,
        humanTissues: device.CONTAINS_HUMAN_TISSUES,
        animalTissues: device.CONTAINS_ANIMAL_TISSUES,
        kit: device.IS_KIT, reagent: device.IS_REAGENT,
        instrument: device.IS_INSTRUMENT,
        companionDiagnostic: device.IS_COMPANION_DIAGNOSTIC,
      },
      status: {
        deviceStatus: device.DEVICE_STATUS, versionState: device.VERSION_STATE,
        versionNumber: device.VERSION_NUMBER, latestVersion: device.LATEST_VERSION,
        lastUpdateDate: device.LAST_UPDATE_DATE,
      },
      manufacturer: rawData.manufacturer || null,
      authorisedRepresentative: authRep[0] || null,
      certificates: certificates.map(c => ({
        certificateNumber: c.CERTIFICATE_NUMBER, certificateType: c.CERTIFICATE_TYPE,
        issueDate: c.ISSUE_DATE, expiryDate: c.EXPIRY_DATE, status: c.STATUS,
        notifiedBody: c.NOTIFIED_BODY_NAME, source: c.SOURCE,
      })),
      adverseEvents: adverseEvents.map(a => ({
        source: a.SOURCE, title: a.TITLE, status: a.STATUS,
        date: a.EVENT_DATE, publicationDate: a.PUBLICATION_DATE,
        authors: a.AUTHORS, journal: a.JOURNAL, doi: a.DOI, url: a.URL,
        matchConfidence: a.MATCH_CONFIDENCE, matchType: a.MATCH_TYPE, matchedKeyword: a.MATCHED_KEYWORD,
      })),
      clinicalEvidence: clinicalEvidence.map(c => ({
        source: c.SOURCE, type: c.EVIDENCE_TYPE, title: c.TITLE,
        authors: c.AUTHORS, journal: c.JOURNAL,
        publicationDate: c.PUBLICATION_DATE, doi: c.DOI, url: c.URL,
        matchConfidence: c.MATCH_CONFIDENCE, matchType: c.MATCH_TYPE, matchedKeyword: c.MATCHED_KEYWORD,
      })),
      relatedMedicines: relatedMeds.map(m => ({
        name: m.MEDICINE_NAME, activeSubstance: m.ACTIVE_SUBSTANCE,
        status: m.STATUS, url: m.URL,
      })),
      dataCounts: {
        adverseEvents: d.AE_CNT,
        clinicalEvidence: d.CE_CNT,
        certificates: d.CERT_CNT,
        adverseEventsBySource: Object.fromEntries(aeSources.map(r => [r.SOURCE, r.CNT])),
        clinicalEvidenceBySource: Object.fromEntries(ceSources.map(r => [r.SOURCE, r.CNT])),
      },
    };

    exportData.push(record);

    console.log(`[${exportData.length}] ${(device.TRADE_NAME || device.DEVICE_NAME || "Unknown").substring(0, 60)}`);
    console.log(`    UUID: ${uuid}`);
    console.log(`    Risk: ${device.RISK_CLASS} | Leg: ${device.LEGISLATION}`);
    console.log(`    AE: ${d.AE_CNT} | CE: ${d.CE_CNT} | Certs: ${d.CERT_CNT}`);
  }

  // Write to JSON
  const outPath = "/Users/rohankumarpandey/Finn/asylum/extraction_EU_Data/output/ml-sample-10-devices.json";
  fs.mkdirSync("/Users/rohankumarpandey/Finn/asylum/extraction_EU_Data/output", { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(exportData, null, 2));

  console.log(`\n✅ Exported to: ${outPath}`);
  console.log(`   Size: ${(fs.statSync(outPath).size / 1024).toFixed(1)} KB`);

  conn.destroy(() => {});
}

main().catch((e) => { console.error("Error:", e.message); process.exit(1); });
