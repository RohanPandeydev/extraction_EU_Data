require("dotenv").config();
const snowflake = require("snowflake-sdk");
snowflake.configure({ logLevel: "ERROR" });

let connection = null;
const DB_NAME = process.env.SNOWFLAKE_DB || "GS_EUDAMED";

function getConnection() {
  return new Promise((resolve, reject) => {
    if (connection) return resolve(connection);
    const conn = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USER,
      password: process.env.SNOWFLAKE_PASSWORD,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      role: process.env.SNOWFLAKE_ROLE,
      database: DB_NAME,
      schema: "MEDICAL_DEVICES",
    });
    conn.connect((err, conn) => {
      if (err) {
        console.error("Snowflake connection error:", err.message);
        return reject(err);
      }
      connection = conn;
      console.log("Snowflake connected successfully");
      resolve(conn);
    });
  });
}

// Snowflake SDK throws on undefined — convert to null
// Also stringify objects/arrays to avoid unsupported VARIANT type errors
function sanitizeBinds(binds) {
  return binds.map((v) => {
    if (v === undefined) return null;
    if (v !== null && typeof v === "object" && !(v instanceof Date)) {
      return JSON.stringify(v);
    }
    return v;
  });
}

function executeSQL(sql, binds = []) {
  return new Promise(async (resolve, reject) => {
    const conn = await getConnection();
    conn.execute({
      sqlText: sql,
      binds: sanitizeBinds(binds),
      complete: (err, stmt, rows) => {
        if (err) return reject(err);
        resolve(rows);
      },
    });
  });
}

let dbContextSet = false;
async function useDB() {
  if (dbContextSet) return;
  await executeSQL(`USE DATABASE ${DB_NAME}`);
  await executeSQL("USE SCHEMA MEDICAL_DEVICES");
  dbContextSet = true;
}

// Generic batched MERGE: dedupes by keyCols (keep last), chunks, runs 1 MERGE per chunk.
// - table: target table name
// - keyCols: array of column names used in ON clause and for dedupe
// - allCols: array of all column names in source (order defines bind order)
// - updateCols: subset of allCols to set on MATCH (exclude keys)
// - extraUpdateSet: optional raw SQL appended to UPDATE SET (e.g. "UPDATED_AT = CURRENT_TIMESTAMP()")
// - rows: array of objects, keyed by column names (values can be scalars, Dates, or objects — objects are JSON.stringified)
async function bulkMerge({ table, keyCols, allCols, updateCols, extraUpdateSet = "", rows }) {
  if (!rows || rows.length === 0) return;

  // Dedupe by composite key (last occurrence wins)
  const seen = new Map();
  for (const row of rows) {
    if (!row) continue;
    const key = keyCols.map(k => (row[k] == null ? "" : String(row[k]))).join("\u0001");
    seen.set(key, row);
  }
  const deduped = [...seen.values()];
  if (deduped.length === 0) return;

  await useDB();

  // Snowflake bind limit is ~16k per statement; keep comfortable margin
  const MAX_BINDS = 15000;
  const chunkSize = Math.max(1, Math.min(200, Math.floor(MAX_BINDS / allCols.length)));

  const placeholder = `(${allCols.map(() => "?").join(",")})`;
  const colList = allCols.join(", ");
  const onClause = keyCols.map(k => `t.${k} = s.${k}`).join(" AND ");
  const updateSet = [
    ...updateCols.map(c => `${c} = s.${c}`),
    ...(extraUpdateSet ? [extraUpdateSet] : []),
  ].join(", ");
  const insertVals = allCols.map(c => `s.${c}`).join(", ");

  for (let i = 0; i < deduped.length; i += chunkSize) {
    const chunk = deduped.slice(i, i + chunkSize);
    const values = chunk.map(() => placeholder).join(", ");
    const sql = `
      MERGE INTO ${table} AS t
      USING (SELECT * FROM (VALUES ${values}) AS v(${colList})) AS s
      ON ${onClause}
      WHEN MATCHED THEN UPDATE SET ${updateSet}
      WHEN NOT MATCHED THEN INSERT (${colList}) VALUES (${insertVals})
    `;
    const binds = chunk.flatMap(r => allCols.map(c => r[c] === undefined ? null : r[c]));
    await executeSQL(sql, binds);
  }
}

async function setupDatabase() {
  console.log("Setting up Snowflake database...");

  await executeSQL(`CREATE DATABASE IF NOT EXISTS ${DB_NAME}`);
  await executeSQL(`USE DATABASE ${DB_NAME}`);
  await executeSQL("CREATE SCHEMA IF NOT EXISTS MEDICAL_DEVICES");
  await executeSQL("USE SCHEMA MEDICAL_DEVICES");

  // Drop deprecated tables (pruned — not useful for ML)
  await executeSQL(`DROP TABLE IF EXISTS DEVICE_RELATED_MEDICINES`);
  await executeSQL(`DROP TABLE IF EXISTS EMA_MEDICINES`);
  await executeSQL(`DROP TABLE IF EXISTS COCHRANE_REVIEWS`);
  await executeSQL(`DROP TABLE IF EXISTS OPENFDA_510K`);
  await executeSQL(`DROP TABLE IF EXISTS OPENFDA_MAUDE`);

  // === DEVICES (main table — flat, queryable columns) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS DEVICES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      UUID VARCHAR(255) NOT NULL UNIQUE,
      ULID VARCHAR(50),
      BASIC_UDI VARCHAR(500),
      PRIMARY_DI VARCHAR(500),
      REFERENCE VARCHAR(500),
      TRADE_NAME VARCHAR(1000),
      DEVICE_NAME VARCHAR(1000),
      DEVICE_MODEL VARCHAR(1000),
      DEVICE_CRITERION VARCHAR(50),
      RISK_CLASS VARCHAR(50),
      RISK_CLASS_CODE VARCHAR(255),
      LEGISLATION VARCHAR(50),
      LEGISLATION_CODE VARCHAR(255),
      LEGACY_DIRECTIVE BOOLEAN DEFAULT FALSE,
      SPECIAL_DEVICE_TYPE VARCHAR(100),
      ISSUING_AGENCY VARCHAR(100),
      CONTAINER_PACKAGE_COUNT NUMBER,
      IS_ACTIVE BOOLEAN DEFAULT FALSE,
      IS_IMPLANTABLE BOOLEAN DEFAULT FALSE,
      IS_REUSABLE BOOLEAN DEFAULT FALSE,
      IS_STERILE BOOLEAN DEFAULT FALSE,
      HAS_MEASURING_FUNCTION BOOLEAN DEFAULT FALSE,
      ADMINISTERS_MEDICINE BOOLEAN DEFAULT FALSE,
      IS_MULTI_COMPONENT BOOLEAN DEFAULT FALSE,
      CONTAINS_HUMAN_TISSUES BOOLEAN DEFAULT FALSE,
      CONTAINS_ANIMAL_TISSUES BOOLEAN DEFAULT FALSE,
      CONTAINS_HUMAN_PRODUCT BOOLEAN DEFAULT FALSE,
      CONTAINS_MEDICINAL_PRODUCT BOOLEAN DEFAULT FALSE,
      IS_KIT BOOLEAN DEFAULT FALSE,
      IS_REAGENT BOOLEAN DEFAULT FALSE,
      IS_INSTRUMENT BOOLEAN DEFAULT FALSE,
      IS_COMPANION_DIAGNOSTIC BOOLEAN DEFAULT FALSE,
      IS_SELF_TESTING BOOLEAN DEFAULT FALSE,
      IS_NEAR_PATIENT_TESTING BOOLEAN DEFAULT FALSE,
      IS_PROFESSIONAL_TESTING BOOLEAN DEFAULT FALSE,
      DEVICE_STATUS VARCHAR(100),
      VERSION_STATE VARCHAR(100),
      LATEST_VERSION BOOLEAN,
      VERSION_NUMBER NUMBER,
      VERSION_DATE VARCHAR(255),
      LAST_UPDATE_DATE VARCHAR(255),
      DISCARDED_DATE VARCHAR(255),
      IS_NEW BOOLEAN DEFAULT FALSE,
      CLINICAL_INVESTIGATION_APPLICABLE BOOLEAN DEFAULT FALSE,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === MANUFACTURERS ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS MANUFACTURERS (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      UUID VARCHAR(255) UNIQUE,
      SRN VARCHAR(100),
      NAME VARCHAR(1000),
      STATUS VARCHAR(100),
      COUNTRY_ISO2 VARCHAR(10),
      COUNTRY_NAME VARCHAR(255),
      COUNTRY_TYPE VARCHAR(50),
      ADDRESS TEXT,
      EMAIL VARCHAR(500),
      PHONE VARCHAR(100),
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === AUTHORISED REPRESENTATIVES ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS AUTHORISED_REPRESENTATIVES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      DEVICE_UUID VARCHAR(255),
      NAME VARCHAR(1000),
      SRN VARCHAR(100),
      ADDRESS TEXT,
      COUNTRY_NAME VARCHAR(255),
      EMAIL VARCHAR(500),
      PHONE VARCHAR(100),
      MANDATE_START_DATE VARCHAR(255),
      MANDATE_END_DATE VARCHAR(255),
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === CERTIFICATES (per device) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS DEVICE_CERTIFICATES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      DEVICE_UUID VARCHAR(255),
      CERTIFICATE_UUID VARCHAR(255),
      CERTIFICATE_NUMBER VARCHAR(500),
      CERTIFICATE_TYPE VARCHAR(255),
      ISSUE_DATE VARCHAR(255),
      EXPIRY_DATE VARCHAR(255),
      STARTING_VALIDITY_DATE VARCHAR(255),
      STATUS VARCHAR(255),
      NOTIFIED_BODY_NAME VARCHAR(500),
      NOTIFIED_BODY_SRN VARCHAR(100),
      NOTIFIED_BODY_COUNTRY VARCHAR(10),
      REVISION VARCHAR(100),
      SOURCE VARCHAR(100) DEFAULT 'EUDAMED',
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === ADVERSE EVENTS (per device) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS DEVICE_ADVERSE_EVENTS (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      DEVICE_UUID VARCHAR(255),
      DEVICE_NAME VARCHAR(1000),
      SOURCE VARCHAR(255),
      TITLE TEXT,
      AUTHORS TEXT,
      JOURNAL VARCHAR(1000),
      PUBLICATION_DATE VARCHAR(255),
      DOI VARCHAR(500),
      URL TEXT,
      STATUS VARCHAR(255),
      EVENT_DATE VARCHAR(255),
      MATCH_CONFIDENCE FLOAT,
      MATCH_TYPE VARCHAR(100),
      MATCHED_KEYWORD VARCHAR(1000),
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);
  await executeSQL(`ALTER TABLE DEVICE_ADVERSE_EVENTS ADD COLUMN IF NOT EXISTS MATCH_CONFIDENCE FLOAT`);
  await executeSQL(`ALTER TABLE DEVICE_ADVERSE_EVENTS ADD COLUMN IF NOT EXISTS MATCH_TYPE VARCHAR(100)`);
  await executeSQL(`ALTER TABLE DEVICE_ADVERSE_EVENTS ADD COLUMN IF NOT EXISTS MATCHED_KEYWORD VARCHAR(1000)`);

  // === CLINICAL EVIDENCE (per device) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS DEVICE_CLINICAL_EVIDENCE (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      DEVICE_UUID VARCHAR(255),
      DEVICE_NAME VARCHAR(1000),
      SOURCE VARCHAR(255),
      EVIDENCE_TYPE VARCHAR(255),
      TITLE TEXT,
      AUTHORS TEXT,
      JOURNAL VARCHAR(1000),
      PUBLICATION_DATE VARCHAR(255),
      DOI VARCHAR(500),
      URL TEXT,
      MATCH_CONFIDENCE FLOAT,
      MATCH_TYPE VARCHAR(100),
      MATCHED_KEYWORD VARCHAR(1000),
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);
  await executeSQL(`ALTER TABLE DEVICE_CLINICAL_EVIDENCE ADD COLUMN IF NOT EXISTS MATCH_CONFIDENCE FLOAT`);
  await executeSQL(`ALTER TABLE DEVICE_CLINICAL_EVIDENCE ADD COLUMN IF NOT EXISTS MATCH_TYPE VARCHAR(100)`);
  await executeSQL(`ALTER TABLE DEVICE_CLINICAL_EVIDENCE ADD COLUMN IF NOT EXISTS MATCHED_KEYWORD VARCHAR(1000)`);

  // === NOTIFIED BODIES ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS NOTIFIED_BODIES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      UUID VARCHAR(255) UNIQUE,
      NAME VARCHAR(500),
      IDENTIFIER VARCHAR(50),
      MDR_STATUS VARCHAR(255),
      IVDR_STATUS VARCHAR(255),
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === REFUSED APPLICATIONS ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS REFUSED_APPLICATIONS (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      UUID VARCHAR(255) UNIQUE,
      ACTOR_SRN VARCHAR(100),
      ACTOR_NAME VARCHAR(500),
      NOTIFIED_BODY_SRN VARCHAR(50),
      APPLICATION_REFERENCE VARCHAR(500),
      CONFORMITY_PROCEDURE VARCHAR(500),
      DECISION VARCHAR(255),
      DECISION_DATE VARCHAR(255),
      LAST_UPDATE_DATE VARCHAR(255),
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === SAFETY NOTICES (ANSM, SCHEER, openFDA recalls etc.) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS SAFETY_NOTICES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      SOURCE VARCHAR(100),
      SOURCE_ID VARCHAR(500),
      TITLE TEXT,
      DEVICE_NAME VARCHAR(1000),
      DEVICE_TYPE VARCHAR(255),
      STATUS VARCHAR(255),
      NOTICE_DATE VARCHAR(255),
      RETURN_DATE VARCHAR(255),
      TOPIC TEXT,
      URL TEXT,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);
  // Migrate existing TOPIC column in case it's still VARCHAR from old schema
  await executeSQL(`ALTER TABLE SAFETY_NOTICES ALTER COLUMN TOPIC SET DATA TYPE TEXT`).catch(() => {});

  // === CLINICALTRIALS.GOV STUDIES ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS CLINICAL_TRIALS (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      NCT_ID VARCHAR(50) UNIQUE,
      TITLE TEXT,
      OFFICIAL_TITLE TEXT,
      BRIEF_SUMMARY TEXT,
      CONDITION VARCHAR(2000),
      INTERVENTION_TYPE VARCHAR(100),
      INTERVENTION_NAME VARCHAR(2000),
      SPONSOR VARCHAR(1000),
      PHASE VARCHAR(100),
      STATUS VARCHAR(100),
      STUDY_TYPE VARCHAR(100),
      PRIMARY_OUTCOME TEXT,
      ENROLLMENT NUMBER,
      START_DATE VARCHAR(100),
      COMPLETION_DATE VARCHAR(100),
      COUNTRY VARCHAR(500),
      URL TEXT,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === MHRA DEVICE ALERTS (UK — with rich narrative + derived quality fields) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS MHRA_ALERTS (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      ALERT_ID VARCHAR(1000) UNIQUE,
      BRAND VARCHAR(500),
      MANUFACTURER_SRN VARCHAR(100),
      MANUFACTURER_NAME VARCHAR(1000),
      MANUFACTURER_COUNTRY VARCHAR(255),
      TITLE TEXT,
      DESCRIPTION TEXT,
      FULL_CONTENT TEXT,
      PUBLISHED_DATE VARCHAR(100),
      ALERT_TYPE VARCHAR(100),
      URL TEXT,
      PROBLEM_CATEGORIES TEXT,
      MENTIONS_PATIENT_HARM BOOLEAN,
      MENTIONS_RECALL BOOLEAN,
      MENTIONS_SOFTWARE BOOLEAN,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === EU MANUFACTURER REGISTRY (from EUDAMED /api/eos) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS EU_MANUFACTURERS (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      SRN VARCHAR(100) UNIQUE,
      UUID VARCHAR(255),
      NAME VARCHAR(1000),
      ABBREVIATED_NAME VARCHAR(500),
      BRAND VARCHAR(500),
      ACTOR_TYPE VARCHAR(100),
      STATUS VARCHAR(100),
      COUNTRY_ISO2 VARCHAR(10),
      COUNTRY_NAME VARCHAR(255),
      COUNTRY_TYPE VARCHAR(50),
      GEOGRAPHICAL_ADDRESS TEXT,
      CITY_NAME VARCHAR(255),
      POSTAL_ZONE VARCHAR(50),
      ELECTRONIC_MAIL VARCHAR(500),
      TELEPHONE VARCHAR(100),
      DATE_OF_REGISTRATION VARCHAR(100),
      VERSION_NUMBER NUMBER,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === EUROPE PMC ARTICLES ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS EUROPE_PMC_ARTICLES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      PMID VARCHAR(50),
      PMCID VARCHAR(50),
      DOI VARCHAR(500),
      TITLE TEXT,
      ABSTRACT TEXT,
      AUTHORS TEXT,
      JOURNAL VARCHAR(1000),
      PUBLICATION_DATE VARCHAR(100),
      SEARCH_TERM VARCHAR(500),
      HAS_FULLTEXT BOOLEAN,
      URL TEXT,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);
  // Clustering keys — critical for MERGE performance.
  // Without these, each single-key MERGE scans the full table (~5s/query on DEVICES).
  // With clustering, Snowflake prunes to the relevant micro-partitions (~50ms/query).
  const clusterKeys = [
    ["DEVICES", "UUID"],
    ["DEVICE_ADVERSE_EVENTS", "DEVICE_UUID"],
    ["DEVICE_CLINICAL_EVIDENCE", "DEVICE_UUID"],
    ["DEVICE_CERTIFICATES", "DEVICE_UUID"],
    ["MANUFACTURERS", "UUID"],
    ["AUTHORISED_REPRESENTATIVES", "DEVICE_UUID"],
    ["SAFETY_NOTICES", "SOURCE_ID"],
    ["NOTIFIED_BODIES", "UUID"],
    ["REFUSED_APPLICATIONS", "UUID"],
    ["MHRA_ALERTS", "ALERT_ID"],
    ["EU_MANUFACTURERS", "SRN"],
    ["CLINICAL_TRIALS", "NCT_ID"],
    ["EUROPE_PMC_ARTICLES", "COALESCE(PMID, PMCID)"],
  ];
  for (const [table, key] of clusterKeys) {
    await executeSQL(`ALTER TABLE ${table} CLUSTER BY (${key})`).catch(() => {});
  }

  console.log("Snowflake database setup complete — all tables created");
}

// === INSERT FUNCTIONS ===

// Column definitions for batched merges
const DEVICES_COLS = [
  "UUID","ULID","BASIC_UDI","PRIMARY_DI","REFERENCE",
  "TRADE_NAME","DEVICE_NAME","DEVICE_MODEL","DEVICE_CRITERION",
  "RISK_CLASS","RISK_CLASS_CODE","LEGISLATION","LEGISLATION_CODE","LEGACY_DIRECTIVE",
  "SPECIAL_DEVICE_TYPE","ISSUING_AGENCY","CONTAINER_PACKAGE_COUNT",
  "IS_ACTIVE","IS_IMPLANTABLE","IS_REUSABLE","IS_STERILE",
  "HAS_MEASURING_FUNCTION","ADMINISTERS_MEDICINE","IS_MULTI_COMPONENT",
  "CONTAINS_HUMAN_TISSUES","CONTAINS_ANIMAL_TISSUES","CONTAINS_HUMAN_PRODUCT","CONTAINS_MEDICINAL_PRODUCT",
  "IS_KIT","IS_REAGENT","IS_INSTRUMENT","IS_COMPANION_DIAGNOSTIC",
  "IS_SELF_TESTING","IS_NEAR_PATIENT_TESTING","IS_PROFESSIONAL_TESTING",
  "DEVICE_STATUS","VERSION_STATE","LATEST_VERSION","VERSION_NUMBER",
  "VERSION_DATE","LAST_UPDATE_DATE","DISCARDED_DATE","IS_NEW",
  "CLINICAL_INVESTIGATION_APPLICABLE","RAW_DATA",
];

function deviceRow(deviceJSON) {
  const uuid = deviceJSON.identity?.uuid;
  if (!uuid) return null;
  return {
    UUID: uuid,
    ULID: deviceJSON.identity?.ulid,
    BASIC_UDI: deviceJSON.identity?.basicUdi,
    PRIMARY_DI: deviceJSON.identity?.primaryDi,
    REFERENCE: deviceJSON.identity?.reference,
    TRADE_NAME: deviceJSON.identity?.tradeName,
    DEVICE_NAME: deviceJSON.identity?.deviceName,
    DEVICE_MODEL: deviceJSON.identity?.deviceModel,
    DEVICE_CRITERION: deviceJSON.identity?.deviceCriterion,
    RISK_CLASS: deviceJSON.classification?.riskClass,
    RISK_CLASS_CODE: deviceJSON.classification?.riskClassCode,
    LEGISLATION: deviceJSON.classification?.legislation,
    LEGISLATION_CODE: deviceJSON.classification?.legislationCode,
    LEGACY_DIRECTIVE: deviceJSON.classification?.legacyDirective || false,
    SPECIAL_DEVICE_TYPE: deviceJSON.classification?.specialDeviceType,
    ISSUING_AGENCY: deviceJSON.classification?.issuingAgency,
    CONTAINER_PACKAGE_COUNT: deviceJSON.classification?.containerPackageCount || 0,
    IS_ACTIVE: deviceJSON.characteristics?.active || false,
    IS_IMPLANTABLE: deviceJSON.characteristics?.implantable || false,
    IS_REUSABLE: deviceJSON.characteristics?.reusable || false,
    IS_STERILE: deviceJSON.characteristics?.sterile || false,
    HAS_MEASURING_FUNCTION: deviceJSON.characteristics?.measuringFunction || false,
    ADMINISTERS_MEDICINE: deviceJSON.characteristics?.administeringMedicine || false,
    IS_MULTI_COMPONENT: Boolean(deviceJSON.characteristics?.multiComponent && typeof deviceJSON.characteristics?.multiComponent !== "object"),
    CONTAINS_HUMAN_TISSUES: deviceJSON.characteristics?.humanTissues || false,
    CONTAINS_ANIMAL_TISSUES: deviceJSON.characteristics?.animalTissues || false,
    CONTAINS_HUMAN_PRODUCT: deviceJSON.characteristics?.humanProduct || false,
    CONTAINS_MEDICINAL_PRODUCT: deviceJSON.characteristics?.medicinalProduct || false,
    IS_KIT: deviceJSON.characteristics?.kit || false,
    IS_REAGENT: deviceJSON.characteristics?.reagent || false,
    IS_INSTRUMENT: deviceJSON.characteristics?.instrument || false,
    IS_COMPANION_DIAGNOSTIC: deviceJSON.characteristics?.companionDiagnostics || false,
    IS_SELF_TESTING: deviceJSON.characteristics?.selfTesting || false,
    IS_NEAR_PATIENT_TESTING: deviceJSON.characteristics?.nearPatientTesting || false,
    IS_PROFESSIONAL_TESTING: deviceJSON.characteristics?.professionalTesting || false,
    DEVICE_STATUS: deviceJSON.status?.deviceStatus,
    VERSION_STATE: deviceJSON.status?.versionState,
    LATEST_VERSION: deviceJSON.status?.latestVersion,
    VERSION_NUMBER: deviceJSON.status?.versionNumber,
    VERSION_DATE: deviceJSON.status?.versionDate,
    LAST_UPDATE_DATE: deviceJSON.status?.lastUpdateDate,
    DISCARDED_DATE: deviceJSON.status?.discardedDate,
    IS_NEW: deviceJSON.status?.isNew || false,
    CLINICAL_INVESTIGATION_APPLICABLE: deviceJSON.clinicalInvestigation?.applicable || false,
    RAW_DATA: JSON.stringify(deviceJSON),
  };
}

function manufacturerRows(deviceJSONs) {
  const out = [];
  for (const d of deviceJSONs) {
    const mfr = d.manufacturer;
    if (!mfr || !(mfr.uuid || mfr.srn || mfr.name)) continue;
    const mfrKey = mfr.uuid || mfr.srn || `name:${mfr.name}`;
    out.push({
      UUID: mfrKey, SRN: mfr.srn, NAME: mfr.name, STATUS: mfr.status,
      COUNTRY_ISO2: mfr.countryIso2Code, COUNTRY_NAME: mfr.countryName, COUNTRY_TYPE: mfr.countryType,
      ADDRESS: mfr.address, EMAIL: mfr.email, PHONE: mfr.phone,
    });
  }
  return out;
}

function arRows(deviceJSONs) {
  const out = [];
  for (const d of deviceJSONs) {
    const ar = d.authorisedRepresentative;
    const uuid = d.identity?.uuid;
    if (!ar?.name || !uuid) continue;
    out.push({
      DEVICE_UUID: uuid, NAME: ar.name, SRN: ar.srn, ADDRESS: ar.address,
      COUNTRY_NAME: ar.countryName, EMAIL: ar.email, PHONE: ar.phone,
      MANDATE_START_DATE: ar.mandateStartDate, MANDATE_END_DATE: ar.mandateEndDate,
    });
  }
  return out;
}

function certRows(deviceJSONs) {
  const primary = [];
  const mfrCerts = [];
  for (const d of deviceJSONs) {
    const uuid = d.identity?.uuid;
    if (!uuid) continue;
    for (const cert of d.certificates || []) {
      primary.push({
        DEVICE_UUID: uuid, CERTIFICATE_UUID: cert.uuid,
        CERTIFICATE_NUMBER: cert.certificateNumber, CERTIFICATE_TYPE: cert.certificateType,
        ISSUE_DATE: cert.issueDate, EXPIRY_DATE: cert.expiryDate,
        STARTING_VALIDITY_DATE: cert.startingValidityDate, STATUS: cert.status,
        NOTIFIED_BODY_NAME: cert.notifiedBody?.name, NOTIFIED_BODY_SRN: cert.notifiedBody?.srn,
        NOTIFIED_BODY_COUNTRY: cert.notifiedBody?.countryIso2Code, REVISION: cert.revision,
        SOURCE: "EUDAMED",
      });
    }
    for (const cert of d.manufacturerCertificates || []) {
      mfrCerts.push({
        DEVICE_UUID: uuid, CERTIFICATE_NUMBER: cert.certificateNumber,
        CERTIFICATE_TYPE: cert.certificateType, ISSUE_DATE: cert.issueDate,
        EXPIRY_DATE: cert.expiryDate, STATUS: cert.status,
        NOTIFIED_BODY_SRN: cert.notifiedBodySrn, REVISION: cert.revision,
        SOURCE: "EUDAMED_MFR",
      });
    }
  }
  return { primary, mfrCerts };
}

function adverseRows(deviceJSONs) {
  const out = [];
  for (const d of deviceJSONs) {
    const uuid = d.identity?.uuid;
    if (!uuid) continue;
    const deviceName = d.identity?.tradeName || d.identity?.deviceName;
    for (const ae of d.adverseEvents || []) {
      if (!ae?.title) continue;
      out.push({
        DEVICE_UUID: uuid, DEVICE_NAME: deviceName, SOURCE: ae.source, TITLE: ae.title,
        AUTHORS: ae.authors, JOURNAL: ae.journal,
        PUBLICATION_DATE: ae.publicationDate || ae.date, DOI: ae.doi, URL: ae.url,
        STATUS: ae.status || ae.type || ae.source,
        EVENT_DATE: ae.date || ae.publicationDate,
        MATCH_CONFIDENCE: ae.matchConfidence ?? null,
        MATCH_TYPE: ae.matchType ?? null,
        MATCHED_KEYWORD: ae.matchedKeyword ?? null,
      });
    }
  }
  return out;
}

function evidenceRows(deviceJSONs) {
  const out = [];
  for (const d of deviceJSONs) {
    const uuid = d.identity?.uuid;
    if (!uuid) continue;
    const deviceName = d.identity?.tradeName || d.identity?.deviceName;
    for (const ce of d.clinicalEvidence || []) {
      if (!ce?.title) continue;
      out.push({
        DEVICE_UUID: uuid, DEVICE_NAME: deviceName, SOURCE: ce.source,
        EVIDENCE_TYPE: ce.type, TITLE: ce.title, AUTHORS: ce.authors,
        JOURNAL: ce.journal, PUBLICATION_DATE: ce.publicationDate,
        DOI: ce.doi, URL: ce.url,
        MATCH_CONFIDENCE: ce.matchConfidence ?? null,
        MATCH_TYPE: ce.matchType ?? null,
        MATCHED_KEYWORD: ce.matchedKeyword ?? null,
      });
    }
  }
  return out;
}

// Cache of UUID -> LAST_UPDATE_DATE already in Snowflake at start of this process.
// Populated lazily on first insertDevicesBatch call; used to skip unchanged devices
// (huge cost saver when the same EUDAMED data is re-loaded on every CI run).
let existingDeviceDates = null;
async function loadExistingDeviceDates() {
  if (existingDeviceDates) return existingDeviceDates;
  await useDB();
  existingDeviceDates = new Map();
  try {
    const rows = await executeSQL("SELECT UUID, LAST_UPDATE_DATE FROM DEVICES");
    for (const r of rows || []) existingDeviceDates.set(r.UUID, r.LAST_UPDATE_DATE);
  } catch (e) {
    // Table may not exist yet on first-ever run — treat as empty
    console.log(`[snowflake] skip-cache init: ${e.message}`);
  }
  return existingDeviceDates;
}
// Force-refresh or bypass (for scripts that need to write regardless of freshness)
function resetDeviceDateCache() { existingDeviceDates = null; }

async function insertDevicesBatch(deviceJSONs) {
  if (!deviceJSONs || deviceJSONs.length === 0) return;

  // Skip devices whose LAST_UPDATE_DATE already matches Snowflake.
  // Children are derived from the device JSON — if the device didn't change, its children didn't either.
  const cache = await loadExistingDeviceDates();
  const changed = deviceJSONs.filter(d => {
    const uuid = d?.identity?.uuid;
    if (!uuid) return false;
    const incoming = d.status?.lastUpdateDate || null;
    const existing = cache.get(uuid);
    if (existing !== undefined && existing === incoming) return false; // unchanged
    // Record the new date so a later duplicate within this run also skips
    cache.set(uuid, incoming);
    return true;
  });
  if (changed.length === 0) return;

  const deviceRows = changed.map(deviceRow).filter(Boolean);
  await bulkMerge({
    table: "DEVICES",
    keyCols: ["UUID"],
    allCols: DEVICES_COLS,
    updateCols: DEVICES_COLS.filter(c => c !== "UUID"),
    extraUpdateSet: "UPDATED_AT = CURRENT_TIMESTAMP()",
    rows: deviceRows,
  });

  await bulkMerge({
    table: "MANUFACTURERS",
    keyCols: ["UUID"],
    allCols: ["UUID","SRN","NAME","STATUS","COUNTRY_ISO2","COUNTRY_NAME","COUNTRY_TYPE","ADDRESS","EMAIL","PHONE"],
    updateCols: ["SRN","NAME","STATUS","COUNTRY_ISO2","COUNTRY_NAME","COUNTRY_TYPE","ADDRESS","EMAIL","PHONE"],
    rows: manufacturerRows(changed),
  });

  await bulkMerge({
    table: "AUTHORISED_REPRESENTATIVES",
    keyCols: ["DEVICE_UUID"],
    allCols: ["DEVICE_UUID","NAME","SRN","ADDRESS","COUNTRY_NAME","EMAIL","PHONE","MANDATE_START_DATE","MANDATE_END_DATE"],
    updateCols: ["NAME","SRN","ADDRESS","COUNTRY_NAME","EMAIL","PHONE","MANDATE_START_DATE","MANDATE_END_DATE"],
    rows: arRows(changed),
  });

  const { primary, mfrCerts } = certRows(changed);
  await bulkMerge({
    table: "DEVICE_CERTIFICATES",
    keyCols: ["DEVICE_UUID","CERTIFICATE_UUID"],
    allCols: ["DEVICE_UUID","CERTIFICATE_UUID","CERTIFICATE_NUMBER","CERTIFICATE_TYPE","ISSUE_DATE","EXPIRY_DATE","STARTING_VALIDITY_DATE","STATUS","NOTIFIED_BODY_NAME","NOTIFIED_BODY_SRN","NOTIFIED_BODY_COUNTRY","REVISION","SOURCE"],
    updateCols: ["CERTIFICATE_NUMBER","CERTIFICATE_TYPE","ISSUE_DATE","EXPIRY_DATE","STARTING_VALIDITY_DATE","STATUS","NOTIFIED_BODY_NAME","NOTIFIED_BODY_SRN","NOTIFIED_BODY_COUNTRY","REVISION","SOURCE"],
    rows: primary,
  });
  await bulkMerge({
    table: "DEVICE_CERTIFICATES",
    keyCols: ["DEVICE_UUID","CERTIFICATE_NUMBER","SOURCE"],
    allCols: ["DEVICE_UUID","CERTIFICATE_NUMBER","CERTIFICATE_TYPE","ISSUE_DATE","EXPIRY_DATE","STATUS","NOTIFIED_BODY_SRN","REVISION","SOURCE"],
    updateCols: ["CERTIFICATE_TYPE","ISSUE_DATE","EXPIRY_DATE","STATUS","NOTIFIED_BODY_SRN","REVISION"],
    rows: mfrCerts,
  });

  await bulkMerge({
    table: "DEVICE_ADVERSE_EVENTS",
    keyCols: ["DEVICE_UUID","TITLE"],
    allCols: ["DEVICE_UUID","DEVICE_NAME","SOURCE","TITLE","AUTHORS","JOURNAL","PUBLICATION_DATE","DOI","URL","STATUS","EVENT_DATE","MATCH_CONFIDENCE","MATCH_TYPE","MATCHED_KEYWORD"],
    updateCols: ["DEVICE_NAME","SOURCE","AUTHORS","JOURNAL","PUBLICATION_DATE","DOI","URL","STATUS","EVENT_DATE","MATCH_CONFIDENCE","MATCH_TYPE","MATCHED_KEYWORD"],
    rows: adverseRows(changed),
  });

  await bulkMerge({
    table: "DEVICE_CLINICAL_EVIDENCE",
    keyCols: ["DEVICE_UUID","TITLE"],
    allCols: ["DEVICE_UUID","DEVICE_NAME","SOURCE","EVIDENCE_TYPE","TITLE","AUTHORS","JOURNAL","PUBLICATION_DATE","DOI","URL","MATCH_CONFIDENCE","MATCH_TYPE","MATCHED_KEYWORD"],
    updateCols: ["DEVICE_NAME","SOURCE","EVIDENCE_TYPE","AUTHORS","JOURNAL","PUBLICATION_DATE","DOI","URL","MATCH_CONFIDENCE","MATCH_TYPE","MATCHED_KEYWORD"],
    rows: evidenceRows(changed),
  });
}

// Single-device wrapper (kept for backward compat with per-device callers)
async function insertDeviceComplete(deviceJSON) {
  if (!deviceJSON?.identity?.uuid) return;
  await insertDevicesBatch([deviceJSON]);
}

// Fetch the set of existing primary keys in `table` (for skip-if-exists optimization).
// Cached per-table for the process lifetime. Returns a Set of stringified keys.
const existingKeysCache = new Map();
async function loadExistingKeys(table, keyCol) {
  const cacheId = `${table}.${keyCol}`;
  if (existingKeysCache.has(cacheId)) return existingKeysCache.get(cacheId);
  await useDB();
  const set = new Set();
  try {
    const rows = await executeSQL(`SELECT ${keyCol} FROM ${table}`);
    for (const r of rows || []) if (r[keyCol] != null) set.add(String(r[keyCol]));
  } catch (e) {
    console.log(`[snowflake] skip-cache init for ${table}: ${e.message}`);
  }
  existingKeysCache.set(cacheId, set);
  return set;
}
function markKeyInserted(table, keyCol, key) {
  const set = existingKeysCache.get(`${table}.${keyCol}`);
  if (set && key != null) set.add(String(key));
}

// === BATCH INSERT FUNCTIONS (preferred) ===

async function insertNotifiedBodiesBatch(nbs) {
  const existing = await loadExistingKeys("NOTIFIED_BODIES", "UUID");
  const rows = (nbs || [])
    .filter(n => n?.uuid && !existing.has(String(n.uuid)))
    .map(nb => {
      markKeyInserted("NOTIFIED_BODIES", "UUID", nb.uuid);
      return {
        UUID: nb.uuid,
        NAME: nb.name,
        IDENTIFIER: nb.eudamedIdentifier,
        MDR_STATUS: nb.legislationStatusMap?.["refdata.applicable-legislation.mdr"]?.code?.split(".").pop() || null,
        IVDR_STATUS: nb.legislationStatusMap?.["refdata.applicable-legislation.ivdr"]?.code?.split(".").pop() || null,
        RAW_DATA: JSON.stringify(nb),
      };
    });
  await bulkMerge({
    table: "NOTIFIED_BODIES",
    keyCols: ["UUID"],
    allCols: ["UUID","NAME","IDENTIFIER","MDR_STATUS","IVDR_STATUS","RAW_DATA"],
    updateCols: ["NAME","IDENTIFIER","MDR_STATUS","IVDR_STATUS","RAW_DATA"],
    rows,
  });
}

async function insertRefusedApplicationsBatch(apps) {
  const existing = await loadExistingKeys("REFUSED_APPLICATIONS", "UUID");
  const rows = (apps || [])
    .filter(a => a?.uuid && !existing.has(String(a.uuid)))
    .map(app => {
      markKeyInserted("REFUSED_APPLICATIONS", "UUID", app.uuid);
      return {
        UUID: app.uuid,
        ACTOR_SRN: app.actorSrn,
        ACTOR_NAME: app.actorName,
        NOTIFIED_BODY_SRN: app.notifiedBodySrn,
        APPLICATION_REFERENCE: app.applicationReferenceNumber,
        CONFORMITY_PROCEDURE: app.conformityAssessmentProcedure?.code,
        DECISION: app.decision?.code,
        DECISION_DATE: app.decisionDate,
        LAST_UPDATE_DATE: app.lastUpdateDate,
        RAW_DATA: JSON.stringify(app),
      };
    });
  await bulkMerge({
    table: "REFUSED_APPLICATIONS",
    keyCols: ["UUID"],
    allCols: ["UUID","ACTOR_SRN","ACTOR_NAME","NOTIFIED_BODY_SRN","APPLICATION_REFERENCE","CONFORMITY_PROCEDURE","DECISION","DECISION_DATE","LAST_UPDATE_DATE","RAW_DATA"],
    updateCols: ["ACTOR_SRN","ACTOR_NAME","NOTIFIED_BODY_SRN","APPLICATION_REFERENCE","CONFORMITY_PROCEDURE","DECISION","DECISION_DATE","LAST_UPDATE_DATE","RAW_DATA"],
    rows,
  });
}

async function insertSafetyNoticesBatch(source, records) {
  const existing = await loadExistingKeys("SAFETY_NOTICES", "SOURCE_ID");
  const rows = [];
  for (const record of records || []) {
    const sourceId = `${source}_${(record.deviceName || record.title || "").substring(0, 200)}_${record.updateDate || record.date || ""}`;
    if (existing.has(sourceId)) continue;
    markKeyInserted("SAFETY_NOTICES", "SOURCE_ID", sourceId);
    rows.push({
      SOURCE: source,
      SOURCE_ID: sourceId,
      TITLE: record.title || record.deviceName,
      DEVICE_NAME: record.deviceName,
      DEVICE_TYPE: record.deviceType,
      STATUS: record.status,
      NOTICE_DATE: record.updateDate || record.date,
      RETURN_DATE: record.returnDate,
      TOPIC: record.topic,
      URL: record.url,
      RAW_DATA: JSON.stringify(record),
    });
  }
  await bulkMerge({
    table: "SAFETY_NOTICES",
    keyCols: ["SOURCE_ID"],
    allCols: ["SOURCE","SOURCE_ID","TITLE","DEVICE_NAME","DEVICE_TYPE","STATUS","NOTICE_DATE","RETURN_DATE","TOPIC","URL","RAW_DATA"],
    updateCols: ["SOURCE","TITLE","DEVICE_NAME","DEVICE_TYPE","STATUS","NOTICE_DATE","RETURN_DATE","TOPIC","URL","RAW_DATA"],
    rows,
  });
}

async function insertMHRAAlertsBatch(alerts) {
  const existing = await loadExistingKeys("MHRA_ALERTS", "ALERT_ID");
  const rows = (alerts || [])
    .filter(a => a?.alertId && !existing.has(String(a.alertId)))
    .map(a => {
      markKeyInserted("MHRA_ALERTS", "ALERT_ID", a.alertId);
      return {
        ALERT_ID: a.alertId,
        BRAND: a.brand,
        MANUFACTURER_SRN: a.manufacturerSrn,
        MANUFACTURER_NAME: a.manufacturerName,
        MANUFACTURER_COUNTRY: a.manufacturerCountry,
        TITLE: a.title,
        DESCRIPTION: a.description,
        FULL_CONTENT: a.fullContent,
        PUBLISHED_DATE: a.publishedDate,
        ALERT_TYPE: a.alertType,
        URL: a.url,
        PROBLEM_CATEGORIES: JSON.stringify(a.problemCategories || []),
        MENTIONS_PATIENT_HARM: !!a.mentionsPatientHarm,
        MENTIONS_RECALL: !!a.mentionsRecall,
        MENTIONS_SOFTWARE: !!a.mentionsSoftware,
        RAW_DATA: JSON.stringify(a),
      };
    });
  await bulkMerge({
    table: "MHRA_ALERTS",
    keyCols: ["ALERT_ID"],
    allCols: ["ALERT_ID","BRAND","MANUFACTURER_SRN","MANUFACTURER_NAME","MANUFACTURER_COUNTRY","TITLE","DESCRIPTION","FULL_CONTENT","PUBLISHED_DATE","ALERT_TYPE","URL","PROBLEM_CATEGORIES","MENTIONS_PATIENT_HARM","MENTIONS_RECALL","MENTIONS_SOFTWARE","RAW_DATA"],
    updateCols: ["BRAND","MANUFACTURER_SRN","MANUFACTURER_NAME","MANUFACTURER_COUNTRY","TITLE","DESCRIPTION","FULL_CONTENT","PUBLISHED_DATE","ALERT_TYPE","URL","PROBLEM_CATEGORIES","MENTIONS_PATIENT_HARM","MENTIONS_RECALL","MENTIONS_SOFTWARE","RAW_DATA"],
    rows,
  });
}

async function insertEUManufacturersBatch(mfrs) {
  const rows = (mfrs || []).filter(m => m?.srn).map(m => ({
    SRN: m.srn,
    UUID: m.uuid,
    NAME: m.name,
    ABBREVIATED_NAME: m.abbreviatedName,
    BRAND: m.brand,
    ACTOR_TYPE: m.actorType?.code?.split(".").pop(),
    STATUS: m.actorStatus?.code?.split(".").pop(),
    COUNTRY_ISO2: m.countryIso2Code,
    COUNTRY_NAME: m.countryName,
    COUNTRY_TYPE: m.countryType,
    GEOGRAPHICAL_ADDRESS: m.geographicalAddress,
    CITY_NAME: m.cityName,
    POSTAL_ZONE: m.postalZone,
    ELECTRONIC_MAIL: m.electronicMail,
    TELEPHONE: m.telephone,
    DATE_OF_REGISTRATION: m.dateOfRegistration,
    VERSION_NUMBER: m.versionNumber,
    RAW_DATA: JSON.stringify(m),
  }));
  await bulkMerge({
    table: "EU_MANUFACTURERS",
    keyCols: ["SRN"],
    allCols: ["SRN","UUID","NAME","ABBREVIATED_NAME","BRAND","ACTOR_TYPE","STATUS","COUNTRY_ISO2","COUNTRY_NAME","COUNTRY_TYPE","GEOGRAPHICAL_ADDRESS","CITY_NAME","POSTAL_ZONE","ELECTRONIC_MAIL","TELEPHONE","DATE_OF_REGISTRATION","VERSION_NUMBER","RAW_DATA"],
    updateCols: ["UUID","NAME","ABBREVIATED_NAME","BRAND","ACTOR_TYPE","STATUS","COUNTRY_ISO2","COUNTRY_NAME","COUNTRY_TYPE","GEOGRAPHICAL_ADDRESS","CITY_NAME","POSTAL_ZONE","ELECTRONIC_MAIL","TELEPHONE","DATE_OF_REGISTRATION","VERSION_NUMBER","RAW_DATA"],
    rows,
  });
}

async function insertClinicalTrialsBatch(trials) {
  const rows = [];
  for (const r of trials || []) {
    const p = r.protocolSection || r;
    const nctId = p.identificationModule?.nctId || r.nct_id;
    if (!nctId) continue;
    const interventions = p.armsInterventionsModule?.interventions || [];
    rows.push({
      NCT_ID: nctId,
      TITLE: p.identificationModule?.briefTitle || null,
      OFFICIAL_TITLE: p.identificationModule?.officialTitle || null,
      BRIEF_SUMMARY: p.descriptionModule?.briefSummary || null,
      CONDITION: (p.conditionsModule?.conditions || []).join("; ") || null,
      INTERVENTION_TYPE: interventions.map(i => i.type).filter(Boolean).join(", ") || null,
      INTERVENTION_NAME: interventions.map(i => i.name).filter(Boolean).join("; ") || null,
      SPONSOR: p.sponsorCollaboratorsModule?.leadSponsor?.name || null,
      PHASE: (p.designModule?.phases || []).join(", ") || null,
      STATUS: p.statusModule?.overallStatus || null,
      STUDY_TYPE: p.designModule?.studyType || null,
      PRIMARY_OUTCOME: (p.outcomesModule?.primaryOutcomes || []).map(o => o.measure).filter(Boolean).join("; ") || null,
      ENROLLMENT: p.designModule?.enrollmentInfo?.count || null,
      START_DATE: p.statusModule?.startDateStruct?.date || null,
      COMPLETION_DATE: p.statusModule?.completionDateStruct?.date || null,
      COUNTRY: [...new Set((p.contactsLocationsModule?.locations || []).map(l => l.country))].filter(Boolean).join(", ") || null,
      URL: `https://clinicaltrials.gov/study/${nctId}`,
      RAW_DATA: JSON.stringify(r),
    });
  }
  await bulkMerge({
    table: "CLINICAL_TRIALS",
    keyCols: ["NCT_ID"],
    allCols: ["NCT_ID","TITLE","OFFICIAL_TITLE","BRIEF_SUMMARY","CONDITION","INTERVENTION_TYPE","INTERVENTION_NAME","SPONSOR","PHASE","STATUS","STUDY_TYPE","PRIMARY_OUTCOME","ENROLLMENT","START_DATE","COMPLETION_DATE","COUNTRY","URL","RAW_DATA"],
    updateCols: ["TITLE","OFFICIAL_TITLE","BRIEF_SUMMARY","CONDITION","INTERVENTION_TYPE","INTERVENTION_NAME","SPONSOR","PHASE","STATUS","STUDY_TYPE","PRIMARY_OUTCOME","ENROLLMENT","START_DATE","COMPLETION_DATE","COUNTRY","URL","RAW_DATA"],
    rows,
  });
}

async function insertEuropePmcBatch(articles) {
  const rows = [];
  for (const r of articles || []) {
    const pmid = r.pmid || null;
    const pmcid = r.pmcid || null;
    if (!pmid && !pmcid && !r.id) continue;
    rows.push({
      // Synthesize a single dedupe key so JS-side dedupe works reliably
      _DEDUPE_KEY: pmid || pmcid || r.id,
      PMID: pmid,
      PMCID: pmcid,
      DOI: r.doi,
      TITLE: r.title,
      ABSTRACT: r.abstractText,
      AUTHORS: r.authorString,
      JOURNAL: r.journalTitle,
      PUBLICATION_DATE: r.firstPublicationDate,
      SEARCH_TERM: r.searchTerm,
      HAS_FULLTEXT: r.hasFullText === "Y",
      URL: pmid ? `https://pubmed.ncbi.nlm.nih.gov/${pmid}/` : (pmcid ? `https://europepmc.org/article/PMC/${pmcid}` : null),
      RAW_DATA: JSON.stringify(r),
    });
  }
  // Dedupe by synthesized key
  const seen = new Map();
  for (const row of rows) seen.set(row._DEDUPE_KEY, row);
  const deduped = [...seen.values()].map(({ _DEDUPE_KEY, ...rest }) => rest);
  // Partition by which key is populated: PMID present vs PMCID-only. Two merges avoid the ambiguous OR join.
  const pmidRows = deduped.filter(r => r.PMID);
  const pmcidOnly = deduped.filter(r => !r.PMID && r.PMCID);
  const allCols = ["PMID","PMCID","DOI","TITLE","ABSTRACT","AUTHORS","JOURNAL","PUBLICATION_DATE","SEARCH_TERM","HAS_FULLTEXT","URL","RAW_DATA"];
  const updateCols = ["DOI","TITLE","ABSTRACT","AUTHORS","JOURNAL","PUBLICATION_DATE","SEARCH_TERM","HAS_FULLTEXT","URL","RAW_DATA"];
  if (pmidRows.length) {
    await bulkMerge({ table: "EUROPE_PMC_ARTICLES", keyCols: ["PMID"], allCols, updateCols: ["PMCID", ...updateCols], rows: pmidRows });
  }
  if (pmcidOnly.length) {
    await bulkMerge({ table: "EUROPE_PMC_ARTICLES", keyCols: ["PMCID"], allCols, updateCols, rows: pmcidOnly });
  }
}

// === SINGLE-ROW WRAPPERS (backward compat — prefer *Batch variants) ===
async function insertNotifiedBody(nb) { await insertNotifiedBodiesBatch([nb]); }
async function insertRefusedApplication(app) { await insertRefusedApplicationsBatch([app]); }
async function insertSafetyNotice(source, record) { await insertSafetyNoticesBatch(source, [record]); }
async function insertMHRAAlert(a) { await insertMHRAAlertsBatch([a]); }
async function insertEUManufacturer(m) { await insertEUManufacturersBatch([m]); }
async function insertClinicalTrial(r) { await insertClinicalTrialsBatch([r]); }
async function insertEuropePmc(r) { await insertEuropePmcBatch([r]); }

async function getTableCount(tableName) {
  try {
    await useDB();
    const rows = await executeSQL(`SELECT COUNT(*) AS CNT FROM ${tableName}`);
    return rows[0]?.CNT || 0;
  } catch {
    return 0;
  }
}

function closeConnection() {
  if (connection) {
    connection.destroy((err) => {
      if (err) console.error("Snowflake disconnect error:", err.message);
      else console.log("Snowflake disconnected");
    });
  }
}

module.exports = {
  getConnection,
  executeSQL,
  useDB,
  setupDatabase,
  // Batch (preferred)
  insertDevicesBatch,
  insertNotifiedBodiesBatch,
  insertRefusedApplicationsBatch,
  insertSafetyNoticesBatch,
  insertMHRAAlertsBatch,
  insertEUManufacturersBatch,
  insertClinicalTrialsBatch,
  insertEuropePmcBatch,
  // Single-row (legacy wrappers)
  insertDeviceComplete,
  insertNotifiedBody,
  insertRefusedApplication,
  insertSafetyNotice,
  insertMHRAAlert,
  insertEUManufacturer,
  insertClinicalTrial,
  insertEuropePmc,
  getTableCount,
  closeConnection,
};
