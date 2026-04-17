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

async function useDB() {
  await executeSQL(`USE DATABASE ${DB_NAME}`);
  await executeSQL("USE SCHEMA MEDICAL_DEVICES");
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

  // === SAFETY NOTICES (ANSM, SCHEER, etc.) ===
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
      TOPIC VARCHAR(500),
      URL TEXT,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === OPENFDA 510(k) CLEARANCES ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS OPENFDA_510K (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      K_NUMBER VARCHAR(50) UNIQUE,
      DEVICE_NAME TEXT,
      APPLICANT VARCHAR(1000),
      PRODUCT_CODE VARCHAR(50),
      DECISION_DATE VARCHAR(100),
      DECISION_DESCRIPTION VARCHAR(500),
      DATE_RECEIVED VARCHAR(100),
      STATEMENT_OR_SUMMARY VARCHAR(100),
      CLEARANCE_TYPE VARCHAR(200),
      THIRD_PARTY_FLAG VARCHAR(10),
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === OPENFDA MAUDE ADVERSE EVENTS ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS OPENFDA_MAUDE (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      MDR_REPORT_KEY VARCHAR(100) UNIQUE,
      EVENT_TYPE VARCHAR(255),
      DATE_RECEIVED VARCHAR(100),
      DATE_OF_EVENT VARCHAR(100),
      REPORT_SOURCE_CODE VARCHAR(10),
      DEVICE_NAME TEXT,
      BRAND_NAME VARCHAR(1000),
      GENERIC_NAME VARCHAR(1000),
      MANUFACTURER_NAME VARCHAR(1000),
      PRODUCT_PROBLEMS TEXT,
      EVENT_DESCRIPTION TEXT,
      PATIENT_OUTCOME TEXT,
      RAW_DATA TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

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
  console.log("Snowflake database setup complete — all tables created");
}

// === INSERT FUNCTIONS ===

async function insertDeviceComplete(deviceJSON) {
  const uuid = deviceJSON.identity?.uuid;
  if (!uuid) return;
  await useDB();

  // 1. DEVICES table
  // Simple INSERT with ON CONFLICT-style handling via MERGE
  await executeSQL(
    `
    MERGE INTO DEVICES AS t USING (SELECT ? AS UUID) AS s ON t.UUID = s.UUID
    WHEN MATCHED THEN UPDATE SET
      TRADE_NAME=?, DEVICE_NAME=?, DEVICE_MODEL=?, RISK_CLASS=?, LEGISLATION=?,
      DEVICE_STATUS=?, LAST_UPDATE_DATE=?, RAW_DATA=?, UPDATED_AT=CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      UUID, ULID, BASIC_UDI, PRIMARY_DI, REFERENCE,
      TRADE_NAME, DEVICE_NAME, DEVICE_MODEL, DEVICE_CRITERION,
      RISK_CLASS, RISK_CLASS_CODE, LEGISLATION, LEGISLATION_CODE, LEGACY_DIRECTIVE,
      SPECIAL_DEVICE_TYPE, ISSUING_AGENCY, CONTAINER_PACKAGE_COUNT,
      IS_ACTIVE, IS_IMPLANTABLE, IS_REUSABLE, IS_STERILE,
      HAS_MEASURING_FUNCTION, ADMINISTERS_MEDICINE, IS_MULTI_COMPONENT,
      CONTAINS_HUMAN_TISSUES, CONTAINS_ANIMAL_TISSUES, CONTAINS_HUMAN_PRODUCT, CONTAINS_MEDICINAL_PRODUCT,
      IS_KIT, IS_REAGENT, IS_INSTRUMENT, IS_COMPANION_DIAGNOSTIC,
      IS_SELF_TESTING, IS_NEAR_PATIENT_TESTING, IS_PROFESSIONAL_TESTING,
      DEVICE_STATUS, VERSION_STATE, LATEST_VERSION, VERSION_NUMBER,
      VERSION_DATE, LAST_UPDATE_DATE, DISCARDED_DATE, IS_NEW,
      CLINICAL_INVESTIGATION_APPLICABLE, RAW_DATA
    ) VALUES (
      ?,?,?,?,?,
      ?,?,?,?,
      ?,?,?,?,?,
      ?,?,?,
      ?,?,?,?,
      ?,?,?,
      ?,?,?,?,
      ?,?,?,?,
      ?,?,?,
      ?,?,?,?,
      ?,?,?,?,
      ?,?
    )
  `,
    [
      uuid,
      // UPDATE binds (8)
      deviceJSON.identity?.tradeName,
      deviceJSON.identity?.deviceName,
      deviceJSON.identity?.deviceModel,
      deviceJSON.classification?.riskClass,
      deviceJSON.classification?.legislation,
      deviceJSON.status?.deviceStatus,
      deviceJSON.status?.lastUpdateDate,
      JSON.stringify(deviceJSON),
      // INSERT binds (45)
      uuid,
      deviceJSON.identity?.ulid,
      deviceJSON.identity?.basicUdi,
      deviceJSON.identity?.primaryDi,
      deviceJSON.identity?.reference,
      deviceJSON.identity?.tradeName,
      deviceJSON.identity?.deviceName,
      deviceJSON.identity?.deviceModel,
      deviceJSON.identity?.deviceCriterion,
      deviceJSON.classification?.riskClass,
      deviceJSON.classification?.riskClassCode,
      deviceJSON.classification?.legislation,
      deviceJSON.classification?.legislationCode,
      deviceJSON.classification?.legacyDirective || false,
      deviceJSON.classification?.specialDeviceType,
      deviceJSON.classification?.issuingAgency,
      deviceJSON.classification?.containerPackageCount || 0,
      deviceJSON.characteristics?.active || false,
      deviceJSON.characteristics?.implantable || false,
      deviceJSON.characteristics?.reusable || false,
      deviceJSON.characteristics?.sterile || false,
      deviceJSON.characteristics?.measuringFunction || false,
      deviceJSON.characteristics?.administeringMedicine || false,
      Boolean(deviceJSON.characteristics?.multiComponent && typeof deviceJSON.characteristics?.multiComponent !== 'object'),
      deviceJSON.characteristics?.humanTissues || false,
      deviceJSON.characteristics?.animalTissues || false,
      deviceJSON.characteristics?.humanProduct || false,
      deviceJSON.characteristics?.medicinalProduct || false,
      deviceJSON.characteristics?.kit || false,
      deviceJSON.characteristics?.reagent || false,
      deviceJSON.characteristics?.instrument || false,
      deviceJSON.characteristics?.companionDiagnostics || false,
      deviceJSON.characteristics?.selfTesting || false,
      deviceJSON.characteristics?.nearPatientTesting || false,
      deviceJSON.characteristics?.professionalTesting || false,
      deviceJSON.status?.deviceStatus,
      deviceJSON.status?.versionState,
      deviceJSON.status?.latestVersion,
      deviceJSON.status?.versionNumber,
      deviceJSON.status?.versionDate,
      deviceJSON.status?.lastUpdateDate,
      deviceJSON.status?.discardedDate,
      deviceJSON.status?.isNew || false,
      deviceJSON.clinicalInvestigation?.applicable || false,
      JSON.stringify(deviceJSON),
    ],
  );

  // 2. MANUFACTURERS table — use uuid if available, otherwise fall back to srn or name
  const mfr = deviceJSON.manufacturer;
  if (mfr && (mfr.uuid || mfr.srn || mfr.name)) {
    const mfrKey = mfr.uuid || mfr.srn || `name:${mfr.name}`;
    await executeSQL(
      `
      MERGE INTO MANUFACTURERS AS t USING (SELECT ? AS UUID) AS s ON t.UUID = s.UUID
      WHEN MATCHED THEN UPDATE SET
        SRN=?, NAME=?, STATUS=?, COUNTRY_ISO2=?, COUNTRY_NAME=?, COUNTRY_TYPE=?, ADDRESS=?, EMAIL=?, PHONE=?
      WHEN NOT MATCHED THEN INSERT (UUID, SRN, NAME, STATUS, COUNTRY_ISO2, COUNTRY_NAME, COUNTRY_TYPE, ADDRESS, EMAIL, PHONE)
      VALUES (?,?,?,?,?,?,?,?,?,?)
    `,
      [
        mfrKey,
        // UPDATE binds (9)
        mfr.srn, mfr.name, mfr.status, mfr.countryIso2Code, mfr.countryName, mfr.countryType, mfr.address, mfr.email, mfr.phone,
        // INSERT binds (10)
        mfrKey, mfr.srn, mfr.name, mfr.status, mfr.countryIso2Code, mfr.countryName, mfr.countryType, mfr.address, mfr.email, mfr.phone,
      ],
    );
  }

  // 3. AUTHORISED REPRESENTATIVES
  const ar = deviceJSON.authorisedRepresentative;
  if (ar?.name) {
    await executeSQL(
      `
      MERGE INTO AUTHORISED_REPRESENTATIVES AS t USING (SELECT ? AS DEVICE_UUID) AS s ON t.DEVICE_UUID = s.DEVICE_UUID
      WHEN MATCHED THEN UPDATE SET
        NAME=?, SRN=?, ADDRESS=?, COUNTRY_NAME=?, EMAIL=?, PHONE=?, MANDATE_START_DATE=?, MANDATE_END_DATE=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, NAME, SRN, ADDRESS, COUNTRY_NAME, EMAIL, PHONE, MANDATE_START_DATE, MANDATE_END_DATE)
      VALUES (?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid,
        // UPDATE binds (8)
        ar.name, ar.srn, ar.address, ar.countryName, ar.email, ar.phone, ar.mandateStartDate, ar.mandateEndDate,
        // INSERT binds (9)
        uuid, ar.name, ar.srn, ar.address, ar.countryName, ar.email, ar.phone, ar.mandateStartDate, ar.mandateEndDate,
      ],
    );
  }

  // 4. CERTIFICATES
  for (const cert of deviceJSON.certificates || []) {
    await executeSQL(
      `
      MERGE INTO DEVICE_CERTIFICATES AS t
      USING (SELECT ? AS DEVICE_UUID, ? AS CERTIFICATE_UUID) AS s
      ON t.DEVICE_UUID = s.DEVICE_UUID AND t.CERTIFICATE_UUID = s.CERTIFICATE_UUID
      WHEN MATCHED THEN UPDATE SET
        CERTIFICATE_NUMBER=?, CERTIFICATE_TYPE=?, ISSUE_DATE=?, EXPIRY_DATE=?, STARTING_VALIDITY_DATE=?, STATUS=?, NOTIFIED_BODY_NAME=?, NOTIFIED_BODY_SRN=?, NOTIFIED_BODY_COUNTRY=?, REVISION=?, SOURCE=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, CERTIFICATE_UUID, CERTIFICATE_NUMBER, CERTIFICATE_TYPE, ISSUE_DATE, EXPIRY_DATE, STARTING_VALIDITY_DATE, STATUS, NOTIFIED_BODY_NAME, NOTIFIED_BODY_SRN, NOTIFIED_BODY_COUNTRY, REVISION, SOURCE)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid, cert.uuid,
        // UPDATE binds (11)
        cert.certificateNumber, cert.certificateType, cert.issueDate, cert.expiryDate, cert.startingValidityDate, cert.status, cert.notifiedBody?.name, cert.notifiedBody?.srn, cert.notifiedBody?.countryIso2Code, cert.revision, "EUDAMED",
        // INSERT binds (13)
        uuid, cert.uuid, cert.certificateNumber, cert.certificateType, cert.issueDate, cert.expiryDate, cert.startingValidityDate, cert.status, cert.notifiedBody?.name, cert.notifiedBody?.srn, cert.notifiedBody?.countryIso2Code, cert.revision, "EUDAMED",
      ],
    );
  }
  for (const cert of deviceJSON.manufacturerCertificates || []) {
    await executeSQL(
      `
      MERGE INTO DEVICE_CERTIFICATES AS t
      USING (SELECT ? AS DEVICE_UUID, ? AS CERTIFICATE_NUMBER, ? AS SOURCE) AS s
      ON t.DEVICE_UUID = s.DEVICE_UUID AND t.CERTIFICATE_NUMBER = s.CERTIFICATE_NUMBER AND t.SOURCE = s.SOURCE
      WHEN MATCHED THEN UPDATE SET
        CERTIFICATE_TYPE=?, ISSUE_DATE=?, EXPIRY_DATE=?, STATUS=?, NOTIFIED_BODY_SRN=?, REVISION=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, CERTIFICATE_NUMBER, CERTIFICATE_TYPE, ISSUE_DATE, EXPIRY_DATE, STATUS, NOTIFIED_BODY_SRN, REVISION, SOURCE)
      VALUES (?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid, cert.certificateNumber, "EUDAMED_MFR",
        // UPDATE binds (6)
        cert.certificateType, cert.issueDate, cert.expiryDate, cert.status, cert.notifiedBodySrn, cert.revision,
        // INSERT binds (9)
        uuid, cert.certificateNumber, cert.certificateType, cert.issueDate, cert.expiryDate, cert.status, cert.notifiedBodySrn, cert.revision, "EUDAMED_MFR",
      ],
    );
  }

  // 5. ADVERSE EVENTS (deduplicate by DEVICE_UUID + TITLE)
  const deviceName =
    deviceJSON.identity?.tradeName || deviceJSON.identity?.deviceName;
  for (const ae of deviceJSON.adverseEvents || []) {
    await executeSQL(
      `
      MERGE INTO DEVICE_ADVERSE_EVENTS AS t
      USING (SELECT ? AS DEVICE_UUID, ? AS TITLE) AS s
      ON t.DEVICE_UUID = s.DEVICE_UUID AND t.TITLE = s.TITLE
      WHEN MATCHED THEN UPDATE SET
        DEVICE_NAME=?, SOURCE=?, AUTHORS=?, JOURNAL=?, PUBLICATION_DATE=?, DOI=?, URL=?, STATUS=?, EVENT_DATE=?,
        MATCH_CONFIDENCE=?, MATCH_TYPE=?, MATCHED_KEYWORD=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, DEVICE_NAME, SOURCE, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, URL, STATUS, EVENT_DATE, MATCH_CONFIDENCE, MATCH_TYPE, MATCHED_KEYWORD)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid, ae.title,
        // UPDATE binds (12)
        deviceName, ae.source, ae.authors, ae.journal, ae.publicationDate || ae.date, ae.doi, ae.url, ae.status || ae.type || ae.source, ae.date || ae.publicationDate,
        ae.matchConfidence ?? null, ae.matchType ?? null, ae.matchedKeyword ?? null,
        // INSERT binds (14)
        uuid, deviceName, ae.source, ae.title, ae.authors, ae.journal, ae.publicationDate || ae.date, ae.doi, ae.url, ae.status || ae.type || ae.source, ae.date || ae.publicationDate,
        ae.matchConfidence ?? null, ae.matchType ?? null, ae.matchedKeyword ?? null,
      ],
    );
  }

  // 6. CLINICAL EVIDENCE (deduplicate by DEVICE_UUID + TITLE)
  for (const ce of deviceJSON.clinicalEvidence || []) {
    await executeSQL(
      `
      MERGE INTO DEVICE_CLINICAL_EVIDENCE AS t
      USING (SELECT ? AS DEVICE_UUID, ? AS TITLE) AS s
      ON t.DEVICE_UUID = s.DEVICE_UUID AND t.TITLE = s.TITLE
      WHEN MATCHED THEN UPDATE SET
        DEVICE_NAME=?, SOURCE=?, EVIDENCE_TYPE=?, AUTHORS=?, JOURNAL=?, PUBLICATION_DATE=?, DOI=?, URL=?,
        MATCH_CONFIDENCE=?, MATCH_TYPE=?, MATCHED_KEYWORD=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, DEVICE_NAME, SOURCE, EVIDENCE_TYPE, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, URL, MATCH_CONFIDENCE, MATCH_TYPE, MATCHED_KEYWORD)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid, ce.title,
        // UPDATE binds (11)
        deviceName, ce.source, ce.type, ce.authors, ce.journal, ce.publicationDate, ce.doi, ce.url,
        ce.matchConfidence ?? null, ce.matchType ?? null, ce.matchedKeyword ?? null,
        // INSERT binds (13)
        uuid, deviceName, ce.source, ce.type, ce.title, ce.authors, ce.journal, ce.publicationDate, ce.doi, ce.url,
        ce.matchConfidence ?? null, ce.matchType ?? null, ce.matchedKeyword ?? null,
      ],
    );
  }

}

async function insertNotifiedBody(nb) {
  await useDB();
  const mdrStatus =
    nb.legislationStatusMap?.["refdata.applicable-legislation.mdr"]?.code
      ?.split(".")
      .pop() || null;
  const ivdrStatus =
    nb.legislationStatusMap?.["refdata.applicable-legislation.ivdr"]?.code
      ?.split(".")
      .pop() || null;
  await executeSQL(
    `
    MERGE INTO NOTIFIED_BODIES AS t USING (SELECT ? AS UUID) AS s ON t.UUID = s.UUID
    WHEN MATCHED THEN UPDATE SET
      NAME=?, IDENTIFIER=?, MDR_STATUS=?, IVDR_STATUS=?, RAW_DATA=?
    WHEN NOT MATCHED THEN INSERT (UUID, NAME, IDENTIFIER, MDR_STATUS, IVDR_STATUS, RAW_DATA)
    VALUES (?,?,?,?,?,?)
    `,
    [
      nb.uuid,
      // UPDATE binds (5)
      nb.name, nb.eudamedIdentifier, mdrStatus, ivdrStatus, JSON.stringify(nb),
      // INSERT binds (6)
      nb.uuid, nb.name, nb.eudamedIdentifier, mdrStatus, ivdrStatus, JSON.stringify(nb),
    ],
  );
}

async function insertRefusedApplication(app) {
  await useDB();
  await executeSQL(
    `
    MERGE INTO REFUSED_APPLICATIONS AS t USING (SELECT ? AS UUID) AS s ON t.UUID = s.UUID
    WHEN MATCHED THEN UPDATE SET
      ACTOR_SRN=?, ACTOR_NAME=?, NOTIFIED_BODY_SRN=?, APPLICATION_REFERENCE=?, CONFORMITY_PROCEDURE=?, DECISION=?, DECISION_DATE=?, LAST_UPDATE_DATE=?, RAW_DATA=?
    WHEN NOT MATCHED THEN INSERT (UUID, ACTOR_SRN, ACTOR_NAME, NOTIFIED_BODY_SRN, APPLICATION_REFERENCE, CONFORMITY_PROCEDURE, DECISION, DECISION_DATE, LAST_UPDATE_DATE, RAW_DATA)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    `,
    [
      app.uuid,
      // UPDATE binds (9)
      app.actorSrn, app.actorName, app.notifiedBodySrn, app.applicationReferenceNumber, app.conformityAssessmentProcedure?.code, app.decision?.code, app.decisionDate, app.lastUpdateDate, JSON.stringify(app),
      // INSERT binds (10)
      app.uuid, app.actorSrn, app.actorName, app.notifiedBodySrn, app.applicationReferenceNumber, app.conformityAssessmentProcedure?.code, app.decision?.code, app.decisionDate, app.lastUpdateDate, JSON.stringify(app),
    ],
  );
}

async function insertSafetyNotice(source, record) {
  await useDB();
  const sourceId = `${source}_${(record.deviceName || record.title || "").substring(0, 200)}_${record.updateDate || record.date || ""}`;
  await executeSQL(
    `
    MERGE INTO SAFETY_NOTICES AS t USING (SELECT ? AS SOURCE_ID) AS s ON t.SOURCE_ID = s.SOURCE_ID
    WHEN MATCHED THEN UPDATE SET
      SOURCE=?, TITLE=?, DEVICE_NAME=?, DEVICE_TYPE=?, STATUS=?, NOTICE_DATE=?, RETURN_DATE=?, TOPIC=?, URL=?, RAW_DATA=?
    WHEN NOT MATCHED THEN INSERT (SOURCE, SOURCE_ID, TITLE, DEVICE_NAME, DEVICE_TYPE, STATUS, NOTICE_DATE, RETURN_DATE, TOPIC, URL, RAW_DATA)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    `,
    [
      sourceId,
      // UPDATE binds (10)
      source, record.title || record.deviceName, record.deviceName, record.deviceType, record.status, record.updateDate || record.date, record.returnDate, record.topic, record.url, JSON.stringify(record),
      // INSERT binds (11)
      source, sourceId, record.title || record.deviceName, record.deviceName, record.deviceType, record.status, record.updateDate || record.date, record.returnDate, record.topic, record.url, JSON.stringify(record),
    ],
  );
}

async function insertOpenFDA510k(r) {
  await executeSQL(
    `MERGE INTO OPENFDA_510K AS t USING (SELECT ? AS K_NUMBER) AS s ON t.K_NUMBER = s.K_NUMBER
     WHEN MATCHED THEN UPDATE SET DEVICE_NAME=?, APPLICANT=?, PRODUCT_CODE=?, DECISION_DATE=?, DECISION_DESCRIPTION=?, DATE_RECEIVED=?, STATEMENT_OR_SUMMARY=?, CLEARANCE_TYPE=?, THIRD_PARTY_FLAG=?, RAW_DATA=?
     WHEN NOT MATCHED THEN INSERT (K_NUMBER, DEVICE_NAME, APPLICANT, PRODUCT_CODE, DECISION_DATE, DECISION_DESCRIPTION, DATE_RECEIVED, STATEMENT_OR_SUMMARY, CLEARANCE_TYPE, THIRD_PARTY_FLAG, RAW_DATA)
     VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
    [
      r.k_number,
      r.device_name, r.applicant, r.product_code, r.decision_date, r.decision_description, r.date_received, r.statement_or_summary, r.clearance_type, r.third_party_flag, JSON.stringify(r),
      r.k_number, r.device_name, r.applicant, r.product_code, r.decision_date, r.decision_description, r.date_received, r.statement_or_summary, r.clearance_type, r.third_party_flag, JSON.stringify(r),
    ],
  );
}

async function insertOpenFDAMaude(r) {
  const key = r.mdr_report_key || r.report_number;
  if (!key) return;
  const device = (r.device || [])[0] || {};
  const products = (r.product_problems || []).join("; ") || null;
  const mdrText = (r.mdr_text || []).map(t => t.text).filter(Boolean).join(" | ").substring(0, 5000);
  await executeSQL(
    `MERGE INTO OPENFDA_MAUDE AS t USING (SELECT ? AS MDR_REPORT_KEY) AS s ON t.MDR_REPORT_KEY = s.MDR_REPORT_KEY
     WHEN MATCHED THEN UPDATE SET EVENT_TYPE=?, DATE_RECEIVED=?, DATE_OF_EVENT=?, REPORT_SOURCE_CODE=?, DEVICE_NAME=?, BRAND_NAME=?, GENERIC_NAME=?, MANUFACTURER_NAME=?, PRODUCT_PROBLEMS=?, EVENT_DESCRIPTION=?, PATIENT_OUTCOME=?, RAW_DATA=?
     WHEN NOT MATCHED THEN INSERT (MDR_REPORT_KEY, EVENT_TYPE, DATE_RECEIVED, DATE_OF_EVENT, REPORT_SOURCE_CODE, DEVICE_NAME, BRAND_NAME, GENERIC_NAME, MANUFACTURER_NAME, PRODUCT_PROBLEMS, EVENT_DESCRIPTION, PATIENT_OUTCOME, RAW_DATA)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [
      key,
      r.event_type, r.date_received, r.date_of_event, r.report_source_code, device.generic_name || device.brand_name, device.brand_name, device.generic_name, device.manufacturer_d_name || r.manufacturer_name, products, mdrText, ((r.patient || [])[0]?.patient_outcome || []).join(", "), JSON.stringify(r),
      key, r.event_type, r.date_received, r.date_of_event, r.report_source_code, device.generic_name || device.brand_name, device.brand_name, device.generic_name, device.manufacturer_d_name || r.manufacturer_name, products, mdrText, ((r.patient || [])[0]?.patient_outcome || []).join(", "), JSON.stringify(r),
    ],
  );
}

async function insertClinicalTrial(r) {
  const p = r.protocolSection || r;
  const nctId = p.identificationModule?.nctId || r.nct_id;
  if (!nctId) return;
  const title = p.identificationModule?.briefTitle || null;
  const officialTitle = p.identificationModule?.officialTitle || null;
  const briefSummary = p.descriptionModule?.briefSummary || null;
  const condition = (p.conditionsModule?.conditions || []).join("; ") || null;
  const interventions = p.armsInterventionsModule?.interventions || [];
  const interventionType = interventions.map(i => i.type).filter(Boolean).join(", ") || null;
  const interventionName = interventions.map(i => i.name).filter(Boolean).join("; ") || null;
  const sponsor = p.sponsorCollaboratorsModule?.leadSponsor?.name || null;
  const phase = (p.designModule?.phases || []).join(", ") || null;
  const status = p.statusModule?.overallStatus || null;
  const studyType = p.designModule?.studyType || null;
  const primaryOutcome = (p.outcomesModule?.primaryOutcomes || []).map(o => o.measure).filter(Boolean).join("; ") || null;
  const enrollment = p.designModule?.enrollmentInfo?.count || null;
  const startDate = p.statusModule?.startDateStruct?.date || null;
  const completionDate = p.statusModule?.completionDateStruct?.date || null;
  const countries = [...new Set((p.contactsLocationsModule?.locations || []).map(l => l.country))].filter(Boolean).join(", ") || null;
  await executeSQL(
    `MERGE INTO CLINICAL_TRIALS AS t USING (SELECT ? AS NCT_ID) AS s ON t.NCT_ID = s.NCT_ID
     WHEN MATCHED THEN UPDATE SET TITLE=?, OFFICIAL_TITLE=?, BRIEF_SUMMARY=?, CONDITION=?, INTERVENTION_TYPE=?, INTERVENTION_NAME=?, SPONSOR=?, PHASE=?, STATUS=?, STUDY_TYPE=?, PRIMARY_OUTCOME=?, ENROLLMENT=?, START_DATE=?, COMPLETION_DATE=?, COUNTRY=?, URL=?, RAW_DATA=?
     WHEN NOT MATCHED THEN INSERT (NCT_ID, TITLE, OFFICIAL_TITLE, BRIEF_SUMMARY, CONDITION, INTERVENTION_TYPE, INTERVENTION_NAME, SPONSOR, PHASE, STATUS, STUDY_TYPE, PRIMARY_OUTCOME, ENROLLMENT, START_DATE, COMPLETION_DATE, COUNTRY, URL, RAW_DATA)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [
      nctId,
      title, officialTitle, briefSummary, condition, interventionType, interventionName, sponsor, phase, status, studyType, primaryOutcome, enrollment, startDate, completionDate, countries, `https://clinicaltrials.gov/study/${nctId}`, JSON.stringify(r),
      nctId, title, officialTitle, briefSummary, condition, interventionType, interventionName, sponsor, phase, status, studyType, primaryOutcome, enrollment, startDate, completionDate, countries, `https://clinicaltrials.gov/study/${nctId}`, JSON.stringify(r),
    ],
  );
}

async function insertEuropePmc(r) {
  const pmid = r.pmid || null;
  const pmcid = r.pmcid || null;
  const key = pmid || pmcid || r.id;
  if (!key) return;
  await executeSQL(
    `MERGE INTO EUROPE_PMC_ARTICLES AS t USING (SELECT ? AS PMID, ? AS PMCID) AS s ON (t.PMID = s.PMID AND s.PMID IS NOT NULL) OR (t.PMCID = s.PMCID AND s.PMCID IS NOT NULL)
     WHEN MATCHED THEN UPDATE SET DOI=?, TITLE=?, ABSTRACT=?, AUTHORS=?, JOURNAL=?, PUBLICATION_DATE=?, SEARCH_TERM=?, HAS_FULLTEXT=?, URL=?, RAW_DATA=?
     WHEN NOT MATCHED THEN INSERT (PMID, PMCID, DOI, TITLE, ABSTRACT, AUTHORS, JOURNAL, PUBLICATION_DATE, SEARCH_TERM, HAS_FULLTEXT, URL, RAW_DATA)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
    [
      pmid, pmcid,
      r.doi, r.title, r.abstractText, r.authorString, r.journalTitle, r.firstPublicationDate, r.searchTerm, r.hasFullText === "Y", pmid ? `https://pubmed.ncbi.nlm.nih.gov/${pmid}/` : (pmcid ? `https://europepmc.org/article/PMC/${pmcid}` : null), JSON.stringify(r),
      pmid, pmcid, r.doi, r.title, r.abstractText, r.authorString, r.journalTitle, r.firstPublicationDate, r.searchTerm, r.hasFullText === "Y", pmid ? `https://pubmed.ncbi.nlm.nih.gov/${pmid}/` : (pmcid ? `https://europepmc.org/article/PMC/${pmcid}` : null), JSON.stringify(r),
    ],
  );
}

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
  insertDeviceComplete,
  insertNotifiedBody,
  insertRefusedApplication,
  insertSafetyNotice,
  insertOpenFDA510k,
  insertOpenFDAMaude,
  insertClinicalTrial,
  insertEuropePmc,
  getTableCount,
  closeConnection,
};
