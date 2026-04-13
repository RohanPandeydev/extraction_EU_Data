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
function sanitizeBinds(binds) {
  return binds.map((v) => (v === undefined ? null : v));
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
      RAW_DATA VARIANT,
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
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

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
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === RELATED MEDICINES (per device) ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS DEVICE_RELATED_MEDICINES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      DEVICE_UUID VARCHAR(255),
      DEVICE_NAME VARCHAR(1000),
      SOURCE VARCHAR(100),
      MEDICINE_NAME VARCHAR(1000),
      ACTIVE_SUBSTANCE VARCHAR(1000),
      THERAPEUTIC_AREA TEXT,
      STATUS VARCHAR(255),
      HOLDER VARCHAR(1000),
      URL TEXT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === NOTIFIED BODIES ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS NOTIFIED_BODIES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      UUID VARCHAR(255) UNIQUE,
      NAME VARCHAR(500),
      IDENTIFIER VARCHAR(50),
      MDR_STATUS VARCHAR(255),
      IVDR_STATUS VARCHAR(255),
      RAW_DATA VARIANT,
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
      RAW_DATA VARIANT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === EMA MEDICINES ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS EMA_MEDICINES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      PRODUCT_ID VARCHAR(500) UNIQUE,
      MEDICINE_NAME VARCHAR(1000),
      ACTIVE_SUBSTANCE TEXT,
      INN_NAME TEXT,
      ATC_CODE VARCHAR(50),
      THERAPEUTIC_AREA TEXT,
      PHARMACOTHERAPEUTIC_GROUP TEXT,
      THERAPEUTIC_INDICATION TEXT,
      MEDICINE_STATUS VARCHAR(255),
      OPINION_STATUS VARCHAR(255),
      MARKETING_AUTH_HOLDER TEXT,
      AUTHORIZATION_DATE VARCHAR(255),
      OPINION_DATE VARCHAR(255),
      DECISION_DATE VARCHAR(255),
      IS_BIOSIMILAR BOOLEAN DEFAULT FALSE,
      IS_GENERIC BOOLEAN DEFAULT FALSE,
      IS_ORPHAN BOOLEAN DEFAULT FALSE,
      IS_CONDITIONAL BOOLEAN DEFAULT FALSE,
      IS_ADVANCED_THERAPY BOOLEAN DEFAULT FALSE,
      MEDICINE_URL TEXT,
      RAW_DATA VARIANT,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `);

  // === COCHRANE REVIEWS ===
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS COCHRANE_REVIEWS (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      PUBMED_ID VARCHAR(50) UNIQUE,
      TITLE TEXT,
      AUTHORS TEXT,
      JOURNAL VARCHAR(1000),
      PUBLICATION_DATE VARCHAR(255),
      DOI VARCHAR(500),
      SEARCH_TERM VARCHAR(255),
      URL TEXT,
      RAW_DATA VARIANT,
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
      RAW_DATA VARIANT,
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
      DEVICE_STATUS=?, LAST_UPDATE_DATE=?, RAW_DATA=PARSE_JSON(?), UPDATED_AT=CURRENT_TIMESTAMP()
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
      ?,PARSE_JSON(?)
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
      deviceJSON.characteristics?.multiComponent || false,
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

  // 2. MANUFACTURERS table
  const mfr = deviceJSON.manufacturer;
  if (mfr?.uuid) {
    await executeSQL(
      `
      MERGE INTO MANUFACTURERS AS t USING (SELECT ? AS UUID) AS s ON t.UUID = s.UUID
      WHEN NOT MATCHED THEN INSERT (UUID, SRN, NAME, STATUS, COUNTRY_ISO2, COUNTRY_NAME, COUNTRY_TYPE, ADDRESS, EMAIL, PHONE)
      VALUES (?,?,?,?,?,?,?,?,?,?)
    `,
      [
        mfr.uuid,
        mfr.uuid,
        mfr.srn,
        mfr.name,
        mfr.status,
        mfr.countryIso2Code,
        mfr.countryName,
        mfr.countryType,
        mfr.address,
        mfr.email,
        mfr.phone,
      ],
    );
  }

  // 3. AUTHORISED REPRESENTATIVES
  const ar = deviceJSON.authorisedRepresentative;
  if (ar?.name) {
    await executeSQL(
      `INSERT INTO AUTHORISED_REPRESENTATIVES (DEVICE_UUID, NAME, SRN, ADDRESS, COUNTRY_NAME, EMAIL, PHONE, MANDATE_START_DATE, MANDATE_END_DATE) SELECT ?,?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM AUTHORISED_REPRESENTATIVES WHERE DEVICE_UUID = ?)`,
      [
        uuid,
        ar.name,
        ar.srn,
        ar.address,
        ar.countryName,
        ar.email,
        ar.phone,
        ar.mandateStartDate,
        ar.mandateEndDate,
        uuid,
      ],
    );
  }

  // 4. CERTIFICATES
  for (const cert of deviceJSON.certificates || []) {
    await executeSQL(
      `INSERT INTO DEVICE_CERTIFICATES (DEVICE_UUID, CERTIFICATE_UUID, CERTIFICATE_NUMBER, CERTIFICATE_TYPE, ISSUE_DATE, EXPIRY_DATE, STARTING_VALIDITY_DATE, STATUS, NOTIFIED_BODY_NAME, NOTIFIED_BODY_SRN, NOTIFIED_BODY_COUNTRY, REVISION, SOURCE) SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM DEVICE_CERTIFICATES WHERE DEVICE_UUID = ? AND CERTIFICATE_UUID = ?)`,
      [
        uuid,
        cert.uuid,
        cert.certificateNumber,
        cert.certificateType,
        cert.issueDate,
        cert.expiryDate,
        cert.startingValidityDate,
        cert.status,
        cert.notifiedBody?.name,
        cert.notifiedBody?.srn,
        cert.notifiedBody?.countryIso2Code,
        cert.revision,
        "EUDAMED",
        uuid,
        cert.uuid,
      ],
    );
  }
  for (const cert of deviceJSON.manufacturerCertificates || []) {
    await executeSQL(
      `INSERT INTO DEVICE_CERTIFICATES (DEVICE_UUID, CERTIFICATE_NUMBER, CERTIFICATE_TYPE, ISSUE_DATE, EXPIRY_DATE, STATUS, NOTIFIED_BODY_SRN, REVISION, SOURCE) SELECT ?,?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM DEVICE_CERTIFICATES WHERE DEVICE_UUID = ? AND CERTIFICATE_NUMBER = ? AND SOURCE = ?)`,
      [
        uuid,
        cert.certificateNumber,
        cert.certificateType,
        cert.issueDate,
        cert.expiryDate,
        cert.status,
        cert.notifiedBodySrn,
        cert.revision,
        "EUDAMED_MFR",
        uuid,
        cert.certificateNumber,
        "EUDAMED_MFR",
      ],
    );
  }

  // 5. ADVERSE EVENTS
  const deviceName =
    deviceJSON.identity?.tradeName || deviceJSON.identity?.deviceName;
  for (const ae of deviceJSON.adverseEvents || []) {
    await executeSQL(
      `INSERT INTO DEVICE_ADVERSE_EVENTS (DEVICE_UUID, DEVICE_NAME, SOURCE, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, URL, STATUS, EVENT_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
      [
        uuid,
        deviceName,
        ae.source,
        ae.title,
        ae.authors,
        ae.journal,
        ae.publicationDate || ae.date,
        ae.doi,
        ae.url,
        ae.status || ae.type || ae.source,
        ae.date || ae.publicationDate,
      ],
    );
  }

  // 6. CLINICAL EVIDENCE
  for (const ce of deviceJSON.clinicalEvidence || []) {
    await executeSQL(
      `INSERT INTO DEVICE_CLINICAL_EVIDENCE (DEVICE_UUID, DEVICE_NAME, SOURCE, EVIDENCE_TYPE, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, URL) VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [
        uuid,
        deviceName,
        ce.source,
        ce.type,
        ce.title,
        ce.authors,
        ce.journal,
        ce.publicationDate,
        ce.doi,
        ce.url,
      ],
    );
  }

  // 7. RELATED MEDICINES
  for (const med of deviceJSON.relatedMedicines || []) {
    await executeSQL(
      `INSERT INTO DEVICE_RELATED_MEDICINES (DEVICE_UUID, DEVICE_NAME, SOURCE, MEDICINE_NAME, ACTIVE_SUBSTANCE, THERAPEUTIC_AREA, STATUS, HOLDER, URL) VALUES (?,?,?,?,?,?,?,?,?)`,
      [
        uuid,
        deviceName,
        med.source,
        med.medicineName,
        med.activeSubstance,
        med.therapeuticArea,
        med.status,
        med.holder,
        med.url,
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
    `INSERT INTO NOTIFIED_BODIES (UUID, NAME, IDENTIFIER, MDR_STATUS, IVDR_STATUS, RAW_DATA) SELECT ?,?,?,?,?,PARSE_JSON(?) WHERE NOT EXISTS (SELECT 1 FROM NOTIFIED_BODIES WHERE UUID = ?)`,
    [
      nb.uuid,
      nb.name,
      nb.eudamedIdentifier,
      mdrStatus,
      ivdrStatus,
      JSON.stringify(nb),
      nb.uuid,
    ],
  );
}

async function insertRefusedApplication(app) {
  await useDB();
  await executeSQL(
    `INSERT INTO REFUSED_APPLICATIONS (UUID, ACTOR_SRN, ACTOR_NAME, NOTIFIED_BODY_SRN, APPLICATION_REFERENCE, CONFORMITY_PROCEDURE, DECISION, DECISION_DATE, LAST_UPDATE_DATE, RAW_DATA) SELECT ?,?,?,?,?,?,?,?,?,PARSE_JSON(?) WHERE NOT EXISTS (SELECT 1 FROM REFUSED_APPLICATIONS WHERE UUID = ?)`,
    [
      app.uuid,
      app.actorSrn,
      app.actorName,
      app.notifiedBodySrn,
      app.applicationReferenceNumber,
      app.conformityAssessmentProcedure?.code,
      app.decision?.code,
      app.decisionDate,
      app.lastUpdateDate,
      JSON.stringify(app),
      app.uuid,
    ],
  );
}

async function insertEMAMedicine(med) {
  await useDB();
  const id = med.ema_product_number || med.name_of_medicine;
  await executeSQL(
    `INSERT INTO EMA_MEDICINES (PRODUCT_ID, MEDICINE_NAME, ACTIVE_SUBSTANCE, INN_NAME, ATC_CODE, THERAPEUTIC_AREA, PHARMACOTHERAPEUTIC_GROUP, THERAPEUTIC_INDICATION, MEDICINE_STATUS, OPINION_STATUS, MARKETING_AUTH_HOLDER, AUTHORIZATION_DATE, OPINION_DATE, DECISION_DATE, IS_BIOSIMILAR, IS_GENERIC, IS_ORPHAN, IS_CONDITIONAL, IS_ADVANCED_THERAPY, MEDICINE_URL, RAW_DATA) SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,PARSE_JSON(?) WHERE NOT EXISTS (SELECT 1 FROM EMA_MEDICINES WHERE PRODUCT_ID = ?)`,
    [
      id,
      med.name_of_medicine,
      med.active_substance,
      med.international_non_proprietary_name_common_name,
      med.atc_code_human,
      med.therapeutic_area_mesh,
      med.pharmacotherapeutic_group_human,
      med.therapeutic_indication,
      med.medicine_status,
      med.opinion_status,
      med.marketing_authorisation_developer_applicant_holder,
      med.marketing_authorisation_date,
      med.opinion_adopted_date,
      med.european_commission_decision_date,
      med.biosimilar === "Yes",
      med.generic === "Yes",
      med.orphan_medicine === "Yes",
      med.conditional_approval === "Yes",
      med.advanced_therapy === "Yes",
      med.medicine_url,
      JSON.stringify(med),
      id,
    ],
  );
}

async function insertCochraneReview(review) {
  await useDB();
  const id = `pubmed_${review.uid}`;
  const authors = review.authors?.map((a) => a.name).join(", ") || "";
  await executeSQL(
    `INSERT INTO COCHRANE_REVIEWS (PUBMED_ID, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, SEARCH_TERM, URL, RAW_DATA) SELECT ?,?,?,?,?,?,?,?,PARSE_JSON(?) WHERE NOT EXISTS (SELECT 1 FROM COCHRANE_REVIEWS WHERE PUBMED_ID = ?)`,
    [
      id,
      review.title,
      authors,
      review.fulljournalname,
      review.pubdate || review.sortpubdate,
      review.elocationid,
      review.searchTerm,
      `https://pubmed.ncbi.nlm.nih.gov/${review.uid}/`,
      JSON.stringify(review),
      id,
    ],
  );
}

async function insertSafetyNotice(source, record) {
  await useDB();
  const sourceId = `${source}_${(record.deviceName || record.title || "").substring(0, 200)}_${record.updateDate || record.date || ""}`;
  await executeSQL(
    `INSERT INTO SAFETY_NOTICES (SOURCE, SOURCE_ID, TITLE, DEVICE_NAME, DEVICE_TYPE, STATUS, NOTICE_DATE, RETURN_DATE, TOPIC, URL, RAW_DATA) SELECT ?,?,?,?,?,?,?,?,?,?,PARSE_JSON(?) WHERE NOT EXISTS (SELECT 1 FROM SAFETY_NOTICES WHERE SOURCE_ID = ?)`,
    [
      source,
      sourceId,
      record.title || record.deviceName,
      record.deviceName,
      record.deviceType,
      record.status,
      record.updateDate || record.date,
      record.returnDate,
      record.topic,
      record.url,
      JSON.stringify(record),
      sourceId,
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
  insertEMAMedicine,
  insertCochraneReview,
  insertSafetyNotice,
  getTableCount,
  closeConnection,
};
