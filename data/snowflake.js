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
      RAW_DATA TEXT,
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
      WHEN MATCHED THEN UPDATE SET
        SRN=?, NAME=?, STATUS=?, COUNTRY_ISO2=?, COUNTRY_NAME=?, COUNTRY_TYPE=?, ADDRESS=?, EMAIL=?, PHONE=?
      WHEN NOT MATCHED THEN INSERT (UUID, SRN, NAME, STATUS, COUNTRY_ISO2, COUNTRY_NAME, COUNTRY_TYPE, ADDRESS, EMAIL, PHONE)
      VALUES (?,?,?,?,?,?,?,?,?,?)
    `,
      [
        mfr.uuid,
        // UPDATE binds (9)
        mfr.srn, mfr.name, mfr.status, mfr.countryIso2Code, mfr.countryName, mfr.countryType, mfr.address, mfr.email, mfr.phone,
        // INSERT binds (10)
        mfr.uuid, mfr.srn, mfr.name, mfr.status, mfr.countryIso2Code, mfr.countryName, mfr.countryType, mfr.address, mfr.email, mfr.phone,
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
        DEVICE_NAME=?, SOURCE=?, AUTHORS=?, JOURNAL=?, PUBLICATION_DATE=?, DOI=?, URL=?, STATUS=?, EVENT_DATE=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, DEVICE_NAME, SOURCE, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, URL, STATUS, EVENT_DATE)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid, ae.title,
        // UPDATE binds (9)
        deviceName, ae.source, ae.authors, ae.journal, ae.publicationDate || ae.date, ae.doi, ae.url, ae.status || ae.type || ae.source, ae.date || ae.publicationDate,
        // INSERT binds (11)
        uuid, deviceName, ae.source, ae.title, ae.authors, ae.journal, ae.publicationDate || ae.date, ae.doi, ae.url, ae.status || ae.type || ae.source, ae.date || ae.publicationDate,
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
        DEVICE_NAME=?, SOURCE=?, EVIDENCE_TYPE=?, AUTHORS=?, JOURNAL=?, PUBLICATION_DATE=?, DOI=?, URL=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, DEVICE_NAME, SOURCE, EVIDENCE_TYPE, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, URL)
      VALUES (?,?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid, ce.title,
        // UPDATE binds (8)
        deviceName, ce.source, ce.type, ce.authors, ce.journal, ce.publicationDate, ce.doi, ce.url,
        // INSERT binds (10)
        uuid, deviceName, ce.source, ce.type, ce.title, ce.authors, ce.journal, ce.publicationDate, ce.doi, ce.url,
      ],
    );
  }

  // 7. RELATED MEDICINES (deduplicate by DEVICE_UUID + MEDICINE_NAME)
  for (const med of deviceJSON.relatedMedicines || []) {
    await executeSQL(
      `
      MERGE INTO DEVICE_RELATED_MEDICINES AS t
      USING (SELECT ? AS DEVICE_UUID, ? AS MEDICINE_NAME) AS s
      ON t.DEVICE_UUID = s.DEVICE_UUID AND t.MEDICINE_NAME = s.MEDICINE_NAME
      WHEN MATCHED THEN UPDATE SET
        DEVICE_NAME=?, SOURCE=?, ACTIVE_SUBSTANCE=?, THERAPEUTIC_AREA=?, STATUS=?, HOLDER=?, URL=?
      WHEN NOT MATCHED THEN INSERT (DEVICE_UUID, DEVICE_NAME, SOURCE, MEDICINE_NAME, ACTIVE_SUBSTANCE, THERAPEUTIC_AREA, STATUS, HOLDER, URL)
      VALUES (?,?,?,?,?,?,?,?,?)
      `,
      [
        uuid, med.medicineName,
        // UPDATE binds (7)
        deviceName, med.source, med.activeSubstance, med.therapeuticArea, med.status, med.holder, med.url,
        // INSERT binds (9)
        uuid, deviceName, med.source, med.medicineName, med.activeSubstance, med.therapeuticArea, med.status, med.holder, med.url,
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

async function insertEMAMedicine(med) {
  await useDB();
  const id = med.ema_product_number || med.name_of_medicine;
  await executeSQL(
    `
    MERGE INTO EMA_MEDICINES AS t USING (SELECT ? AS PRODUCT_ID) AS s ON t.PRODUCT_ID = s.PRODUCT_ID
    WHEN MATCHED THEN UPDATE SET
      MEDICINE_NAME=?, ACTIVE_SUBSTANCE=?, INN_NAME=?, ATC_CODE=?, THERAPEUTIC_AREA=?, PHARMACOTHERAPEUTIC_GROUP=?, THERAPEUTIC_INDICATION=?, MEDICINE_STATUS=?, OPINION_STATUS=?, MARKETING_AUTH_HOLDER=?, AUTHORIZATION_DATE=?, OPINION_DATE=?, DECISION_DATE=?, IS_BIOSIMILAR=?, IS_GENERIC=?, IS_ORPHAN=?, IS_CONDITIONAL=?, IS_ADVANCED_THERAPY=?, MEDICINE_URL=?, RAW_DATA=?
    WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, MEDICINE_NAME, ACTIVE_SUBSTANCE, INN_NAME, ATC_CODE, THERAPEUTIC_AREA, PHARMACOTHERAPEUTIC_GROUP, THERAPEUTIC_INDICATION, MEDICINE_STATUS, OPINION_STATUS, MARKETING_AUTH_HOLDER, AUTHORIZATION_DATE, OPINION_DATE, DECISION_DATE, IS_BIOSIMILAR, IS_GENERIC, IS_ORPHAN, IS_CONDITIONAL, IS_ADVANCED_THERAPY, MEDICINE_URL, RAW_DATA)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `,
    [
      id,
      // UPDATE binds (20)
      med.name_of_medicine, med.active_substance, med.international_non_proprietary_name_common_name, med.atc_code_human, med.therapeutic_area_mesh, med.pharmacotherapeutic_group_human, med.therapeutic_indication, med.medicine_status, med.opinion_status, med.marketing_authorisation_developer_applicant_holder, med.marketing_authorisation_date, med.opinion_adopted_date, med.european_commission_decision_date, med.biosimilar === "Yes", med.generic === "Yes", med.orphan_medicine === "Yes", med.conditional_approval === "Yes", med.advanced_therapy === "Yes", med.medicine_url, JSON.stringify(med),
      // INSERT binds (21)
      id, med.name_of_medicine, med.active_substance, med.international_non_proprietary_name_common_name, med.atc_code_human, med.therapeutic_area_mesh, med.pharmacotherapeutic_group_human, med.therapeutic_indication, med.medicine_status, med.opinion_status, med.marketing_authorisation_developer_applicant_holder, med.marketing_authorisation_date, med.opinion_adopted_date, med.european_commission_decision_date, med.biosimilar === "Yes", med.generic === "Yes", med.orphan_medicine === "Yes", med.conditional_approval === "Yes", med.advanced_therapy === "Yes", med.medicine_url, JSON.stringify(med),
    ],
  );
}

async function insertCochraneReview(review) {
  await useDB();
  const id = `pubmed_${review.uid}`;
  const authors = review.authors?.map((a) => a.name).join(", ") || "";
  await executeSQL(
    `
    MERGE INTO COCHRANE_REVIEWS AS t USING (SELECT ? AS PUBMED_ID) AS s ON t.PUBMED_ID = s.PUBMED_ID
    WHEN MATCHED THEN UPDATE SET
      TITLE=?, AUTHORS=?, JOURNAL=?, PUBLICATION_DATE=?, DOI=?, SEARCH_TERM=?, URL=?, RAW_DATA=?
    WHEN NOT MATCHED THEN INSERT (PUBMED_ID, TITLE, AUTHORS, JOURNAL, PUBLICATION_DATE, DOI, SEARCH_TERM, URL, RAW_DATA)
    VALUES (?,?,?,?,?,?,?,?,?)
    `,
    [
      id,
      // UPDATE binds (8)
      review.title, authors, review.fulljournalname, review.pubdate || review.sortpubdate, review.elocationid, review.searchTerm, `https://pubmed.ncbi.nlm.nih.gov/${review.uid}/`, JSON.stringify(review),
      // INSERT binds (9)
      id, review.title, authors, review.fulljournalname, review.pubdate || review.sortpubdate, review.elocationid, review.searchTerm, `https://pubmed.ncbi.nlm.nih.gov/${review.uid}/`, JSON.stringify(review),
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
