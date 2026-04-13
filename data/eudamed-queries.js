const { query } = require("./database");

// === VIGILANCE / ADVERSE EVENTS ===
async function createTableVigilance() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS eudamed_vigilance (
      id SERIAL PRIMARY KEY,
      report_uuid VARCHAR(255) UNIQUE,
      ulid VARCHAR(26),
      report_type VARCHAR(255),
      event_description TEXT,
      event_date VARCHAR(255),
      device_name VARCHAR(500),
      manufacturer_srn VARCHAR(50),
      manufacturer_name VARCHAR(500),
      country_iso2_code VARCHAR(10),
      report_status VARCHAR(255),
      risk_class_code VARCHAR(255),
      basic_udi VARCHAR(255),
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table eudamed_vigilance created successfully");
}

async function insertVigilanceData(data) {
  const queryText = `
    INSERT INTO eudamed_vigilance (
      report_uuid, ulid, report_type, event_description, event_date,
      device_name, manufacturer_srn, manufacturer_name, country_iso2_code,
      report_status, risk_class_code, basic_udi, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (report_uuid) DO NOTHING
  `;
  const values = [
    data.uuid || data.reportUuid,
    data.ulid,
    data.reportType?.code || data.reportType,
    data.eventDescription || data.description,
    data.eventDate || data.reportDate,
    data.deviceName || data.tradeName,
    data.manufacturerSrn,
    data.manufacturerName,
    data.countryIso2Code || data.country,
    data.reportStatus?.code || data.status,
    data.riskClass?.code || data.riskClassCode,
    data.basicUdi,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting vigilance data:", error.message);
  }
}

// === CLINICAL INVESTIGATIONS ===
async function createTableClinicalInvestigations() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS eudamed_clinical_investigations (
      id SERIAL PRIMARY KEY,
      investigation_uuid VARCHAR(255) UNIQUE,
      ulid VARCHAR(26),
      title TEXT,
      sponsor_name VARCHAR(500),
      status_code VARCHAR(255),
      start_date VARCHAR(255),
      end_date VARCHAR(255),
      country_iso2_code VARCHAR(10),
      device_name VARCHAR(500),
      manufacturer_name VARCHAR(500),
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table eudamed_clinical_investigations created successfully");
}

async function insertClinicalInvestigationData(data) {
  const queryText = `
    INSERT INTO eudamed_clinical_investigations (
      investigation_uuid, ulid, title, sponsor_name, status_code,
      start_date, end_date, country_iso2_code, device_name, manufacturer_name, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (investigation_uuid) DO NOTHING
  `;
  const values = [
    data.uuid,
    data.ulid,
    data.title || data.ciTitle,
    data.sponsorName || data.sponsor?.name,
    data.status?.code || data.statusCode,
    data.startDate,
    data.endDate,
    data.countryIso2Code || data.country,
    data.deviceName,
    data.manufacturerName || data.sponsor?.name,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error(
      "Error inserting clinical investigation data:",
      error.message,
    );
  }
}

// === CERTIFICATES ===
async function createTableCertificates() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS eudamed_certificates (
      id SERIAL PRIMARY KEY,
      certificate_uuid VARCHAR(255) UNIQUE,
      ulid VARCHAR(26),
      certificate_type VARCHAR(255),
      notified_body_srn VARCHAR(50),
      notified_body_name VARCHAR(500),
      valid_from VARCHAR(255),
      valid_to VARCHAR(255),
      status_code VARCHAR(255),
      device_name VARCHAR(500),
      manufacturer_name VARCHAR(500),
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table eudamed_certificates created successfully");
}

async function insertCertificateData(data) {
  const queryText = `
    INSERT INTO eudamed_certificates (
      certificate_uuid, ulid, certificate_type, notified_body_srn,
      notified_body_name, valid_from, valid_to, status_code,
      device_name, manufacturer_name, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (certificate_uuid) DO NOTHING
  `;
  const values = [
    data.uuid,
    data.ulid,
    data.certificateType?.code || data.certificateType,
    data.notifiedBody?.srn || data.notifiedBodySrn,
    data.notifiedBody?.name || data.notifiedBodyName,
    data.validFrom || data.startDate,
    data.validTo || data.endDate,
    data.status?.code || data.statusCode,
    data.deviceName,
    data.manufacturerName,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting certificate data:", error.message);
  }
}

// === FIELD SAFETY CORRECTIVE ACTIONS (FSCA) ===
async function createTableFSCA() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS eudamed_fsca (
      id SERIAL PRIMARY KEY,
      fsca_uuid VARCHAR(255) UNIQUE,
      ulid VARCHAR(26),
      fsca_type VARCHAR(255),
      description TEXT,
      action_date VARCHAR(255),
      country_iso2_code VARCHAR(10),
      manufacturer_srn VARCHAR(50),
      manufacturer_name VARCHAR(500),
      device_name VARCHAR(500),
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table eudamed_fsca created successfully");
}

async function insertFSCAData(data) {
  const queryText = `
    INSERT INTO eudamed_fsca (
      fsca_uuid, ulid, fsca_type, description, action_date,
      country_iso2_code, manufacturer_srn, manufacturer_name, device_name, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (fsca_uuid) DO NOTHING
  `;
  const values = [
    data.uuid,
    data.ulid,
    data.fscaType?.code || data.type,
    data.description || data.fscaDescription,
    data.actionDate || data.date,
    data.countryIso2Code || data.country,
    data.manufacturerSrn,
    data.manufacturerName,
    data.deviceName || data.tradeName,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting FSCA data:", error.message);
  }
}

module.exports = {
  createTableVigilance,
  insertVigilanceData,
  createTableClinicalInvestigations,
  insertClinicalInvestigationData,
  createTableCertificates,
  insertCertificateData,
  createTableFSCA,
  insertFSCAData,
};
