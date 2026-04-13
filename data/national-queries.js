const { query } = require("./database");

// Shared schema for all national regulator recall/alert tables
async function createNationalTable(tableName) {
  const queryText = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id SERIAL PRIMARY KEY,
      source_id TEXT UNIQUE,
      title TEXT,
      device_name TEXT,
      manufacturer_name TEXT,
      recall_date VARCHAR(255),
      recall_type VARCHAR(255),
      risk_level VARCHAR(255),
      description TEXT,
      affected_countries TEXT,
      corrective_action TEXT,
      language_code VARCHAR(5),
      raw_data JSONB,
      source_url TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log(`Table ${tableName} created successfully`);
}

async function insertNationalRecord(tableName, data) {
  const queryText = `
    INSERT INTO ${tableName} (
      source_id, title, device_name, manufacturer_name, recall_date,
      recall_type, risk_level, description, affected_countries,
      corrective_action, language_code, raw_data, source_url
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (source_id) DO NOTHING
  `;
  const values = [
    data.sourceId,
    data.title,
    data.deviceName,
    data.manufacturerName,
    data.recallDate,
    data.recallType,
    data.riskLevel,
    data.description,
    data.affectedCountries,
    data.correctiveAction,
    data.languageCode,
    JSON.stringify(data),
    data.sourceUrl,
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error(`Error inserting into ${tableName}:`, error.message);
  }
}

// BfArM (Germany)
const createTableBfarm = () => createNationalTable("bfarm_recalls");
const insertBfarmRecord = (data) =>
  insertNationalRecord("bfarm_recalls", { ...data, languageCode: "de" });

// ANSM (France)
const createTableANSM = () => createNationalTable("ansm_decisions");
const insertANSMRecord = (data) =>
  insertNationalRecord("ansm_decisions", { ...data, languageCode: "fr" });

// ISS (Italy)
const createTableISS = () => createNationalTable("iss_safety_data");
const insertISSRecord = (data) =>
  insertNationalRecord("iss_safety_data", { ...data, languageCode: "it" });

// IGJ (Netherlands)
const createTableIGJ = () => createNationalTable("igj_recalls");
const insertIGJRecord = (data) =>
  insertNationalRecord("igj_recalls", { ...data, languageCode: "nl" });

// AEMPS (Spain)
const createTableAEMPS = () => createNationalTable("aemps_alerts");
const insertAEMPSRecord = (data) =>
  insertNationalRecord("aemps_alerts", { ...data, languageCode: "es" });

module.exports = {
  createTableBfarm,
  insertBfarmRecord,
  createTableANSM,
  insertANSMRecord,
  createTableISS,
  insertISSRecord,
  createTableIGJ,
  insertIGJRecord,
  createTableAEMPS,
  insertAEMPSRecord,
};
