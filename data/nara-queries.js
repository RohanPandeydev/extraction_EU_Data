const { query } = require("./database");

async function createTableNARA() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS nara_implant_registry (
      id SERIAL PRIMARY KEY,
      registry_id VARCHAR(255) UNIQUE,
      device_name VARCHAR(500),
      manufacturer_name VARCHAR(500),
      implant_type VARCHAR(255),
      country_code VARCHAR(10),
      report_year INT,
      total_procedures INT,
      revision_rate VARCHAR(50),
      survival_rate VARCHAR(50),
      raw_data JSONB,
      source_url TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table nara_implant_registry created successfully");
}

async function insertNARARecord(data) {
  const queryText = `
    INSERT INTO nara_implant_registry (
      registry_id, device_name, manufacturer_name, implant_type,
      country_code, report_year, total_procedures, revision_rate,
      survival_rate, raw_data, source_url
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (registry_id) DO NOTHING
  `;
  const values = [
    data.registryId,
    data.deviceName,
    data.manufacturerName,
    data.implantType,
    data.countryCode,
    data.reportYear,
    data.totalProcedures,
    data.revisionRate,
    data.survivalRate,
    JSON.stringify(data),
    data.sourceUrl,
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting NARA record:", error.message);
  }
}

module.exports = { createTableNARA, insertNARARecord };
