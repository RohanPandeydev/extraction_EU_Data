const { query } = require("./database");

async function createTableWHOAdverseEvents() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS who_adverse_events (
      id SERIAL PRIMARY KEY,
      record_id VARCHAR(255) UNIQUE,
      device_name VARCHAR(500),
      product_type VARCHAR(255),
      country_code VARCHAR(10),
      report_year INT,
      event_type VARCHAR(255),
      outcome VARCHAR(255),
      total_reports INT,
      soc_name VARCHAR(500),
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table who_adverse_events created successfully");
}

async function insertWHOAdverseEvent(data) {
  const queryText = `
    INSERT INTO who_adverse_events (
      record_id, device_name, product_type, country_code, report_year,
      event_type, outcome, total_reports, soc_name, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (record_id) DO NOTHING
  `;
  const values = [
    data.recordId || `${data.deviceName}_${data.socName}_${data.reportYear}`,
    data.deviceName,
    data.productType,
    data.countryCode,
    data.reportYear,
    data.eventType,
    data.outcome,
    data.totalReports || data.count,
    data.socName,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting WHO adverse event:", error.message);
  }
}

module.exports = {
  createTableWHOAdverseEvents,
  insertWHOAdverseEvent,
};
