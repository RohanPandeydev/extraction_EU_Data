const { query } = require("./database");

async function createTableDeviceComplete() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS device_complete (
      id SERIAL PRIMARY KEY,
      uuid VARCHAR(255) UNIQUE,
      device_name TEXT,
      manufacturer_name TEXT,
      risk_class VARCHAR(50),
      data JSONB,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table device_complete created successfully");
}

async function insertDeviceComplete(
  uuid,
  deviceName,
  manufacturerName,
  riskClass,
  fullData,
) {
  const queryText = `
    INSERT INTO device_complete (uuid, device_name, manufacturer_name, risk_class, data)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (uuid) DO UPDATE SET
      data = $5,
      device_name = $2,
      manufacturer_name = $3,
      risk_class = $4,
      updated_at = NOW()
  `;
  await query(queryText, [
    uuid,
    deviceName,
    manufacturerName,
    riskClass,
    JSON.stringify(fullData),
  ]);
}

async function getDeviceCompleteCount() {
  try {
    const result = await query("SELECT count(*) FROM device_complete");
    return parseInt(result.rows[0].count);
  } catch {
    return 0;
  }
}

module.exports = {
  createTableDeviceComplete,
  insertDeviceComplete,
  getDeviceCompleteCount,
};
