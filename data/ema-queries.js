const { query } = require("./database");

async function createTableEMACombinationProducts() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS ema_combination_products (
      id SERIAL PRIMARY KEY,
      product_id VARCHAR(255) UNIQUE,
      product_name TEXT,
      active_substance TEXT,
      applicant_name TEXT,
      procedure_number VARCHAR(500),
      opinion_date VARCHAR(255),
      decision_date VARCHAR(255),
      decision_type VARCHAR(255),
      therapeutic_area TEXT,
      atc_code VARCHAR(50),
      product_type VARCHAR(255),
      raw_data JSONB,
      source_url TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table ema_combination_products created successfully");
}

async function insertEMAProduct(data) {
  const queryText = `
    INSERT INTO ema_combination_products (
      product_id, product_name, active_substance, applicant_name,
      procedure_number, opinion_date, decision_date, decision_type,
      therapeutic_area, atc_code, product_type, raw_data, source_url
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (product_id) DO NOTHING
  `;
  const values = [
    data.productId || data.id,
    data.productName || data.medicineName,
    data.activeSubstance,
    data.applicantName || data.marketingAuthorisationHolder,
    data.procedureNumber,
    data.opinionDate,
    data.decisionDate,
    data.decisionType,
    data.therapeuticArea,
    data.atcCode,
    data.productType,
    JSON.stringify(data),
    data.sourceUrl || data.url,
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting EMA product:", error.message);
  }
}

module.exports = {
  createTableEMACombinationProducts,
  insertEMAProduct,
};
