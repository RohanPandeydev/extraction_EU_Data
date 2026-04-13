const { query } = require("./database");

async function createTableSCENIHR() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS scenihr_opinions (
      id SERIAL PRIMARY KEY,
      opinion_id VARCHAR(255) UNIQUE,
      title TEXT,
      committee_name VARCHAR(100),
      adoption_date VARCHAR(255),
      device_category VARCHAR(500),
      safety_conclusion TEXT,
      pdf_url TEXT,
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table scenihr_opinions created successfully");
}

async function insertSCENIHROpinion(data) {
  const queryText = `
    INSERT INTO scenihr_opinions (
      opinion_id, title, committee_name, adoption_date,
      device_category, safety_conclusion, pdf_url, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (opinion_id) DO NOTHING
  `;
  const values = [
    data.opinionId,
    data.title,
    data.committeeName,
    data.adoptionDate,
    data.deviceCategory,
    data.safetyConclusion,
    data.pdfUrl,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting SCENIHR opinion:", error.message);
  }
}

module.exports = { createTableSCENIHR, insertSCENIHROpinion };
