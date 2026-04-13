require("dotenv").config();
const { fetchWithEudamedHeaders } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { query } = require("../../data/database");

async function createTableEconomicOperators() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS eudamed_economic_operators (
      id SERIAL PRIMARY KEY,
      eo_uuid VARCHAR(255) UNIQUE,
      ulid VARCHAR(26),
      name VARCHAR(500),
      abbreviated_name VARCHAR(500),
      srn VARCHAR(50),
      actor_type_code VARCHAR(255),
      status_code VARCHAR(255),
      country_iso2_code VARCHAR(10),
      country_name VARCHAR(255),
      geographical_address TEXT,
      electronic_mail VARCHAR(255),
      telephone VARCHAR(50),
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table eudamed_economic_operators created successfully");
}

async function insertEconomicOperator(data) {
  const queryText = `
    INSERT INTO eudamed_economic_operators (
      eo_uuid, ulid, name, abbreviated_name, srn, actor_type_code,
      status_code, country_iso2_code, country_name, geographical_address,
      electronic_mail, telephone, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (eo_uuid) DO NOTHING
  `;
  const values = [
    data.uuid,
    data.ulid,
    data.name,
    data.abbreviatedName,
    data.srn,
    data.actorType?.code || data.actorTypeCode,
    data.status?.code || data.statusCode,
    data.countryIso2Code,
    data.countryName,
    data.geographicalAddress,
    data.electronicMail,
    data.telephone,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting economic operator:", error.message);
  }
}

async function fetchEOPage(page, pageSize) {
  const url = `https://ec.europa.eu/tools/eudamed/api/eos?page=${page}&pageSize=${pageSize}&languageIso2Code=en`;
  const data = await fetchWithEudamedHeaders(url);
  return {
    contents: data.content || [],
    totalPages: data.totalPages || 0,
    totalElements: data.totalElements || 0,
  };
}

async function run() {
  console.log("=== EUDAMED Economic Operators ===");
  await createTableEconomicOperators();

  const { totalPages, totalElements } = await fetchEOPage(0, 5);
  console.log(`Total pages: ${totalPages}, Total economic operators: ${totalElements}`);

  for (let i = 0; i < totalPages; i++) {
    console.log(`Fetching EO page ${i + 1}/${totalPages}...`);
    const { contents } = await fetchEOPage(i, 300);
    for (const eo of contents) {
      await insertEconomicOperator(eo);
    }
    console.log(`Page ${i + 1} done — inserted ${contents.length} operators`);
    await sleep(5000, 5000);
  }
  console.log("=== Economic Operators extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
