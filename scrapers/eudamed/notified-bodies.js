require("dotenv").config();
const { fetchWithEudamedHeaders } = require("../../utils/httpClient");
const { query } = require("../../data/database");

async function createTableNotifiedBodies() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS eudamed_notified_bodies (
      id SERIAL PRIMARY KEY,
      nb_uuid VARCHAR(255) UNIQUE,
      ulid VARCHAR(26),
      name VARCHAR(500),
      srn VARCHAR(50),
      notified_body_number VARCHAR(50),
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
  console.log("Table eudamed_notified_bodies created successfully");
}

async function insertNotifiedBody(data) {
  const queryText = `
    INSERT INTO eudamed_notified_bodies (
      nb_uuid, ulid, name, srn, notified_body_number, status_code,
      country_iso2_code, country_name, geographical_address,
      electronic_mail, telephone, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT (nb_uuid) DO NOTHING
  `;
  const values = [
    data.uuid,
    data.ulid,
    data.name,
    data.srn,
    data.notifiedBodyNumber || data.nbNumber,
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
    console.error("Error inserting notified body:", error.message);
  }
}

async function run() {
  console.log("=== EUDAMED Notified Bodies ===");
  await createTableNotifiedBodies();

  const url = "https://ec.europa.eu/tools/eudamed/api/ses/notifiedBodies";
  console.log("Fetching notified bodies...");

  try {
    const data = await fetchWithEudamedHeaders(url);
    const bodies = Array.isArray(data) ? data : data.content || [data];

    for (const nb of bodies) {
      await insertNotifiedBody(nb);
    }
    console.log(`Inserted ${bodies.length} notified bodies`);
  } catch (error) {
    console.error("Error fetching notified bodies:", error.message);
  }

  console.log("=== Notified Bodies extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
