require("dotenv").config();
const { fetchWithEudamedHeaders } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { query } = require("../../data/database");

async function createTableRefusedApplications() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS eudamed_refused_applications (
      id SERIAL PRIMARY KEY,
      app_uuid VARCHAR(255) UNIQUE,
      ulid VARCHAR(26),
      notified_body_srn VARCHAR(50),
      actor_srn VARCHAR(50),
      actor_name VARCHAR(500),
      application_reference_number VARCHAR(255),
      conformity_assessment_procedure VARCHAR(255),
      decision_code VARCHAR(255),
      decision_date VARCHAR(255),
      certificate_refusal_date VARCHAR(255),
      last_update_date VARCHAR(255),
      raw_data JSONB,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table eudamed_refused_applications created successfully");
}

async function insertRefusedApplication(data) {
  const queryText = `
    INSERT INTO eudamed_refused_applications (
      app_uuid, ulid, notified_body_srn, actor_srn, actor_name,
      application_reference_number, conformity_assessment_procedure,
      decision_code, decision_date, certificate_refusal_date,
      last_update_date, raw_data
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT (app_uuid) DO NOTHING
  `;
  const values = [
    data.uuid,
    data.ulid,
    data.notifiedBodySrn,
    data.actorSrn,
    data.actorName,
    data.applicationReferenceNumber,
    data.conformityAssessmentProcedure?.code || data.conformityAssessmentProcedure,
    data.decision?.code || data.decision,
    data.decisionDate,
    data.certificateRefusalDate,
    data.lastUpdateDate,
    JSON.stringify(data),
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting refused application:", error.message);
  }
}

async function fetchApplicationsPage(page, pageSize) {
  const url = `https://ec.europa.eu/tools/eudamed/api/applications/search/?page=${page}&pageSize=${pageSize}&languageIso2Code=en`;
  const data = await fetchWithEudamedHeaders(url);
  return {
    contents: data.content || [],
    totalPages: data.totalPages || 0,
    totalElements: data.totalElements || 0,
  };
}

async function run() {
  console.log("=== EUDAMED Refused/Withdrawn Applications ===");
  await createTableRefusedApplications();

  const { totalPages, totalElements } = await fetchApplicationsPage(0, 5);
  console.log(`Total pages: ${totalPages}, Total applications: ${totalElements}`);

  for (let i = 0; i < totalPages; i++) {
    console.log(`Fetching applications page ${i + 1}/${totalPages}...`);
    const { contents } = await fetchApplicationsPage(i, 100);
    for (const app of contents) {
      await insertRefusedApplication(app);
    }
    console.log(`Page ${i + 1} done — inserted ${contents.length} applications`);
    await sleep(3000, 3000);
  }
  console.log("=== Refused Applications extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
