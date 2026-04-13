require("dotenv").config();
const { fetchJSON } = require("../../utils/httpClient");
const { createTableEMACombinationProducts, insertEMAProduct } = require("../../data/ema-queries");

const EMA_JSON_URL = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json";

async function run() {
  console.log("=== EMA Medicines Data (JSON API) ===");
  await createTableEMACombinationProducts();

  console.log("Fetching EMA medicines JSON (this may take a moment, ~6MB)...");
  const data = await fetchJSON(EMA_JSON_URL, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
      Accept: "application/json",
    },
  });

  const records = data.data || data;
  console.log(`Total EMA records: ${records.length}`);

  let inserted = 0;
  for (const med of records) {
    // Filter for human medicines only (includes combination products)
    if (med.category !== "Human") continue;

    await insertEMAProduct({
      productId: med.ema_product_number || med.name_of_medicine,
      productName: med.name_of_medicine,
      activeSubstance: med.active_substance,
      applicantName: med.marketing_authorisation_developer_applicant_holder,
      procedureNumber: med.latest_procedure_affecting_product_information,
      opinionDate: med.opinion_adopted_date,
      decisionDate: med.european_commission_decision_date,
      decisionType: med.medicine_status,
      therapeuticArea: med.therapeutic_area_mesh,
      atcCode: med.atc_code_human,
      productType: [
        med.biosimilar === "Yes" ? "Biosimilar" : null,
        med.generic === "Yes" ? "Generic" : null,
        med.orphan_medicine === "Yes" ? "Orphan" : null,
        med.advanced_therapy === "Yes" ? "ATMP" : null,
        med.conditional_approval === "Yes" ? "Conditional" : null,
      ].filter(Boolean).join(", ") || "Standard",
      sourceUrl: med.medicine_url,
      // Extra fields stored in raw_data
      internationalNonProprietaryName: med.international_non_proprietary_name_common_name,
      pharmacotherapeuticGroup: med.pharmacotherapeutic_group_human,
      therapeuticIndication: med.therapeutic_indication,
      acceleratedAssessment: med.accelerated_assessment,
      additionalMonitoring: med.additional_monitoring,
      exceptionalCircumstances: med.exceptional_circumstances,
      primePriorityMedicine: med.prime_priority_medicine,
      marketingAuthorisationDate: med.marketing_authorisation_date,
      firstPublishedDate: med.first_published_date,
      lastUpdatedDate: med.last_updated_date,
      revisionNumber: med.revision_number,
    });
    inserted++;
  }

  console.log(`Inserted ${inserted} human medicines`);
  console.log("=== EMA extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
