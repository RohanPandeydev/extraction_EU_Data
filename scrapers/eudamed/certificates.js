require("dotenv").config();
const { fetchWithEudamedHeaders } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { createTableCertificates, insertCertificateData } = require("../../data/eudamed-queries");

const BASE_URL = "https://ec.europa.eu/tools/eudamed/api";

async function fetchCertificatesPage(page, pageSize) {
  const url = `${BASE_URL}/certificates/search/?page=${page}&pageSize=${pageSize}&languageIso2Code=en`;
  const data = await fetchWithEudamedHeaders(url);
  return {
    contents: data.content || [],
    totalPages: data.totalPages || 0,
    totalElements: data.totalElements || 0,
  };
}

async function run() {
  console.log("=== EUDAMED Certificates ===");
  await createTableCertificates();

  const { totalPages, totalElements } = await fetchCertificatesPage(0, 5);
  console.log(`Total pages: ${totalPages}, Total certificates: ${totalElements}`);

  for (let i = 0; i < totalPages; i++) {
    console.log(`Fetching certificates page ${i + 1}/${totalPages}...`);
    const { contents } = await fetchCertificatesPage(i, 300);
    for (const cert of contents) {
      await insertCertificateData(cert);
    }
    console.log(`Page ${i + 1} done — inserted ${contents.length} certificates`);
    await sleep(5000, 5000);
  }
  console.log("=== Certificates extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
