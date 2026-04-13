require("dotenv").config();
const { fetchJSON } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");

const BASE_URL = "https://api.fda.gov/device/recall.json";
const PAGE_SIZE = 100;

/**
 * Fetch device recalls from openFDA API
 * 57,823+ recalls, updated weekly, includes global companies (EU manufacturers selling in US)
 * No API key required
 */
async function fetchRecallsPage(skip = 0, searchQuery = null) {
  let url = `${BASE_URL}?limit=${PAGE_SIZE}&skip=${skip}&sort=event_date_posted:desc`;

  if (searchQuery) {
    url += `&search=${encodeURIComponent(searchQuery)}`;
  }

  try {
    const data = await fetchJSON(url, {
      headers: {
        Accept: "application/json",
        "User-Agent":
          "eudamed-data-extraction/1.0 (https://github.com/openregulatory/eudamed-api)",
      },
    });
    return data;
  } catch (error) {
    console.error(`Error fetching recalls (skip=${skip}):`, error.message);
    return { meta: {}, results: [] };
  }
}

/**
 * Normalize a single recall record
 */
function normalizeRecall(recall) {
  const products = recall.products || [];
  return {
    source: "openFDA",
    cfresId: recall.cfres_id || null,
    productResNumber: recall.product_res_number || null,
    resEventNumber: recall.res_event_number || null,

    // Dates
    dateInitiated: recall.event_date_initiated || null,
    datePosted: recall.event_date_posted || null,
    dateTerminated: recall.event_date_terminated || null,
    recallStatus: recall.recall_status || null,

    // Product info
    productDescription: products[0]?.product_description || null,
    productCode: recall.product_code || null,
    codeInfo: recall.code_info || null,
    productQuantity: recall.product_quantity || null,
    brandName: products[0]?.brand_name || null,
    genericName: products[0]?.generic_name || null,
    modelNumber: products[0]?.model_number || null,
    catalogNumber: products[0]?.catalog_number || null,
    lotNumber: products[0]?.lot_number || null,
    rxOnly: products[0]?.rx_only || null,
    deviceReported: products[0]?.device_reported || null,

    // 510(k) numbers
    kNumbers: recall.k_numbers || [],

    // Manufacturer / Firm
    recallingFirm: recall.recalling_firm || null,
    firmFeiNumber: recall.firm_fei_number || null,
    city: recall.city || null,
    state: recall.state || null,
    country: recall.country || null,
    postalCode: recall.postal_code || null,
    address1: recall.address_1 || null,

    // Recall details
    classification: recall.classification || null,
    reasonForRecall: recall.reason_for_recall || null,
    rootCause: recall.root_cause_description || null,
    action: recall.action || null,
    distributionPattern: recall.distribution_pattern || null,
    additionalContactInfo: recall.additional_info_contact || null,

    // OpenFDA enrichment
    openfda: recall.openfda || null,
  };
}

/**
 * Extract all device recalls from openFDA
 * Supports optional search query to filter (e.g., by firm name, product)
 */
async function extractAllRecalls(outputCb, maxPages = null, searchQuery = null) {
  console.log("=== openFDA: Extracting device recalls ===");
  if (searchQuery) {
    console.log(`Filter: ${searchQuery}`);
  }

  let skip = 0;
  let page = 0;
  let totalExtracted = 0;
  let totalAvailable = null;

  while (true) {
    const data = await fetchRecallsPage(skip, searchQuery);

    if (!data.results || data.results.length === 0) {
      break;
    }

    if (totalAvailable === null && data.meta?.results?.total) {
      totalAvailable = data.meta.results.total;
      console.log(`Total recalls available: ${totalAvailable.toLocaleString()}`);
      console.log(`API last updated: ${data.meta.last_updated || "unknown"}`);
    }

    for (const recall of data.results) {
      const normalized = normalizeRecall(recall);
      if (outputCb) await outputCb(normalized);
      totalExtracted++;
    }

    console.log(
      `Page ${page + 1} — extracted ${data.results.length} recalls (total: ${totalExtracted.toLocaleString()}${totalAvailable ? ` / ${totalAvailable.toLocaleString()}` : ""})`
    );

    skip += PAGE_SIZE;
    page++;

    if (maxPages && page >= maxPages) break;
    if (data.results.length < PAGE_SIZE) break;

    await sleep(300, 800); // openFDA rate limit is generous but be polite
  }

  console.log(`Total recalls extracted: ${totalExtracted.toLocaleString()}`);
  return totalExtracted;
}

/**
 * Search recalls by specific criteria (firm name, product, etc.)
 */
async function searchRecalls(searchQuery, limit = 50) {
  const url = `${BASE_URL}?search=${encodeURIComponent(searchQuery)}&limit=${limit}`;

  try {
    const data = await fetchJSON(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "eudamed-data-extraction/1.0",
      },
    });

    console.log(`Search "${searchQuery}": ${data.meta?.results?.total} results`);
    return (data.results || []).map(normalizeRecall);
  } catch (error) {
    console.error(`Search error: ${error.message}`);
    return [];
  }
}

module.exports = {
  extractAllRecalls,
  searchRecalls,
  fetchRecallsPage,
  normalizeRecall,
};

if (require.main === module) {
  (async () => {
    // Quick test: fetch 1 page
    const data = await fetchRecallsPage(0);
    console.log(`Total recalls: ${data.meta?.results?.total?.toLocaleString()}`);
    console.log(`API last updated: ${data.meta?.last_updated}`);
    console.log("\nLatest recall:");
    if (data.results?.length > 0) {
      console.log(JSON.stringify(data.results[0], null, 2));
    }
  })().catch(console.error);
}
