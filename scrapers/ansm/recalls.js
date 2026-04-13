require("dotenv").config();
const cheerio = require("cheerio");
const { fetchHTML } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { createTableANSM, insertANSMRecord } = require("../../data/national-queries");

const BASE_URL = "https://ansm.sante.fr";

// ANSM has two relevant pages:
// 1. Supply availability: /disponibilites-des-produits-de-sante/dispositifs-medicaux
// 2. Safety information: /informations-de-securite (FSNs and recalls)

async function fetchANSMSupplyData() {
  const url = `${BASE_URL}/disponibilites-des-produits-de-sante/dispositifs-medicaux`;
  console.log("Fetching ANSM supply availability data...");
  try {
    const html = await fetchHTML(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        Accept: "text/html",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
      },
    });
    const $ = cheerio.load(html);
    const records = [];

    // Parse the table on the supply availability page
    $("table tbody tr, .table-responsive tbody tr").each((_, el) => {
      const cells = $(el).find("td");
      if (cells.length >= 4) {
        const status = $(cells[0]).text().trim();
        const updateDate = $(cells[1]).text().trim();
        const deviceType = $(cells[2]).text().trim();
        const deviceName = $(cells[3]).text().trim();
        const returnDate = cells.length > 4 ? $(cells[4]).text().trim() : null;

        if (deviceName) {
          // Split device name and manufacturer (format: "Device – Manufacturer")
          const parts = deviceName.split(" – ");
          const name = parts[0]?.trim();
          const manufacturer = parts[1]?.trim() || null;

          records.push({
            sourceId: `ansm_supply_${deviceName.substring(0, 80)}_${updateDate}`,
            title: `[${status}] ${deviceName}`,
            deviceName: name,
            manufacturerName: manufacturer,
            recallDate: updateDate,
            recallType: status, // RUPTURE, ARRÊT DE COMMERCIALISATION, REMISE À DISPOSITION, TENSION
            description: `Type: ${deviceType}. Return date: ${returnDate || "N/A"}`,
            sourceUrl: url,
          });
        }
      }
    });

    return records;
  } catch (error) {
    console.error("Error fetching ANSM supply data:", error.message);
    return [];
  }
}

async function fetchANSMSafetyInfo(page) {
  const url = `${BASE_URL}/informations-de-securite?page=${page}`;
  try {
    const html = await fetchHTML(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        Accept: "text/html",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
      },
    });
    const $ = cheerio.load(html);
    const records = [];

    $(".views-row, article, .node--type-article, .search-result, li.result, .content-item").each((_, el) => {
      const titleEl = $(el).find("h2 a, h3 a, .title a, a").first();
      const title = titleEl.text().trim();
      const link = titleEl.attr("href");
      const date = $(el).find(".date, time, .field-date, .meta-date").first().text().trim();
      const description = $(el).find(".field-body, p, .summary, .teaser").first().text().trim();

      if (title && title.length > 5) {
        records.push({
          sourceId: link || `ansm_safety_${title.substring(0, 80)}_${date}`,
          title,
          deviceName: title,
          recallDate: date,
          recallType: "Safety Information",
          description,
          sourceUrl: link ? (link.startsWith("http") ? link : `${BASE_URL}${link}`) : url,
        });
      }
    });

    return records;
  } catch (error) {
    console.error(`Error fetching ANSM safety page ${page}:`, error.message);
    return [];
  }
}

async function run() {
  console.log("=== ANSM (France) Device Safety Data ===");
  await createTableANSM();

  // Part 1: Supply availability table
  const supplyRecords = await fetchANSMSupplyData();
  for (const record of supplyRecords) {
    await insertANSMRecord(record);
  }
  console.log(`Inserted ${supplyRecords.length} supply availability records`);

  // Part 2: Safety information pages
  let page = 0;
  let totalSafety = 0;
  let hasMore = true;

  while (hasMore) {
    console.log(`Fetching ANSM safety info page ${page + 1}...`);
    const records = await fetchANSMSafetyInfo(page);

    if (records.length === 0) {
      hasMore = false;
      break;
    }

    for (const record of records) {
      await insertANSMRecord(record);
    }
    totalSafety += records.length;
    console.log(`Page ${page + 1} done — found ${records.length} safety records`);
    page++;
    await sleep(5000, 5000);

    if (page > 100) break;
  }

  console.log(`Total ANSM records: ${supplyRecords.length} supply + ${totalSafety} safety`);
  console.log("=== ANSM extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
