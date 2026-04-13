require("dotenv").config();
const cheerio = require("cheerio");
const { fetchHTML } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { createTableISS, insertISSRecord } = require("../../data/national-queries");

const BASE_URL = "https://www.iss.it";

async function fetchISSData(page) {
  const url = `${BASE_URL}/dispositivi-medici?page=${page}`;
  try {
    const html = await fetchHTML(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        Accept: "text/html",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
      },
    });
    const $ = cheerio.load(html);
    const records = [];

    $(".views-row, article, .node, .list-item, li, .content-item").each((_, el) => {
      const titleEl = $(el).find("h2 a, h3 a, .title a, a").first();
      const title = titleEl.text().trim();
      const link = titleEl.attr("href");
      const date = $(el).find(".date, time, .field-date").first().text().trim();
      const description = $(el).find(".field-body, p, .summary").first().text().trim();

      if (title && title.length > 5) {
        records.push({
          sourceId: link || `iss_${title.substring(0, 80)}_${date}`,
          title,
          deviceName: title,
          recallDate: date,
          description,
          sourceUrl: link ? (link.startsWith("http") ? link : `${BASE_URL}${link}`) : url,
        });
      }
    });

    return records;
  } catch (error) {
    console.error(`Error fetching ISS page ${page}:`, error.message);
    return [];
  }
}

async function run() {
  console.log("=== ISS (Italy) Device Safety Data ===");
  await createTableISS();

  let page = 0;
  let totalInserted = 0;
  let hasMore = true;

  while (hasMore) {
    console.log(`Fetching ISS page ${page + 1}...`);
    const records = await fetchISSData(page);

    if (records.length === 0) {
      hasMore = false;
      break;
    }

    for (const record of records) {
      await insertISSRecord(record);
    }
    totalInserted += records.length;
    console.log(`Page ${page + 1} done — found ${records.length} records (total: ${totalInserted})`);
    page++;
    await sleep(5000, 5000);

    if (page > 50) break;
  }

  console.log(`Total ISS records inserted: ${totalInserted}`);
  console.log("=== ISS extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
