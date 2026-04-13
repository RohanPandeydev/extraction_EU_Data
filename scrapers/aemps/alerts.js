require("dotenv").config();
const cheerio = require("cheerio");
const { fetchHTML } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { createTableAEMPS, insertAEMPSRecord } = require("../../data/national-queries");

const BASE_URL = "https://www.aemps.gob.es";

async function fetchAEMPSPage(page) {
  // WordPress archive with pagination
  const url = `${BASE_URL}/productossanitarios/page/${page}/`;
  try {
    const html = await fetchHTML(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        Accept: "text/html",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
      },
    });
    const $ = cheerio.load(html);
    const records = [];

    $("article, .post, .entry, .hentry, .type-post").each((_, el) => {
      const titleEl = $(el).find("h2 a, h3 a, .entry-title a").first();
      const title = titleEl.text().trim();
      const link = titleEl.attr("href");
      const date = $(el).find("time, .entry-date, .published, .date").first().attr("datetime") ||
                   $(el).find("time, .entry-date, .published, .date").first().text().trim();
      const description = $(el).find(".entry-summary, .entry-content, .excerpt, p").first().text().trim();
      const categories = [];
      $(el).find(".cat-links a, .tag-links a, .entry-categories a, .category a").each((_, cat) => {
        categories.push($(cat).text().trim());
      });

      if (title && title.length > 5) {
        // Extract reference number from description (format: PS, NN/YYYY)
        const refMatch = description.match(/PS,?\s*\d+\/\d{4}/);

        records.push({
          sourceId: refMatch ? refMatch[0] : link || `aemps_${title.substring(0, 80)}`,
          title,
          deviceName: title,
          recallDate: date,
          recallType: "Safety Alert",
          description: description.substring(0, 2000),
          affectedCountries: categories.join(", "),
          sourceUrl: link || url,
        });
      }
    });

    return records;
  } catch (error) {
    // WordPress returns 404 when page is beyond the last page
    if (error.message.includes("404")) return [];
    console.error(`Error fetching AEMPS page ${page}:`, error.message);
    return [];
  }
}

async function run() {
  console.log("=== AEMPS (Spain) Device Safety Alerts ===");
  await createTableAEMPS();

  let page = 1;
  let totalInserted = 0;
  let hasMore = true;

  while (hasMore) {
    console.log(`Fetching AEMPS page ${page}...`);
    const records = await fetchAEMPSPage(page);

    if (records.length === 0) {
      hasMore = false;
      break;
    }

    for (const record of records) {
      await insertAEMPSRecord(record);
    }
    totalInserted += records.length;
    console.log(`Page ${page} done — found ${records.length} alerts (total: ${totalInserted})`);
    page++;
    await sleep(5000, 5000);

    if (page > 60) break; // ~57 pages
  }

  console.log(`Total AEMPS alerts inserted: ${totalInserted}`);
  console.log("=== AEMPS extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
