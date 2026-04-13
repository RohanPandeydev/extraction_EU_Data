require("dotenv").config();
const cheerio = require("cheerio");
const { fetchHTML } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { createTableIGJ, insertIGJRecord } = require("../../data/national-queries");

const BASE_URL = "https://www.igj.nl";
const LISTING_URL = `${BASE_URL}/documenten`;

async function fetchIGJPage(page) {
  const params = new URLSearchParams({
    "filters[0][field]": "information_type",
    "filters[0][values][0]": "Waarschuwing",
    "filters[0][type]": "all",
    page: page.toString(),
  });

  const url = `${LISTING_URL}?${params.toString()}`;
  try {
    const html = await fetchHTML(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        Accept: "text/html",
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
      },
    });
    const $ = cheerio.load(html);
    const records = [];

    $("li.result, .search-result, .document-item, article, .overzicht-item").each((_, el) => {
      const titleEl = $(el).find("h3 a, h2 a, .title a, a").first();
      const rawTitle = titleEl.text().trim();
      const link = titleEl.attr("href");
      const date = $(el).find(".date, time, .meta-date, .document-date").first().text().trim();
      const description = $(el).find("p, .description, .snippet, .summary").first().text().trim();

      if (rawTitle && rawTitle.length > 5) {
        // IGJ title format: "Manufacturer, FSN-YYYY-NNN, Product Name"
        const parts = rawTitle.split(",").map((s) => s.trim());
        const manufacturer = parts.length >= 3 ? parts[0] : null;
        const fsnRef = parts.length >= 3 ? parts[1] : null;
        const productName = parts.length >= 3 ? parts.slice(2).join(", ") : rawTitle;

        records.push({
          sourceId: fsnRef || link || `igj_${rawTitle.substring(0, 80)}`,
          title: rawTitle,
          deviceName: productName,
          manufacturerName: manufacturer,
          recallDate: date,
          recallType: "Field Safety Notice",
          description,
          sourceUrl: link ? (link.startsWith("http") ? link : `${BASE_URL}${link}`) : url,
        });
      }
    });

    return records;
  } catch (error) {
    console.error(`Error fetching IGJ page ${page}:`, error.message);
    return [];
  }
}

async function run() {
  console.log("=== IGJ (Netherlands) Safety Notices ===");
  await createTableIGJ();

  let page = 1;
  let totalInserted = 0;
  let hasMore = true;

  while (hasMore) {
    console.log(`Fetching IGJ page ${page}...`);
    const records = await fetchIGJPage(page);

    if (records.length === 0) {
      hasMore = false;
      break;
    }

    for (const record of records) {
      await insertIGJRecord(record);
    }
    totalInserted += records.length;
    console.log(`Page ${page} done — found ${records.length} notices (total: ${totalInserted})`);
    page++;
    await sleep(5000, 5000);

    if (page > 200) break; // 1,934 / 10 per page = ~194 pages
  }

  console.log(`Total IGJ notices inserted: ${totalInserted}`);
  console.log("=== IGJ extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
