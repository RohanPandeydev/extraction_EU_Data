require("dotenv").config();
const cheerio = require("cheerio");
const { fetchHTML } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { createTableBfarm, insertBfarmRecord } = require("../../data/national-queries");

const BASE_URL = "https://www.bfarm.de";
const SEARCH_URL = `${BASE_URL}/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html`;

async function fetchBfarmPage(page) {
  const params = new URLSearchParams({
    nn: "708434",
    cl2Categories_Format: "kundeninfo",
    resultsPerPage: "100",
    sortOrder: "dateOfIssue_dt+desc",
    gtp: `${page * 100}_%2F${BASE_URL}/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html`,
  });

  const url = `${SEARCH_URL}?${params.toString()}`;
  try {
    const html = await fetchHTML(url);
    const $ = cheerio.load(html);
    const records = [];

    $(".search-result, .result-list li, .c-search-result, article").each((_, el) => {
      const titleEl = $(el).find("h3 a, h2 a, .headline a, a.title").first();
      const title = titleEl.text().trim();
      const link = titleEl.attr("href");
      const date = $(el).find(".date, time, .c-search-result__date, .meta").first().text().trim();
      const snippet = $(el).find("p, .snippet, .c-search-result__text, .description").first().text().trim();
      const productGroup = $(el).find(".category, .product-group, .c-search-result__category").first().text().trim();

      if (title && title.length > 5) {
        records.push({
          sourceId: link || `bfarm_${title.substring(0, 80)}_${date}`,
          title,
          deviceName: title,
          manufacturerName: extractManufacturer(title),
          recallDate: date,
          recallType: "Field Safety Corrective Action",
          riskLevel: null,
          description: snippet,
          affectedCountries: productGroup,
          correctiveAction: null,
          sourceUrl: link ? (link.startsWith("http") ? link : `${BASE_URL}${link}`) : url,
        });
      }
    });

    return records;
  } catch (error) {
    console.error(`Error fetching BfArM page ${page}:`, error.message);
    return [];
  }
}

function extractManufacturer(title) {
  // BfArM titles often end with "by [Manufacturer]"
  const byMatch = title.match(/\bby\s+(.+?)$/i);
  if (byMatch) return byMatch[1].trim();
  return null;
}

async function run() {
  console.log("=== BfArM (Germany) Field Safety Notices ===");
  await createTableBfarm();

  let page = 0;
  let totalInserted = 0;
  let hasMore = true;

  while (hasMore) {
    console.log(`Fetching BfArM page ${page + 1}...`);
    const records = await fetchBfarmPage(page);

    if (records.length === 0) {
      hasMore = false;
      break;
    }

    for (const record of records) {
      await insertBfarmRecord(record);
    }
    totalInserted += records.length;
    console.log(`Page ${page + 1} done — found ${records.length} notices (total: ${totalInserted})`);
    page++;
    await sleep(5000, 5000);

    if (page > 200) break; // ~16,700 / 100 per page
  }

  console.log(`Total BfArM notices inserted: ${totalInserted}`);
  console.log("=== BfArM extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
