require("dotenv").config();
const cheerio = require("cheerio");
const { fetchHTML } = require("../../utils/httpClient");
const { createTableSCENIHR, insertSCENIHROpinion } = require("../../data/scenihr-queries");

// SCENIHR merged into SCHEER
const SCHEER_URL = "https://health.ec.europa.eu/scientific-committees/scientific-committee-health-environmental-and-emerging-risks-scheer/scheer-opinions_en";

async function run() {
  console.log("=== SCHEER (formerly SCENIHR) Scientific Opinions ===");
  await createTableSCENIHR();

  console.log("Fetching SCHEER opinions page...");
  try {
    const html = await fetchHTML(SCHEER_URL, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        Accept: "text/html",
        "Accept-Language": "en-GB,en;q=0.9",
      },
    });
    const $ = cheerio.load(html);
    let totalInserted = 0;

    // Parse all opinion entries - they are grouped under H2 topic headings
    let currentTopic = "General";

    $("h2, h3, li, p, a").each((_, el) => {
      const tag = $(el).prop("tagName").toLowerCase();

      if (tag === "h2") {
        currentTopic = $(el).text().trim();
        return;
      }

      // Look for opinion links
      if (tag === "a" || tag === "li" || tag === "p") {
        const linkEl = tag === "a" ? $(el) : $(el).find("a").first();
        const title = linkEl.text().trim();
        const link = linkEl.attr("href");

        if (!title || title.length < 10) return;
        if (!link) return;

        // Filter for device-related content
        const lower = title.toLowerCase() + " " + currentTopic.toLowerCase();
        const isDeviceRelated =
          lower.includes("device") ||
          lower.includes("implant") ||
          lower.includes("prosth") ||
          lower.includes("biocompat") ||
          lower.includes("nanomaterial") ||
          lower.includes("silicone") ||
          lower.includes("metal") ||
          lower.includes("surgical") ||
          lower.includes("medical") ||
          lower.includes("phthalate") ||
          lower.includes("brain stimulat") ||
          lower.includes("annex xvi") ||
          lower.includes("tattoo") ||
          lower.includes("tissue");

        if (!isDeviceRelated) return;

        // Extract adoption date from text (e.g., "Adopted on 14 June 2024")
        const dateMatch = title.match(/(?:adopted|adoption)\s+(?:on\s+)?(\d{1,2}\s+\w+\s+\d{4})/i);
        const parentText = $(el).parent().text();
        const parentDateMatch = parentText.match(/(?:adopted|adoption)\s+(?:on\s+)?(\d{1,2}\s+\w+\s+\d{4})/i);

        const fullUrl = link.startsWith("http") ? link : `https://health.ec.europa.eu${link}`;

        insertSCENIHROpinion({
          opinionId: link || `scheer_${title.substring(0, 80)}`,
          title,
          committeeName: "SCHEER",
          adoptionDate: dateMatch?.[1] || parentDateMatch?.[1] || null,
          deviceCategory: currentTopic,
          pdfUrl: fullUrl,
        });
        totalInserted++;
      }
    });

    console.log(`Inserted ${totalInserted} device-related SCHEER opinions`);
  } catch (error) {
    console.error("Error fetching SCHEER opinions:", error.message);
  }

  console.log("=== SCHEER extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
