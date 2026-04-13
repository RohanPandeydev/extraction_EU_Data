require("dotenv").config();
const { fetchJSON } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");
const { createTableCochraneReviews, insertCochraneReview } = require("../../data/cochrane-queries");

// Cochrane Library requires OAuth — use PubMed/NCBI E-utilities API instead
// PubMed indexes all Cochrane reviews and is free/public
const PUBMED_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils";

async function searchPubMed(term, retstart, retmax) {
  const url = `${PUBMED_BASE}/esearch.fcgi?db=pubmed&term=${encodeURIComponent(term)}&retstart=${retstart}&retmax=${retmax}&retmode=json`;
  const data = await fetchJSON(url);
  return {
    ids: data.esearchresult?.idlist || [],
    count: parseInt(data.esearchresult?.count || "0"),
  };
}

async function fetchPubMedDetails(ids) {
  if (ids.length === 0) return [];
  const url = `${PUBMED_BASE}/esummary.fcgi?db=pubmed&id=${ids.join(",")}&retmode=json`;
  const data = await fetchJSON(url);
  const results = [];
  for (const id of ids) {
    const article = data.result?.[id];
    if (article) {
      results.push(article);
    }
  }
  return results;
}

async function run() {
  console.log("=== Cochrane/PubMed Medical Device Reviews ===");
  await createTableCochraneReviews();

  // Search PubMed for Cochrane systematic reviews about medical devices
  const searchTerms = [
    '"Cochrane Database Syst Rev"[Journal] AND "medical device"',
    '"Cochrane Database Syst Rev"[Journal] AND "implant"',
    '"Cochrane Database Syst Rev"[Journal] AND "prosthesis"',
    '"Cochrane Database Syst Rev"[Journal] AND "stent"',
    '"Cochrane Database Syst Rev"[Journal] AND "pacemaker"',
    '"Cochrane Database Syst Rev"[Journal] AND "surgical mesh"',
    '"Cochrane Database Syst Rev"[Journal] AND "hip replacement"',
    '"Cochrane Database Syst Rev"[Journal] AND "knee replacement"',
    '"Cochrane Database Syst Rev"[Journal] AND "defibrillator"',
    '"Cochrane Database Syst Rev"[Journal] AND "ventilator"',
    '"Cochrane Database Syst Rev"[Journal] AND "insulin pump"',
    '"Cochrane Database Syst Rev"[Journal] AND "heart valve"',
    '"Cochrane Database Syst Rev"[Journal] AND "cochlear implant"',
    '"Cochrane Database Syst Rev"[Journal] AND "dental implant"',
    '"Cochrane Database Syst Rev"[Journal] AND "dialysis"',
  ];

  let totalInserted = 0;

  for (const term of searchTerms) {
    const shortTerm = term.split("AND")[1]?.trim().replace(/"/g, "") || term;
    console.log(`Searching PubMed for Cochrane reviews: ${shortTerm}...`);

    const { ids, count } = await searchPubMed(term, 0, 100);
    if (ids.length === 0) {
      console.log(`  No results for ${shortTerm}`);
      await sleep(1000, 500); // PubMed rate limit: 3 requests/sec without API key
      continue;
    }

    console.log(`  Found ${count} results, fetching details for ${ids.length}...`);
    await sleep(500, 500);

    const articles = await fetchPubMedDetails(ids);
    for (const article of articles) {
      const authors = article.authors?.map((a) => a.name).join(", ") || "";
      const doi = article.elocationid?.replace("doi: ", "") || article.articleids?.find((a) => a.idtype === "doi")?.value;

      await insertCochraneReview({
        reviewId: `pubmed_${article.uid}`,
        doi,
        title: article.title,
        authors,
        publicationDate: article.pubdate || article.sortpubdate,
        deviceKeywords: shortTerm,
        abstract: article.abstract || null,
        reviewType: "Cochrane Systematic Review",
        sourceUrl: `https://pubmed.ncbi.nlm.nih.gov/${article.uid}/`,
      });
      totalInserted++;
    }

    console.log(`  Inserted ${articles.length} reviews for ${shortTerm}`);
    await sleep(1000, 500); // Respect PubMed rate limits
  }

  console.log(`Total Cochrane/PubMed reviews inserted: ${totalInserted}`);
  console.log("=== Cochrane/PubMed extraction complete ===");
}

module.exports = { run };

if (require.main === module) {
  run().catch(console.error);
}
