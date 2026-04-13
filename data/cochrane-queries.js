const { query } = require("./database");

async function createTableCochraneReviews() {
  const queryText = `
    CREATE TABLE IF NOT EXISTS cochrane_reviews (
      id SERIAL PRIMARY KEY,
      review_id VARCHAR(255) UNIQUE,
      doi VARCHAR(255),
      title TEXT,
      authors TEXT,
      publication_date VARCHAR(255),
      device_keywords TEXT,
      conclusion_summary TEXT,
      abstract TEXT,
      review_type VARCHAR(255),
      raw_data JSONB,
      source_url TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await query(queryText);
  console.log("Table cochrane_reviews created successfully");
}

async function insertCochraneReview(data) {
  const queryText = `
    INSERT INTO cochrane_reviews (
      review_id, doi, title, authors, publication_date,
      device_keywords, conclusion_summary, abstract, review_type, raw_data, source_url
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (review_id) DO NOTHING
  `;
  const values = [
    data.reviewId || data.id || data.doi,
    data.doi,
    data.title,
    data.authors || (data.authorList && data.authorList.join(", ")),
    data.publicationDate || data.publishedDate,
    data.deviceKeywords || data.keywords,
    data.conclusionSummary || data.conclusion,
    data.abstract,
    data.reviewType || "Systematic Review",
    JSON.stringify(data),
    data.sourceUrl || data.url,
  ];
  try {
    await query(queryText, values);
  } catch (error) {
    console.error("Error inserting Cochrane review:", error.message);
  }
}

module.exports = {
  createTableCochraneReviews,
  insertCochraneReview,
};
