const cheerio = require("cheerio");
const { fetchHTML } = require("./httpClient");

async function loadPage(url, headers) {
  const html = await fetchHTML(url, { headers });
  return cheerio.load(html);
}

module.exports = { loadPage, cheerio };
