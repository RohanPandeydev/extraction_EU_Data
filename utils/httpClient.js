const { getAllHeaders } = require("../headers");
const { sleep } = require("./rateLimiter");

async function fetchJSON(url, options = {}) {
  const maxRetries = options.retries || 3;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, {
        method: options.method || "GET",
        headers: options.headers || {},
        redirect: "follow",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} for ${url}`);
      }
      return await response.json();
    } catch (error) {
      console.error(`Attempt ${attempt}/${maxRetries} failed for ${url}: ${error.message}`);
      if (attempt < maxRetries) {
        await sleep(2000, 3000);
      } else {
        throw error;
      }
    }
  }
}

async function fetchHTML(url, options = {}) {
  const maxRetries = options.retries || 3;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: options.headers || {
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
          "Accept": "text/html,application/xhtml+xml",
          "Accept-Language": "en-GB,en;q=0.9",
        },
        redirect: "follow",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} for ${url}`);
      }
      return await response.text();
    } catch (error) {
      console.error(`Attempt ${attempt}/${maxRetries} failed for ${url}: ${error.message}`);
      if (attempt < maxRetries) {
        await sleep(2000, 3000);
      } else {
        throw error;
      }
    }
  }
}

async function fetchWithEudamedHeaders(url) {
  const myHeaders = await getAllHeaders();
  return fetchJSON(url, { headers: myHeaders });
}

module.exports = { fetchJSON, fetchHTML, fetchWithEudamedHeaders };
