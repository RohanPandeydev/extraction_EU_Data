async function getAllHeaders() {
  const myHeaders = new Headers();
  myHeaders.append("Accept", "application/json");
  myHeaders.append("Accept-Language", "en-GB,en;q=0.6");
  myHeaders.append("Cache-Control", "No-Cache");
  myHeaders.append("Connection", "keep-alive");
  myHeaders.append("Content-Type", "application/json");
  myHeaders.append("Referer", "https://ec.europa.eu/tools/eudamed/");
  myHeaders.append("Sec-Fetch-Dest", "empty");
  myHeaders.append("Sec-Fetch-Mode", "cors");
  myHeaders.append("Sec-Fetch-Site", "same-origin");
  myHeaders.append("Sec-GPC", "1");
  myHeaders.append(
    "User-Agent",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
  );
  myHeaders.append("X-Requested-With", "XMLHttpRequest");
  myHeaders.append(
    "sec-ch-ua",
    '"Brave";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
  );
  myHeaders.append("sec-ch-ua-mobile", "?0");
  myHeaders.append("sec-ch-ua-platform", '"macOS"');
  myHeaders.append(
    "Cookie",
    "cck1=%7B%22cm%22%3Atrue%2C%22all1st%22%3Atrue%7D",
  );
  return myHeaders;
}
module.exports = {
  getAllHeaders,
};
