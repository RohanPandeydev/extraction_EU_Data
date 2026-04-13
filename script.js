const { getAllHeaders } = require("./headers");
const { sleep } = require("./utils/rateLimiter");
const { fetchJSON, fetchHTML } = require("./utils/httpClient");
const {
  setupDatabase, insertDeviceComplete, insertNotifiedBody,
  insertRefusedApplication, insertEMAMedicine, insertCochraneReview,
  insertSafetyNotice, getTableCount, closeConnection,
} = require("./data/snowflake");

// === LOGGING ===
function log(source, message) {
  const timestamp = new Date().toISOString().replace("T", " ").substring(0, 19);
  console.log(`[${timestamp}] [${source}] ${message}`);
}

// === EUDAMED API ===
async function fetchDevicesPage(page, pageSize) {
  const myHeaders = await getAllHeaders();
  const url = `https://ec.europa.eu/tools/eudamed/api/devices/udiDiData?page=${page}&pageSize=${pageSize}&size=${pageSize}&iso2Code=en&deviceStatusCode=refdata.device-model-status.on-the-market&languageIso2Code=en&includeHistoricalVersion=true`;
  const response = await fetch(url, { method: "GET", headers: myHeaders, redirect: "follow" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const data = await response.json();
  return { contents: data.content, totalPages: data.totalPages, totalElements: data.totalElements };
}

async function fetchDeviceDetail(uuid) {
  const myHeaders = await getAllHeaders();
  const url = `https://ec.europa.eu/tools/eudamed/api/devices/basicUdiData/udiDiData/${uuid}?languageIso2Code=en`;
  const response = await fetch(url, { method: "GET", headers: myHeaders, redirect: "follow" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return await response.json();
}

// === BUILD COMPLETE DEVICE JSON ===
function buildDeviceJSON(basic, detail) {
  return {
    identity: {
      uuid: basic.uuid, ulid: basic.ulid, basicUdi: basic.basicUdi, primaryDi: basic.primaryDi,
      reference: basic.reference, tradeName: basic.tradeName,
      deviceName: detail?.deviceName || basic.deviceName, deviceModel: detail?.deviceModel || basic.deviceModel,
      deviceModelApplicable: detail?.deviceModelApplicable, deviceCriterion: detail?.deviceCriterion || basic.deviceCriterion,
    },
    classification: {
      riskClass: basic.riskClass?.code?.split(".").pop() || null, riskClassCode: basic.riskClass?.code,
      legislation: detail?.legislation?.code?.split(".").pop() || basic.applicableLegislation?.code?.split(".").pop() || null,
      legislationCode: detail?.legislation?.code || basic.applicableLegislation?.code,
      legacyDirective: detail?.legislation?.legacyDirective || false,
      specialDeviceType: detail?.specialDeviceType?.code?.split(".").pop() || null,
      specialDeviceTypeApplicable: detail?.specialDeviceTypeApplicable || false,
      issuingAgency: basic.issuingAgency?.code?.split(".").pop() || null,
      containerPackageCount: basic.containerPackageCount,
    },
    characteristics: {
      active: detail?.active || false, implantable: detail?.implantable || false,
      reusable: detail?.reusable || false, sterile: detail?.sterile || basic.sterile || false,
      measuringFunction: detail?.measuringFunction || false, administeringMedicine: detail?.administeringMedicine || false,
      multiComponent: detail?.multiComponent || basic.multiComponent || false,
      humanTissues: detail?.humanTissues || false, animalTissues: detail?.animalTissues || false,
      humanProduct: detail?.humanProduct || false, medicinalProduct: detail?.medicinalProduct || false,
      device: detail?.device || false, kit: detail?.kit || false, reagent: detail?.reagent || false,
      instrument: detail?.instrument || false, companionDiagnostics: detail?.companionDiagnostics || false,
      selfTesting: detail?.selfTesting || false, nearPatientTesting: detail?.nearPatientTesting || false,
      professionalTesting: detail?.professionalTesting || false,
    },
    manufacturer: {
      name: detail?.manufacturer?.name || basic.manufacturerName, srn: detail?.manufacturer?.srn || basic.manufacturerSrn,
      status: detail?.manufacturer?.status?.code?.split(".").pop() || null,
      countryIso2Code: detail?.manufacturer?.countryIso2Code || null, countryName: detail?.manufacturer?.countryName || null,
      countryType: detail?.manufacturer?.countryType || null, address: detail?.manufacturer?.geographicalAddress || null,
      email: detail?.manufacturer?.electronicMail || null, phone: detail?.manufacturer?.telephone || null,
      uuid: detail?.manufacturer?.uuid || null,
    },
    authorisedRepresentative: detail?.authorisedRepresentative ? {
      name: detail.authorisedRepresentative.name || basic.authorisedRepresentativeName,
      srn: detail.authorisedRepresentative.srn || basic.authorisedRepresentativeSrn,
      address: detail.authorisedRepresentative.address || null, countryName: detail.authorisedRepresentative.countryName || null,
      email: detail.authorisedRepresentative.email || null, phone: detail.authorisedRepresentative.telephone || null,
      mandateStartDate: detail.authorisedRepresentative.startDate || null, mandateEndDate: detail.authorisedRepresentative.endDate || null,
    } : { name: basic.authorisedRepresentativeName || null, srn: basic.authorisedRepresentativeSrn || null },
    basicUdi: detail?.basicUdi ? {
      uuid: detail.basicUdi.uuid, code: detail.basicUdi.code,
      issuingAgency: detail.basicUdi.issuingAgency?.code?.split(".").pop() || null, type: detail.basicUdi.type,
    } : null,
    certificates: (detail?.deviceCertificateInfoList || []).map((cert) => ({
      uuid: cert.uuid, certificateNumber: cert.certificateNumber,
      certificateType: cert.certificateType?.code?.split(".").pop() || null,
      issueDate: cert.issueDate || null, expiryDate: cert.certificateExpiry || null,
      startingValidityDate: cert.startingValidityDate || null,
      status: cert.status?.code?.split(".").pop() || cert.versionState?.code?.split(".").pop() || null,
      notifiedBody: cert.notifiedBody ? { name: cert.notifiedBody.name, srn: cert.notifiedBody.srn, countryIso2Code: cert.notifiedBody.countryIso2Code } : null,
      revision: cert.certificateRevision || null,
    })),
    clinicalInvestigation: { applicable: detail?.clinicalInvestigationApplicable || false, links: detail?.clinicalInvestigationLinks || [] },
    medicalPurpose: detail?.medicalPurpose || null, nbDecision: detail?.nbDecision || null,
    sutures: detail?.sutures || null, microbialSubstances: detail?.microbialSubstances || null,
    typeExaminationApplicable: detail?.typeExaminationApplicable || null, linkedSscp: detail?.linkedSscp || null,
    legacyDeviceUdiDiApplicable: detail?.legacyDeviceUdiDiApplicable || null,
    status: {
      deviceStatus: basic.deviceStatusType?.code?.split(".").pop() || null,
      versionState: detail?.versionState?.code?.split(".").pop() || null,
      latestVersion: detail?.latestVersion ?? basic.latestVersion, versionNumber: detail?.versionNumber || basic.versionNumber,
      versionDate: detail?.versionDate || null, lastUpdateDate: detail?.lastUpdated || basic.lastUpdateDate,
      discardedDate: detail?.discardedDate || null, isNew: detail?.new || false,
    },
  };
}

// === BULK DATA FOR MATCHING ===
let bulkData = { ansm: [], cochrane: [], scheer: [], ema: [] };

function matchKeywords(text, keywords) {
  if (!text) return false;
  const lower = text.toLowerCase();
  return keywords.some((kw) => {
    if (!kw || kw.length < 4) return false;
    const kwLower = kw.toLowerCase();
    const generic = ["device", "medical", "system", "standard", "health", "product", "group", "test", "type", "model", "active", "service", "general", "international", "global", "europe"];
    if (generic.includes(kwLower)) return false;
    if (kwLower.length < 8) {
      const regex = new RegExp(`\\b${kwLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, "i");
      return regex.test(text);
    }
    return lower.includes(kwLower);
  });
}

function findAdverseEvents(deviceName, manufacturerName, tradeName) {
  const keywords = [deviceName, manufacturerName, tradeName].filter(Boolean);
  if (keywords.length === 0) return [];
  const results = [];
  for (const r of bulkData.ansm) {
    if (matchKeywords(r.deviceName, keywords)) results.push({ source: "ANSM (France)", title: r.deviceName, status: r.status, date: r.updateDate });
  }
  return results;
}

function findClinicalEvidence(deviceName, tradeName) {
  const keywords = [deviceName, tradeName].filter(Boolean);
  if (keywords.length === 0) return [];
  const results = [];
  for (const r of bulkData.cochrane) {
    if (matchKeywords(r.title, keywords)) results.push({ source: "Cochrane/PubMed", title: r.title, publicationDate: r.pubdate, url: `https://pubmed.ncbi.nlm.nih.gov/${r.uid}/` });
  }
  for (const r of bulkData.scheer) {
    if (matchKeywords(r.title, keywords)) results.push({ source: "SCHEER", title: r.title, topic: r.topic, url: r.url });
  }
  return results;
}

function findRelatedMedicines(deviceName, manufacturerName) {
  const keywords = [deviceName, manufacturerName].filter(Boolean);
  if (keywords.length === 0) return [];
  const results = [];
  for (const r of bulkData.ema) {
    if (matchKeywords(r.name_of_medicine, keywords) || matchKeywords(r.marketing_authorisation_developer_applicant_holder, keywords)) {
      results.push({ source: "EMA", medicineName: r.name_of_medicine, activeSubstance: r.active_substance, status: r.medicine_status, url: r.medicine_url });
    }
  }
  return results;
}

// === LIVE API PER DEVICE ===
async function fetchPubMedStudies(deviceName, tradeName) {
  const searchTerm = tradeName || deviceName;
  if (!searchTerm || searchTerm.length < 4) return [];
  const generic = ["device", "medical", "system", "standard", "health", "product", "test", "model", "instrument"];
  if (generic.includes(searchTerm.toLowerCase())) return [];
  try {
    const encoded = encodeURIComponent(searchTerm);
    const searchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term="${encoded}"+AND+(medical+device+OR+clinical+trial+OR+systematic+review+OR+safety+OR+adverse+event)&retmax=10&retmode=json`;
    const searchRes = await fetch(searchUrl);
    if (!searchRes.ok) return [];
    const searchData = await searchRes.json();
    const ids = searchData.esearchresult?.idlist || [];
    if (ids.length === 0) return [];
    await sleep(500, 300);
    const detailUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${ids.join(",")}&retmode=json`;
    const detailRes = await fetch(detailUrl);
    if (!detailRes.ok) return [];
    const detailData = await detailRes.json();
    const results = [];
    for (const id of ids) {
      const a = detailData.result?.[id];
      if (a) {
        const lower = (a.title || "").toLowerCase() + " " + (a.fulljournalname || "").toLowerCase();
        const type = lower.includes("adverse") || lower.includes("vigilance") || lower.includes("safety report") ? "Adverse Event / Safety Report"
          : lower.includes("systematic review") || lower.includes("meta-analysis") || lower.includes("cochrane") ? "Systematic Review"
          : lower.includes("clinical trial") || lower.includes("randomized") ? "Clinical Trial" : "Clinical Evidence";
        results.push({ source: "PubMed", type, title: a.title, authors: a.authors?.map((x) => x.name).join(", "), journal: a.fulljournalname, publicationDate: a.pubdate, doi: a.elocationid, url: `https://pubmed.ncbi.nlm.nih.gov/${a.uid}/` });
      }
    }
    return results;
  } catch { return []; }
}

async function fetchPubMedAdverseEvents(deviceName, tradeName) {
  const searchTerm = tradeName || deviceName;
  if (!searchTerm || searchTerm.length < 4) return [];
  const generic = ["device", "medical", "system", "standard", "health", "product", "test", "model", "instrument"];
  if (generic.includes(searchTerm.toLowerCase())) return [];
  try {
    const encoded = encodeURIComponent(searchTerm);
    const searchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term="${encoded}"+AND+(adverse+event+OR+complication+OR+recall+OR+vigilance+OR+post-market+surveillance)&retmax=5&retmode=json`;
    const searchRes = await fetch(searchUrl);
    if (!searchRes.ok) return [];
    const searchData = await searchRes.json();
    const ids = searchData.esearchresult?.idlist || [];
    if (ids.length === 0) return [];
    await sleep(400, 200);
    const detailUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${ids.join(",")}&retmode=json`;
    const detailRes = await fetch(detailUrl);
    if (!detailRes.ok) return [];
    const detailData = await detailRes.json();
    const results = [];
    for (const id of ids) {
      const a = detailData.result?.[id];
      if (a) results.push({ source: "PubMed (Adverse Events)", title: a.title, authors: a.authors?.map((x) => x.name).join(", "), journal: a.fulljournalname, publicationDate: a.pubdate, doi: a.elocationid, url: `https://pubmed.ncbi.nlm.nih.gov/${a.uid}/` });
    }
    return results;
  } catch { return []; }
}

async function fetchEudamedCertsByManufacturer(manufacturerSrn) {
  if (!manufacturerSrn) return [];
  try {
    const myHeaders = await getAllHeaders();
    const url = `https://ec.europa.eu/tools/eudamed/api/certificates/search/?page=0&pageSize=10&actorSrn=${encodeURIComponent(manufacturerSrn)}&languageIso2Code=en`;
    const response = await fetch(url, { method: "GET", headers: myHeaders, redirect: "follow" });
    if (!response.ok) return [];
    const data = await response.json();
    return (data.content || []).map((cert) => ({ source: "EUDAMED Certificates", certificateNumber: cert.certificateNumber, certificateType: cert.certificateType?.code?.split(".").pop() || null, issueDate: cert.issueDate, expiryDate: cert.expiryDate, status: cert.certificateStatus?.code?.split(".").pop() || null, notifiedBodySrn: cert.notifiedBodySrn, revision: cert.revisionNumber }));
  } catch { return []; }
}

// === PROCESS SINGLE DEVICE -> SNOWFLAKE ===
async function processOneDevice(basicDevice, deviceIndex, totalInPage) {
  const uuid = basicDevice.uuid;
  const name = basicDevice.tradeName || basicDevice.deviceName || basicDevice.reference || "Unknown";

  let detail = null;
  let certCount = 0;
  try {
    detail = await fetchDeviceDetail(uuid);
    certCount = (detail.deviceCertificateInfoList || []).length;
  } catch (error) {
    log("DEVICE", `  [${deviceIndex}/${totalInPage}] "${name}" | detail: FAILED (${error.message})`);
  }

  const deviceJSON = buildDeviceJSON(basicDevice, detail);
  const deviceName = detail?.deviceName || basicDevice.deviceName;
  const tradeName = basicDevice.tradeName;
  const manufacturerName = detail?.manufacturer?.name || basicDevice.manufacturerName;

  // Bulk matching
  deviceJSON.adverseEvents = findAdverseEvents(deviceName, manufacturerName, tradeName);
  deviceJSON.clinicalEvidence = findClinicalEvidence(deviceName, tradeName);
  deviceJSON.relatedMedicines = findRelatedMedicines(deviceName, manufacturerName);

  // Live API calls
  try {
    const manufacturerSrn = detail?.manufacturer?.srn || basicDevice.manufacturerSrn;
    const [pubmedStudies, pubmedAdverse, eudamedCerts] = await Promise.all([
      fetchPubMedStudies(deviceName, tradeName),
      fetchPubMedAdverseEvents(deviceName, tradeName),
      fetchEudamedCertsByManufacturer(manufacturerSrn),
    ]);
    for (const study of pubmedStudies) {
      if (study.type === "Adverse Event / Safety Report") deviceJSON.adverseEvents.push(study);
      else deviceJSON.clinicalEvidence.push(study);
    }
    deviceJSON.adverseEvents.push(...pubmedAdverse);
    if (eudamedCerts.length > 0 && deviceJSON.certificates.length === 0) deviceJSON.manufacturerCertificates = eudamedCerts;
  } catch (error) {
    log("DEVICE", `  [${deviceIndex}/${totalInPage}] "${name}" | live API error: ${error.message}`);
  }

  // Push to Snowflake
  await insertDeviceComplete(deviceJSON);

  const aeCount = deviceJSON.adverseEvents.length;
  const ceCount = deviceJSON.clinicalEvidence.length;
  log("DEVICE", `  [${deviceIndex}/${totalInPage}] "${name}" | detail: ${detail ? "OK" : "SKIP"} | certs: ${certCount} | adverse: ${aeCount} | evidence: ${ceCount} | -> Snowflake`);
}

// === BULK SCRAPERS -> SNOWFLAKE ===
async function runBulkScrapers() {
  const results = [];

  // 1. Notified Bodies
  log("BULK", ">>> EUDAMED Notified Bodies");
  try {
    const nbData = await fetchJSON("https://ec.europa.eu/tools/eudamed/api/ses/notifiedBodies", { headers: await getAllHeaders() });
    for (const nb of (Array.isArray(nbData) ? nbData : [])) await insertNotifiedBody(nb);
    log("BULK", `<<< Notified Bodies: ${Array.isArray(nbData) ? nbData.length : 0} -> Snowflake`);
    results.push({ name: "Notified Bodies", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< Notified Bodies FAILED: ${e.message}`); results.push({ name: "Notified Bodies", status: "FAILED", error: e.message }); }

  // 2. Refused Applications
  log("BULK", ">>> EUDAMED Refused Applications");
  try {
    const allApps = [];
    let page = 0, hasMore = true;
    while (hasMore) {
      const data = await fetchJSON(`https://ec.europa.eu/tools/eudamed/api/applications/search/?page=${page}&pageSize=100&languageIso2Code=en`, { headers: await getAllHeaders() });
      const items = data.content || [];
      allApps.push(...items);
      hasMore = items.length > 0 && page < (data.totalPages || 0) - 1;
      page++;
      await sleep(2000, 1000);
    }
    for (const app of allApps) await insertRefusedApplication(app);
    log("BULK", `<<< Refused Applications: ${allApps.length} -> Snowflake`);
    results.push({ name: "Refused Applications", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< Refused Applications FAILED: ${e.message}`); results.push({ name: "Refused Applications", status: "FAILED", error: e.message }); }

  // 3. EMA Medicines
  log("BULK", ">>> EMA Medicines (JSON API)");
  try {
    const emaData = await fetchJSON("https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json", { headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" } });
    const records = emaData.data || emaData;
    bulkData.ema = records; // keep in memory for matching
    log("BULK", `  EMA downloaded: ${records.length} records, inserting to Snowflake...`);
    let count = 0;
    for (const med of records) {
      if (med.category === "Human") {
        await insertEMAMedicine(med);
        count++;
        if (count % 200 === 0) log("BULK", `  EMA progress: ${count} inserted...`);
      }
    }
    log("BULK", `<<< EMA Medicines: ${count} -> Snowflake`);
    results.push({ name: "EMA Medicines", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< EMA Medicines FAILED: ${e.message}`); results.push({ name: "EMA Medicines", status: "FAILED", error: e.message }); }

  // 4. Cochrane/PubMed
  log("BULK", ">>> Cochrane/PubMed Reviews");
  try {
    const allReviews = [];
    const terms = ['"Cochrane Database Syst Rev"[Journal] AND "medical device"','"Cochrane Database Syst Rev"[Journal] AND "implant"','"Cochrane Database Syst Rev"[Journal] AND "prosthesis"','"Cochrane Database Syst Rev"[Journal] AND "stent"','"Cochrane Database Syst Rev"[Journal] AND "pacemaker"','"Cochrane Database Syst Rev"[Journal] AND "surgical mesh"','"Cochrane Database Syst Rev"[Journal] AND "hip replacement"','"Cochrane Database Syst Rev"[Journal] AND "knee replacement"','"Cochrane Database Syst Rev"[Journal] AND "defibrillator"','"Cochrane Database Syst Rev"[Journal] AND "ventilator"','"Cochrane Database Syst Rev"[Journal] AND "heart valve"','"Cochrane Database Syst Rev"[Journal] AND "dental implant"','"Cochrane Database Syst Rev"[Journal] AND "dialysis"'];
    for (const term of terms) {
      const shortTerm = term.split("AND")[1]?.trim().replace(/"/g, "") || term;
      const searchData = await fetchJSON(`https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(term)}&retmax=100&retmode=json`);
      const ids = searchData.esearchresult?.idlist || [];
      if (ids.length > 0) {
        await sleep(500, 500);
        const detailData = await fetchJSON(`https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${ids.join(",")}&retmode=json`);
        for (const id of ids) {
          if (detailData.result?.[id]) {
            const review = { ...detailData.result[id], searchTerm: shortTerm };
            allReviews.push(review);
            await insertCochraneReview(review);
          }
        }
        log("BULK", `  Cochrane "${shortTerm}": ${ids.length} found`);
      }
      await sleep(1000, 500);
    }
    bulkData.cochrane = allReviews;
    log("BULK", `<<< Cochrane/PubMed: ${allReviews.length} -> Snowflake`);
    results.push({ name: "Cochrane/PubMed", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< Cochrane/PubMed FAILED: ${e.message}`); results.push({ name: "Cochrane/PubMed", status: "FAILED", error: e.message }); }

  // 5. ANSM
  log("BULK", ">>> ANSM Safety (France)");
  try {
    const cheerio = require("cheerio");
    const html = await fetchHTML("https://ansm.sante.fr/disponibilites-des-produits-de-sante/dispositifs-medicaux", { headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "fr-FR" } });
    const $ = cheerio.load(html);
    const records = [];
    $("table tbody tr").each((_, el) => {
      const cells = $(el).find("td");
      if (cells.length >= 4) {
        const record = { status: $(cells[0]).text().trim(), updateDate: $(cells[1]).text().trim(), deviceType: $(cells[2]).text().trim(), deviceName: $(cells[3]).text().trim(), returnDate: cells.length > 4 ? $(cells[4]).text().trim() : null };
        records.push(record);
        insertSafetyNotice("ANSM", record);
      }
    });
    bulkData.ansm = records;
    log("BULK", `<<< ANSM: ${records.length} -> Snowflake`);
    results.push({ name: "ANSM", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< ANSM FAILED: ${e.message}`); results.push({ name: "ANSM", status: "FAILED", error: e.message }); }

  // 6. SCHEER
  log("BULK", ">>> SCHEER Opinions");
  try {
    const cheerio = require("cheerio");
    const html = await fetchHTML("https://health.ec.europa.eu/scientific-committees/scientific-committee-health-environmental-and-emerging-risks-scheer/scheer-opinions_en", { headers: { "User-Agent": "Mozilla/5.0" } });
    const $ = cheerio.load(html);
    const records = [];
    let topic = "General";
    $("h2, a").each((_, el) => {
      const tag = $(el).prop("tagName").toLowerCase();
      if (tag === "h2") { topic = $(el).text().trim(); return; }
      const title = $(el).text().trim(), link = $(el).attr("href");
      if (!title || title.length < 10 || !link) return;
      const lower = (title + " " + topic).toLowerCase();
      if (["device","implant","prosth","biocompat","nanomaterial","silicone","metal","surgical","medical","phthalate","brain stimulat","tissue"].some(k => lower.includes(k))) {
        const record = { title, topic, url: link.startsWith("http") ? link : `https://health.ec.europa.eu${link}` };
        records.push(record);
        insertSafetyNotice("SCHEER", { ...record, deviceName: title });
      }
    });
    bulkData.scheer = records;
    log("BULK", `<<< SCHEER: ${records.length} -> Snowflake`);
    results.push({ name: "SCHEER", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< SCHEER FAILED: ${e.message}`); results.push({ name: "SCHEER", status: "FAILED", error: e.message }); }

  return results;
}

// === MAIN ===
async function main() {
  console.log("");
  console.log("========================================================");
  console.log("       MEDICAL DEVICE DATA EXTRACTION SUITE");
  console.log("         (Direct to Snowflake)");
  console.log("========================================================");
  log("MAIN", `Snowflake: ${process.env.SNOWFLAKE_ACCOUNT}`);
  log("MAIN", `Started at: ${new Date().toISOString()}`);
  console.log("========================================================\n");

  await setupDatabase();
  console.log("");

  // PHASE 1: Per-device -> Snowflake (FIRST — main priority)
  log("MAIN", "====== PHASE 1: Per-Device Extraction -> Snowflake ======");
  log("MAIN", "Each device: basic + detail + certs + PubMed adverse/clinical -> Snowflake");
  console.log("");

  const { totalPages, totalElements } = await fetchDevicesPage(0, 5);
  log("EUDAMED", `Total devices: ${totalElements.toLocaleString()} | Pages: ${totalPages.toLocaleString()} (300/page)`);
  console.log("");

  let globalDeviceCount = 0;

  for (let page = 0; page < totalPages; page++) {
    log("EUDAMED", `--- Page ${page + 1}/${totalPages} ---`);
    let contents;
    try {
      const result = await fetchDevicesPage(page, 300);
      contents = result.contents;
    } catch (error) {
      log("EUDAMED", `Page ${page + 1} fetch FAILED: ${error.message}, skipping...`);
      await sleep(5000, 5000);
      continue;
    }

    for (let j = 0; j < contents.length; j++) {
      await processOneDevice(contents[j], j + 1, contents.length);
      await sleep(1000, 1000);
    }

    globalDeviceCount += contents.length;
    const dbCount = await getTableCount("DEVICES");
    log("EUDAMED", `Page ${page + 1} complete | Snowflake DEVICES: ${dbCount} rows`);
    console.log("");
    await sleep(3000, 3000);
  }

  // PHASE 2: Bulk scrapers -> Snowflake (AFTER devices)
  console.log("");
  log("MAIN", "====== PHASE 2: Bulk Data Sources -> Snowflake ======");
  console.log("");
  const bulkResults = await runBulkScrapers();

  // Final stats
  console.log("");
  log("MAIN", "====== Final Snowflake Stats ======");
  const tables = ["DEVICES", "MANUFACTURERS", "AUTHORISED_REPRESENTATIVES", "DEVICE_CERTIFICATES", "DEVICE_ADVERSE_EVENTS", "DEVICE_CLINICAL_EVIDENCE", "DEVICE_RELATED_MEDICINES", "NOTIFIED_BODIES", "REFUSED_APPLICATIONS", "EMA_MEDICINES", "COCHRANE_REVIEWS", "SAFETY_NOTICES"];
  for (const t of tables) { const c = await getTableCount(t); log("MAIN", `  ${t}: ${c} rows`); }

  console.log("");
  console.log("========================================================");
  console.log("                  EXTRACTION SUMMARY");
  console.log("========================================================");
  console.log(`  PHASE 1 - Devices: ${globalDeviceCount.toLocaleString()} processed`);
  console.log("  PHASE 2 - Bulk Sources:");
  for (const r of bulkResults) { console.log(`    ${r.status === "SUCCESS" ? "[OK]  " : "[FAIL]"} ${r.name} ${r.error ? "- " + r.error : ""}`); }
  console.log("========================================================");
  log("MAIN", `Finished at: ${new Date().toISOString()}`);
  console.log("");

  closeConnection();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  closeConnection();
  process.exit(1);
});
