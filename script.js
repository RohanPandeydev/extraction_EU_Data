const { getAllHeaders } = require("./headers");
const { sleep } = require("./utils/rateLimiter");
const { fetchJSON, fetchHTML } = require("./utils/httpClient");
const {
  setupDatabase, insertDeviceComplete, insertNotifiedBody,
  insertRefusedApplication, insertSafetyNotice,
  insertClinicalTrial, insertEuropePmc,
  getTableCount, closeConnection,
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

// === BULK DATA FOR MATCHING (in-memory only for reference sources) ===
let bulkData = {
  ansm: [], cochrane: [], scheer: [], ema: [],
  bfarm: [], aemps: [], igj: [], iss: [],
  openfdaRecalls: [], openfdaMaude: [],
  clinicalTrials: [], europePmc: [],
};

const GENERIC_TERMS = ["device", "medical", "system", "standard", "health", "product", "group", "test", "type", "model", "active", "service", "general", "international", "global", "europe"];

function matchKeyword(text, kw) {
  if (!text || !kw || kw.length < 4) return false;
  const kwLower = kw.toLowerCase();
  if (GENERIC_TERMS.includes(kwLower)) return false;
  if (kwLower.length < 8) {
    const regex = new RegExp(`\\b${kwLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, "i");
    return regex.test(text);
  }
  return text.toLowerCase().includes(kwLower);
}

function matchKeywords(text, keywords) {
  return keywords.some((kw) => matchKeyword(text, kw));
}

// Returns { score: 0-1, matchType: string, matchedKeyword: string }
// score semantics:
//   1.0 = device name + manufacturer both matched (strongest signal)
//   0.9 = device/trade name exact whole-word match
//   0.7 = device/trade name substring match (long keywords)
//   0.5 = manufacturer-only match (weak — company makes many devices)
//   0.0 = no match
function scoreMatch(text, { deviceName, tradeName, manufacturerName }) {
  if (!text) return { score: 0, matchType: "none", matchedKeyword: null };

  const checkKeyword = (kw) => {
    if (!kw || kw.length < 4) return { hit: false, exact: false };
    const kwLower = kw.toLowerCase();
    if (GENERIC_TERMS.includes(kwLower)) return { hit: false, exact: false };
    const regex = new RegExp(`\\b${kwLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, "i");
    if (regex.test(text)) return { hit: true, exact: true };
    if (kwLower.length >= 8 && text.toLowerCase().includes(kwLower)) return { hit: true, exact: false };
    return { hit: false, exact: false };
  };

  const dHit = checkKeyword(deviceName);
  const tHit = checkKeyword(tradeName);
  const mHit = checkKeyword(manufacturerName);

  const deviceHit = dHit.hit ? dHit : tHit;
  const deviceKw = dHit.hit ? deviceName : (tHit.hit ? tradeName : null);

  if (deviceHit.hit && mHit.hit) {
    return { score: 1.0, matchType: "device+manufacturer", matchedKeyword: `${deviceKw} + ${manufacturerName}` };
  }
  if (deviceHit.hit) {
    return { score: deviceHit.exact ? 0.9 : 0.7, matchType: dHit.hit ? "deviceName" : "tradeName", matchedKeyword: deviceKw };
  }
  if (mHit.hit) {
    return { score: 0.5, matchType: "manufacturerName", matchedKeyword: manufacturerName };
  }
  return { score: 0, matchType: "none", matchedKeyword: null };
}

function findAdverseEvents(deviceName, manufacturerName, tradeName) {
  if (!deviceName && !manufacturerName && !tradeName) return [];
  const ctx = { deviceName, tradeName, manufacturerName };
  const results = [];

  for (const r of bulkData.ansm) {
    const m = scoreMatch(r.deviceName, ctx);
    if (m.score > 0) results.push({
      source: "ANSM (France)", title: r.deviceName, status: r.status,
      date: r.updateDate, type: r.deviceType || "Safety Notice",
      url: "https://ansm.sante.fr/disponibilites-des-produits-de-sante/dispositifs-medicaux",
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.bfarm) {
    const m = scoreMatch(`${r.deviceName || r.title || ""} ${r.manufacturerName || ""}`, ctx);
    if (m.score > 0) results.push({
      source: "BfArM (Germany)", title: r.title, status: r.recallType,
      date: r.recallDate, type: "Field Safety Corrective Action", url: r.sourceUrl,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.aemps) {
    const m = scoreMatch(r.deviceName || r.title, ctx);
    if (m.score > 0) results.push({
      source: "AEMPS (Spain)", title: r.title, status: r.recallType,
      date: r.recallDate, type: "Safety Alert", url: r.sourceUrl,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.igj) {
    const m = scoreMatch(`${r.deviceName || r.title || ""} ${r.manufacturerName || ""}`, ctx);
    if (m.score > 0) results.push({
      source: "IGJ (Netherlands)", title: r.title, status: r.recallType,
      date: r.recallDate, type: "Field Safety Notice", url: r.sourceUrl,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.iss) {
    const m = scoreMatch(r.deviceName || r.title, ctx);
    if (m.score > 0) results.push({
      source: "ISS (Italy)", title: r.title, status: r.recallType || "Safety Info",
      date: r.recallDate, type: "Safety Information", url: r.sourceUrl,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.openfdaRecalls) {
    const m = scoreMatch(`${r.productDescription || ""} ${r.brandName || ""} ${r.recallingFirm || ""}`, ctx);
    if (m.score > 0) results.push({
      source: "openFDA Recalls (US)", title: r.productDescription || r.brandName,
      status: r.recallStatus, date: r.datePosted || r.dateInitiated,
      type: r.classification || "Recall",
      url: r.cfresId ? `https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfres/res.cfm?id=${r.cfresId}` : null,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.openfdaMaude) {
    const device = (r.device || [])[0] || {};
    const searchText = `${device.generic_name || ""} ${device.brand_name || ""} ${device.manufacturer_d_name || ""}`;
    const m = scoreMatch(searchText, ctx);
    if (m.score > 0) results.push({
      source: "openFDA MAUDE (US)", title: device.generic_name || device.brand_name,
      status: r.event_type, date: r.date_received,
      type: r.event_type || "Adverse Event",
      url: r.mdr_report_key ? `https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/detail.cfm?mdrfoi__id=${r.mdr_report_key}` : null,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  return results;
}

function findClinicalEvidence(deviceName, tradeName, manufacturerName) {
  if (!deviceName && !tradeName) return [];
  const ctx = { deviceName, tradeName, manufacturerName };
  const results = [];
  for (const r of bulkData.cochrane) {
    const m = scoreMatch(r.title, ctx);
    if (m.score > 0) results.push({
      source: "Cochrane/PubMed", type: "Systematic Review", title: r.title,
      authors: r.authors?.map((a) => a.name).join(", ") || null,
      journal: r.fulljournalname || null,
      publicationDate: r.pubdate || r.sortpubdate || null,
      doi: r.elocationid || null,
      url: r.uid ? `https://pubmed.ncbi.nlm.nih.gov/${r.uid}/` : null,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.scheer) {
    const m = scoreMatch(r.title, ctx);
    if (m.score > 0) results.push({
      source: "SCHEER", type: "Scientific Opinion", title: r.title, url: r.url,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.clinicalTrials) {
    const p = r.protocolSection || {};
    const nctId = p.identificationModule?.nctId;
    const title = p.identificationModule?.briefTitle || "";
    const interventions = (p.armsInterventionsModule?.interventions || []).map(i => i.name).join(" ");
    const sponsor = p.sponsorCollaboratorsModule?.leadSponsor?.name || "";
    const m = scoreMatch(`${title} ${interventions} ${sponsor}`, ctx);
    if (m.score > 0) results.push({
      source: "ClinicalTrials.gov", type: p.designModule?.studyType || "Clinical Trial",
      title, authors: sponsor, journal: null,
      publicationDate: p.statusModule?.startDateStruct?.date || null,
      doi: null, url: nctId ? `https://clinicaltrials.gov/study/${nctId}` : null,
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
  }
  for (const r of bulkData.europePmc) {
    const m = scoreMatch(`${r.title || ""} ${r.abstractText || ""}`, ctx);
    if (m.score > 0) results.push({
      source: "Europe PMC", type: "Research Article",
      title: r.title, authors: r.authorString, journal: r.journalTitle,
      publicationDate: r.firstPublicationDate, doi: r.doi,
      url: r.pmid ? `https://pubmed.ncbi.nlm.nih.gov/${r.pmid}/` : (r.pmcid ? `https://europepmc.org/article/PMC/${r.pmcid}` : null),
      matchConfidence: m.score, matchType: m.matchType, matchedKeyword: m.matchedKeyword,
    });
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
        // PubMed searched with quoted exact term → high confidence since title/journal contains it
        const hitExact = matchKeyword(a.title, searchTerm) || matchKeyword(a.fulljournalname, searchTerm);
        results.push({
          source: "PubMed", type, title: a.title,
          authors: a.authors?.map((x) => x.name).join(", "),
          journal: a.fulljournalname, publicationDate: a.pubdate,
          doi: a.elocationid, url: `https://pubmed.ncbi.nlm.nih.gov/${a.uid}/`,
          matchConfidence: hitExact ? 0.85 : 0.6,
          matchType: hitExact ? "pubmed-exact" : "pubmed-related",
          matchedKeyword: searchTerm,
        });
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
      if (a) {
        const hitExact = matchKeyword(a.title, searchTerm) || matchKeyword(a.fulljournalname, searchTerm);
        results.push({
          source: "PubMed (Adverse Events)", title: a.title,
          authors: a.authors?.map((x) => x.name).join(", "),
          journal: a.fulljournalname, publicationDate: a.pubdate,
          doi: a.elocationid, url: `https://pubmed.ncbi.nlm.nih.gov/${a.uid}/`,
          matchConfidence: hitExact ? 0.85 : 0.6,
          matchType: hitExact ? "pubmed-exact" : "pubmed-related",
          matchedKeyword: searchTerm,
        });
      }
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
  deviceJSON.clinicalEvidence = findClinicalEvidence(deviceName, tradeName, manufacturerName);
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

  // 3. EMA Medicines (in-memory only — reference for drug-device combo matching)
  log("BULK", ">>> EMA Medicines (JSON API, in-memory only)");
  try {
    const emaData = await fetchJSON("https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json", { headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" } });
    const records = emaData.data || emaData;
    bulkData.ema = records;
    log("BULK", `<<< EMA Medicines: ${records.length} kept in memory`);
    results.push({ name: "EMA Medicines", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< EMA Medicines FAILED: ${e.message}`); results.push({ name: "EMA Medicines", status: "FAILED", error: e.message }); }

  // 4. Cochrane/PubMed Reviews (in-memory only — data flows to DEVICE_CLINICAL_EVIDENCE via matching)
  log("BULK", ">>> Cochrane/PubMed Reviews (in-memory only)");
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
            allReviews.push({ ...detailData.result[id], searchTerm: shortTerm });
          }
        }
        log("BULK", `  Cochrane "${shortTerm}": ${ids.length} found`);
      }
      await sleep(1000, 500);
    }
    bulkData.cochrane = allReviews;
    log("BULK", `<<< Cochrane/PubMed: ${allReviews.length} kept in memory`);
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
      }
    });
    for (const r of records) await insertSafetyNotice("ANSM", r);
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
      }
    });
    for (const r of records) await insertSafetyNotice("SCHEER", { ...r, deviceName: r.title });
    bulkData.scheer = records;
    log("BULK", `<<< SCHEER: ${records.length} -> Snowflake`);
    results.push({ name: "SCHEER", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< SCHEER FAILED: ${e.message}`); results.push({ name: "SCHEER", status: "FAILED", error: e.message }); }

  // 7. BfArM (Germany)
  log("BULK", ">>> BfArM Field Safety (Germany)");
  try {
    const cheerio = require("cheerio");
    const records = [];
    for (let page = 0; page < 5; page++) {
      const url = `https://www.bfarm.de/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html?nn=708434&cl2Categories_Format=kundeninfo&resultsPerPage=100&gtp=${page * 100}_%2Fhttps%3A%2F%2Fwww.bfarm.de%2FSiteGlobals%2FForms%2FSuche%2FEN%2FExpertensuche_Formular.html`;
      const html = await fetchHTML(url, { headers: { "User-Agent": "Mozilla/5.0" } });
      const $ = cheerio.load(html);
      let pageCount = 0;
      $(".search-result, .result-list li, .c-search-result, article").each((_, el) => {
        const titleEl = $(el).find("h3 a, h2 a, .headline a, a.title").first();
        const title = titleEl.text().trim();
        const link = titleEl.attr("href");
        const date = $(el).find(".date, time, .c-search-result__date, .meta").first().text().trim();
        if (title && title.length > 5) {
          const byMatch = title.match(/\bby\s+(.+?)$/i);
          const record = { title, deviceName: title, manufacturerName: byMatch ? byMatch[1].trim() : null, recallDate: date, recallType: "Field Safety Corrective Action", sourceUrl: link ? (link.startsWith("http") ? link : `https://www.bfarm.de${link}`) : url };
          records.push(record);
          pageCount++;
        }
      });
      if (pageCount === 0) break;
      await sleep(1000, 500);
    }
    for (const r of records) await insertSafetyNotice("BFARM", { ...r, updateDate: r.recallDate, url: r.sourceUrl });
    bulkData.bfarm = records;
    log("BULK", `<<< BfArM: ${records.length} -> Snowflake`);
    results.push({ name: "BfArM", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< BfArM FAILED: ${e.message}`); results.push({ name: "BfArM", status: "FAILED", error: e.message }); }

  // 8. AEMPS (Spain)
  log("BULK", ">>> AEMPS Safety Alerts (Spain)");
  try {
    const cheerio = require("cheerio");
    const records = [];
    for (let page = 1; page <= 5; page++) {
      try {
        const html = await fetchHTML(`https://www.aemps.gob.es/productossanitarios/page/${page}/`, { headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "es-ES" } });
        const $ = cheerio.load(html);
        let pageCount = 0;
        $("article, .post, .entry, .hentry, .type-post").each((_, el) => {
          const titleEl = $(el).find("h2 a, h3 a, .entry-title a").first();
          const title = titleEl.text().trim();
          const link = titleEl.attr("href");
          const date = $(el).find("time, .entry-date, .published, .date").first().attr("datetime") || $(el).find("time, .entry-date").first().text().trim();
          if (title && title.length > 5) {
            const record = { title, deviceName: title, recallDate: date, recallType: "Safety Alert", sourceUrl: link };
            records.push(record);
            pageCount++;
          }
        });
        if (pageCount === 0) break;
      } catch (err) { if (err.message.includes("404")) break; throw err; }
      await sleep(1000, 500);
    }
    for (const r of records) await insertSafetyNotice("AEMPS", { ...r, updateDate: r.recallDate, url: r.sourceUrl });
    bulkData.aemps = records;
    log("BULK", `<<< AEMPS: ${records.length} -> Snowflake`);
    results.push({ name: "AEMPS", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< AEMPS FAILED: ${e.message}`); results.push({ name: "AEMPS", status: "FAILED", error: e.message }); }

  // 9. IGJ (Netherlands)
  log("BULK", ">>> IGJ Field Safety Notices (Netherlands)");
  try {
    const cheerio = require("cheerio");
    const records = [];
    for (let page = 0; page < 5; page++) {
      const url = `https://www.igj.nl/documenten?filters%5B0%5D%5Bfield%5D=information_type&filters%5B0%5D%5Bvalues%5D%5B0%5D=Waarschuwing&filters%5B0%5D%5Btype%5D=all&page=${page}`;
      const html = await fetchHTML(url, { headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "nl-NL" } });
      const $ = cheerio.load(html);
      let pageCount = 0;
      $("li.result, .search-result, .document-item, article, .overzicht-item").each((_, el) => {
        const titleEl = $(el).find("h3 a, h2 a, .title a, a").first();
        const rawTitle = titleEl.text().trim();
        const link = titleEl.attr("href");
        const date = $(el).find(".date, time, .meta-date, .document-date").first().text().trim();
        if (rawTitle && rawTitle.length > 5) {
          const parts = rawTitle.split(",").map(s => s.trim());
          const manufacturer = parts.length >= 3 ? parts[0] : null;
          const productName = parts.length >= 3 ? parts.slice(2).join(", ") : rawTitle;
          const record = { title: rawTitle, deviceName: productName, manufacturerName: manufacturer, recallDate: date, recallType: "Field Safety Notice", sourceUrl: link ? (link.startsWith("http") ? link : `https://www.igj.nl${link}`) : url };
          records.push(record);
          pageCount++;
        }
      });
      if (pageCount === 0) break;
      await sleep(1000, 500);
    }
    for (const r of records) await insertSafetyNotice("IGJ", { ...r, updateDate: r.recallDate, url: r.sourceUrl });
    bulkData.igj = records;
    log("BULK", `<<< IGJ: ${records.length} -> Snowflake`);
    results.push({ name: "IGJ", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< IGJ FAILED: ${e.message}`); results.push({ name: "IGJ", status: "FAILED", error: e.message }); }

  // 10. ISS (Italy)
  log("BULK", ">>> ISS Safety Info (Italy)");
  try {
    const cheerio = require("cheerio");
    const records = [];
    for (let page = 0; page < 3; page++) {
      const html = await fetchHTML(`https://www.iss.it/dispositivi-medici?page=${page}`, { headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "it-IT" } });
      const $ = cheerio.load(html);
      let pageCount = 0;
      $(".views-row, article, .node, .list-item, li, .content-item").each((_, el) => {
        const titleEl = $(el).find("h2 a, h3 a, .title a, a").first();
        const title = titleEl.text().trim();
        const link = titleEl.attr("href");
        const date = $(el).find(".date, time, .field-date").first().text().trim();
        if (title && title.length > 5) {
          const record = { title, deviceName: title, recallDate: date, recallType: "Safety Information", sourceUrl: link ? (link.startsWith("http") ? link : `https://www.iss.it${link}`) : null };
          records.push(record);
          pageCount++;
        }
      });
      if (pageCount === 0) break;
      await sleep(1000, 500);
    }
    for (const r of records) await insertSafetyNotice("ISS", { ...r, updateDate: r.recallDate, url: r.sourceUrl });
    bulkData.iss = records;
    log("BULK", `<<< ISS: ${records.length} -> Snowflake`);
    results.push({ name: "ISS", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< ISS FAILED: ${e.message}`); results.push({ name: "ISS", status: "FAILED", error: e.message }); }

  // 11. openFDA Device Recalls (US — matches EU manufacturers with US market presence)
  log("BULK", ">>> openFDA Device Recalls (US)");
  try {
    const records = [];
    const maxPages = process.env.OPENFDA_MAX_PAGES ? parseInt(process.env.OPENFDA_MAX_PAGES) : 10;
    for (let page = 0; page < maxPages; page++) {
      const data = await fetchJSON(`https://api.fda.gov/device/recall.json?limit=100&skip=${page * 100}&sort=event_date_posted:desc`, { headers: { "User-Agent": "eudamed-extraction/1.0" } });
      if (!data.results || data.results.length === 0) break;
      for (const recall of data.results) {
        const products = recall.products || [];
        const record = {
          source: "openFDA", cfresId: recall.cfres_id, productDescription: products[0]?.product_description || null,
          brandName: products[0]?.brand_name || null, recallingFirm: recall.recalling_firm || null,
          dateInitiated: recall.event_date_initiated || null, datePosted: recall.event_date_posted || null,
          recallStatus: recall.recall_status || null, classification: recall.classification || null,
          reasonForRecall: recall.reason_for_recall || null, country: recall.country || null,
        };
        records.push(record);
      }
      await sleep(300, 500);
    }
    for (const r of records) {
      await insertSafetyNotice("OPENFDA", {
        title: r.productDescription || r.brandName, deviceName: r.productDescription || r.brandName,
        deviceType: r.classification, status: r.recallStatus, updateDate: r.datePosted,
        url: r.cfresId ? `https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfres/res.cfm?id=${r.cfresId}` : null,
        topic: r.reasonForRecall || null,
      });
    }
    bulkData.openfdaRecalls = records;
    log("BULK", `<<< openFDA Recalls: ${records.length} -> Snowflake`);
    results.push({ name: "openFDA Recalls", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< openFDA Recalls FAILED: ${e.message}`); results.push({ name: "openFDA Recalls", status: "FAILED", error: e.message }); }

  // 12. openFDA MAUDE Adverse Events
  log("BULK", ">>> openFDA MAUDE Adverse Events");
  try {
    const records = [];
    const maxPages = process.env.OPENFDA_MAUDE_PAGES ? parseInt(process.env.OPENFDA_MAUDE_PAGES) : 10;
    for (let page = 0; page < maxPages; page++) {
      const data = await fetchJSON(`https://api.fda.gov/device/event.json?limit=100&skip=${page * 100}&sort=date_received:desc`, { headers: { "User-Agent": "eudamed-extraction/1.0" } });
      if (!data.results || data.results.length === 0) break;
      for (const event of data.results) records.push(event);
      await sleep(300, 500);
    }
    for (const r of records) {
      const device = (r.device || [])[0] || {};
      const key = r.mdr_report_key || r.report_number;
      if (!key) continue;
      await insertSafetyNotice("OPENFDA_MAUDE", {
        title: device.generic_name || device.brand_name,
        deviceName: device.generic_name || device.brand_name,
        deviceType: r.event_type,
        status: r.event_type,
        updateDate: r.date_received,
        topic: (r.product_problems || []).join("; ") || null,
        url: `https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/detail.cfm?mdrfoi__id=${key}`,
      });
    }
    bulkData.openfdaMaude = records;
    log("BULK", `<<< openFDA MAUDE: ${records.length} -> SAFETY_NOTICES (source=OPENFDA_MAUDE)`);
    results.push({ name: "openFDA MAUDE", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< openFDA MAUDE FAILED: ${e.message}`); results.push({ name: "openFDA MAUDE", status: "FAILED", error: e.message }); }

  // 14. ClinicalTrials.gov (device interventional studies)
  log("BULK", ">>> ClinicalTrials.gov (device interventional studies)");
  try {
    const records = [];
    const maxPages = process.env.CTGOV_PAGES ? parseInt(process.env.CTGOV_PAGES) : 5;
    let nextPageToken = null;
    for (let page = 0; page < maxPages; page++) {
      const params = new URLSearchParams({
        "query.intr": "device", "filter.overallStatus": "COMPLETED|RECRUITING|ACTIVE_NOT_RECRUITING|TERMINATED", pageSize: "100", format: "json",
      });
      if (nextPageToken) params.set("pageToken", nextPageToken);
      const data = await fetchJSON(`https://clinicaltrials.gov/api/v2/studies?${params.toString()}`, { headers: { Accept: "application/json", "User-Agent": "eudamed-extraction/1.0" } });
      const studies = data.studies || [];
      if (studies.length === 0) break;
      for (const s of studies) records.push(s);
      nextPageToken = data.nextPageToken;
      if (!nextPageToken) break;
      await sleep(400, 300);
    }
    for (const r of records) await insertClinicalTrial(r);
    bulkData.clinicalTrials = records;
    log("BULK", `<<< ClinicalTrials.gov: ${records.length} -> Snowflake`);
    results.push({ name: "ClinicalTrials.gov", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< ClinicalTrials.gov FAILED: ${e.message}`); results.push({ name: "ClinicalTrials.gov", status: "FAILED", error: e.message }); }

  // 15. Europe PMC (full-text biomedical articles with abstracts)
  log("BULK", ">>> Europe PMC (full-text articles)");
  try {
    const records = [];
    const terms = ["medical device adverse event", "implant failure", "stent complication", "pacemaker recall", "surgical mesh safety", "prosthesis post-market", "defibrillator malfunction", "heart valve complication", "hip replacement failure", "device vigilance"];
    for (const term of terms) {
      try {
        const url = `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=${encodeURIComponent(term)}&resultType=core&format=json&pageSize=50`;
        const data = await fetchJSON(url, { headers: { Accept: "application/json", "User-Agent": "eudamed-extraction/1.0" } });
        const results = data.resultList?.result || [];
        for (const a of results) records.push({ ...a, searchTerm: term });
        log("BULK", `  Europe PMC "${term}": ${results.length} articles`);
      } catch (err) { log("BULK", `  Europe PMC "${term}" FAILED: ${err.message}`); }
      await sleep(500, 300);
    }
    for (const r of records) await insertEuropePmc(r);
    bulkData.europePmc = records;
    log("BULK", `<<< Europe PMC: ${records.length} -> Snowflake`);
    results.push({ name: "Europe PMC", status: "SUCCESS" });
  } catch (e) { log("BULK", `<<< Europe PMC FAILED: ${e.message}`); results.push({ name: "Europe PMC", status: "FAILED", error: e.message }); }

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

  // PHASE 1: Bulk scrapers -> Snowflake (skippable via SKIP_PHASE1=true)
  console.log("");
  const skipPhase1 = (process.env.SKIP_PHASE1 || "").toLowerCase() === "true";
  let bulkResults = [];
  if (skipPhase1) {
    log("MAIN", "====== PHASE 1: SKIPPED (SKIP_PHASE1=true, relying on existing Snowflake data) ======");
    console.log("");
  } else {
    log("MAIN", "====== PHASE 1: Bulk Data Sources -> Snowflake ======");
    console.log("");
    bulkResults = await runBulkScrapers();
  }

  // PHASE 2: Per-device -> Snowflake (skippable via SKIP_PHASE2=true)
  let globalDeviceCount = 0;
  const skipPhase2 = (process.env.SKIP_PHASE2 || "").toLowerCase() === "true";
  if (skipPhase2) {
    log("MAIN", "====== PHASE 2: SKIPPED (SKIP_PHASE2=true) ======");
    console.log("");
  } else {
    log("MAIN", "====== PHASE 2: Per-Device Extraction -> Snowflake ======");
    log("MAIN", "Each device: basic + detail + certs + PubMed adverse/clinical -> Snowflake");
    console.log("");

    const maxDevicesEnv = (process.env.MAX_DEVICES || "").trim();
    const maxDevices = maxDevicesEnv && !isNaN(parseInt(maxDevicesEnv)) ? parseInt(maxDevicesEnv) : null;
    const startPageEnv = (process.env.START_PAGE || "0").trim();
    const startPage = parseInt(startPageEnv) || 0;
    const { totalPages, totalElements } = await fetchDevicesPage(0, 5);
    log("EUDAMED", `Total devices: ${totalElements.toLocaleString()} | Pages: ${totalPages.toLocaleString()} (300/page)${maxDevices ? ` | MAX_DEVICES=${maxDevices}` : ""}${startPage > 0 ? ` | START_PAGE=${startPage}` : ""}`);
    console.log("");

    for (let page = startPage; page < totalPages; page++) {
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

      if (maxDevices) contents = contents.slice(0, maxDevices - globalDeviceCount);

      for (let j = 0; j < contents.length; j++) {
        await processOneDevice(contents[j], j + 1, contents.length);
        await sleep(1000, 1000);
      }

      globalDeviceCount += contents.length;
      const dbCount = await getTableCount("DEVICES");
      log("EUDAMED", `Page ${page + 1} complete | Snowflake DEVICES: ${dbCount} rows | Processed this run: ${globalDeviceCount}`);
      console.log("");
      await sleep(3000, 3000);

      if (maxDevices && globalDeviceCount >= maxDevices) {
        log("EUDAMED", `MAX_DEVICES reached. To resume, set START_PAGE=${page + 1} in next run.`);
        break;
      }
    }
  }

  // Final stats
  console.log("");
  log("MAIN", "====== Final Snowflake Stats ======");
  const tables = ["DEVICES", "MANUFACTURERS", "AUTHORISED_REPRESENTATIVES", "DEVICE_CERTIFICATES", "DEVICE_ADVERSE_EVENTS", "DEVICE_CLINICAL_EVIDENCE", "NOTIFIED_BODIES", "REFUSED_APPLICATIONS", "SAFETY_NOTICES", "CLINICAL_TRIALS", "EUROPE_PMC_ARTICLES"];
  for (const t of tables) { const c = await getTableCount(t); log("MAIN", `  ${t}: ${c} rows`); }

  console.log("");
  console.log("========================================================");
  console.log("                  EXTRACTION SUMMARY");
  console.log("========================================================");
  console.log("  PHASE 1 - Bulk Sources:");
  for (const r of bulkResults) { console.log(`    ${r.status === "SUCCESS" ? "[OK]  " : "[FAIL]"} ${r.name} ${r.error ? "- " + r.error : ""}`); }
  console.log(`  PHASE 2 - Devices: ${globalDeviceCount.toLocaleString()} processed`);
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
