require("dotenv").config();
const { fetchJSON } = require("../../utils/httpClient");
const { sleep } = require("../../utils/rateLimiter");

const BASE_URL = "https://ec.europa.eu/tools/eudamed/api";
const PAGE_SIZE = 300; // Max allowed per page

/**
 * Fetch devices from EUDAMED search API (udiDiData endpoint)
 * Returns ~1.5M devices with: name, UDI, manufacturer, SRN, risk class, status
 */
async function fetchDevicesPage(page, searchFilters = {}) {
  const params = new URLSearchParams({
    size: String(PAGE_SIZE),
    page: String(page),
    ...searchFilters,
  });

  const url = `${BASE_URL}/devices/udiDiData?${params}`;

  try {
    const data = await fetchJSON(url, {
      headers: {
        Accept: "application/json",
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
      },
    });
    return data;
  } catch (error) {
    console.error(`Error fetching devices page ${page}:`, error.message);
    return { content: [], totalPages: 0, totalElements: 0 };
  }
}

/**
 * Fetch detailed device data from basicUdiData endpoint
 * Returns full manufacturer details, clinical investigation flags, certificates
 */
async function fetchDeviceDetail(uuid) {
  const url = `${BASE_URL}/devices/basicUdiData/${uuid}?languageIso2Code=en`;

  try {
    const data = await fetchJSON(url, {
      headers: {
        Accept: "application/json",
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
      },
    });
    return data;
  } catch (error) {
    console.error(`Error fetching detail for ${uuid}:`, error.message);
    return null;
  }
}

/**
 * Normalize a device list entry into a flat object
 */
function normalizeDevice(device) {
  return {
    basicUdi: device.basicUdi,
    primaryDi: device.primaryDi,
    uuid: device.uuid,
    ulid: device.ulid,
    riskClass: device.riskClass?.code || null,
    tradeName: device.tradeName || null,
    manufacturerName: device.manufacturerName || null,
    manufacturerSrn: device.manufacturerSrn || null,
    deviceStatus: device.deviceStatusType?.code || null,
    manufacturerStatus: device.manufacturerStatus?.code || null,
    isLatestVersion: device.latestVersion,
    versionNumber: device.versionNumber,
    authorisedRepSrn: device.authorisedRepresentativeSrn || null,
    authorisedRepName: device.authorisedRepresentativeName || null,
    reference: device.reference || null,
    applicableLegislation: device.applicableLegislation || null,
  };
}

/**
 * Normalize a basicUdiData detail record
 */
function normalizeDeviceDetail(detail) {
  if (!detail) return null;

  const mfr = detail.manufacturer || {};
  const nb = detail.authorisedRepresentative || {};

  return {
    // Device info
    uuid: detail.uuid,
    ulid: detail.ulid,
    deviceName: detail.deviceName || null,
    deviceModel: detail.deviceModel || null,
    basicUdi: detail.basicUdi?.code || null,
    riskClass: detail.riskClass?.code || null,
    legislation: detail.legislation?.code || null,
    isLegacyDevice: detail.deviceCriterion === "LEGACY",
    versionState: detail.versionState?.code || null,
    lastUpdated: detail.lastUpdated || null,
    versionDate: detail.versionDate || null,

    // Booleans (device characteristics)
    isKit: detail.kit,
    isReagent: detail.reagent,
    isInstrument: detail.instrument,
    isImplantable: detail.implantable,
    isReusable: detail.reusable,
    isSelfTesting: detail.selfTesting,
    isNearPatientTesting: detail.nearPatientTesting,
    isProfessionalTesting: detail.professionalTesting,
    clinicalInvestigationApplicable: detail.clinicalInvestigationApplicable,

    // Manufacturer
    manufacturerUuid: mfr.uuid || null,
    manufacturerName: mfr.name || null,
    manufacturerSrn: mfr.srn || null,
    manufacturerCountry: mfr.countryIso2Code || null,
    manufacturerCountryName: mfr.countryName || null,
    manufacturerAddress: mfr.geographicalAddress || null,
    manufacturerEmail: mfr.electronicMail || null,
    manufacturerStatus: mfr.status?.code || null,

    // Authorized Rep
    authRepSrn: nb.srn || null,
    authRepName: nb.name || null,
    authRepEmail: nb.email || null,

    // Certificates
    certificateCount: detail.deviceCertificateInfoList?.length || 0,
    certificates: (detail.deviceCertificateInfoList || []).map((c) => ({
      certificateNumber: c.certificateNumber,
      issueDate: c.issueDate,
      expiryDate: c.certificateExpiry,
      status: c.status?.code || null,
      notifiedBodyName: c.notifiedBody?.name || null,
      notifiedBodySrn: c.notifiedBody?.srn || null,
    })),

    // Clinical
    clinicalInvestigationLinks: detail.clinicalInvestigationLinks || [],
    linkedSscp: detail.linkedSscp ? {
      referenceNumber: detail.linkedSscp.referenceNumber,
      issueDate: detail.linkedSscp.issueDate,
      validated: detail.linkedSscp.validated,
    } : null,
  };
}

/**
 * Extract all devices from EUDAMED (search list only — fast, no details)
 */
async function extractAllDevices(outputCb, maxPages = null) {
  console.log("=== EUDAMED: Extracting devices (search list) ===");

  let page = 0;
  let totalExtracted = 0;
  let hasMore = true;

  while (hasMore) {
    const data = await fetchDevicesPage(page);

    if (!data.content || data.content.length === 0) {
      hasMore = false;
      break;
    }

    for (const device of data.content) {
      const normalized = normalizeDevice(device);
      if (outputCb) await outputCb(normalized);
      totalExtracted++;
    }

    const totalPages = data.totalPages || 0;
    const totalElements = data.totalElements || 0;
    console.log(
      `Page ${page + 1}/${totalPages} — extracted ${data.content.length} devices (total: ${totalExtracted.toLocaleString()} / ${totalElements.toLocaleString()})`
    );

    page++;

    if (maxPages && page >= maxPages) break;
    if (page >= totalPages) break;

    await sleep(500, 1000); // Be polite
  }

  console.log(`Total devices extracted: ${totalExtracted.toLocaleString()}`);
  return totalExtracted;
}

/**
 * Extract basicUdiData (includes full manufacturer details)
 */
async function extractBasicUdiData(outputCb, maxPages = null) {
  console.log("=== EUDAMED: Extracting devices (basicUdiData w/ manufacturer) ===");

  let page = 0;
  let totalExtracted = 0;
  let hasMore = true;

  while (hasMore) {
    const data = await fetchJSON(
      `${BASE_URL}/devices/basicUdiData?size=${PAGE_SIZE}&page=${page}`,
      {
        headers: {
          Accept: "application/json",
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        },
      }
    ).catch((err) => {
      console.error(`Error fetching basicUdiData page ${page}:`, err.message);
      return { content: [], totalPages: 0, totalElements: 0 };
    });

    if (!data.content || data.content.length === 0) {
      hasMore = false;
      break;
    }

    for (const device of data.content) {
      const normalized = normalizeDeviceDetail(device);
      if (outputCb) await outputCb(normalized);
      totalExtracted++;
    }

    const totalPages = data.totalPages || 0;
    const totalElements = data.totalElements || 0;
    console.log(
      `Page ${page + 1}/${totalPages} — extracted ${data.content.length} devices (total: ${totalExtracted.toLocaleString()} / ${totalElements.toLocaleString()})`
    );

    page++;

    if (maxPages && page >= maxPages) break;
    if (page >= totalPages) break;

    await sleep(500, 1000);
  }

  console.log(`Total basicUdiData records extracted: ${totalExtracted.toLocaleString()}`);
  return totalExtracted;
}

module.exports = {
  extractAllDevices,
  extractBasicUdiData,
  fetchDevicesPage,
  fetchDeviceDetail,
  normalizeDevice,
  normalizeDeviceDetail,
};

if (require.main === module) {
  (async () => {
    // Quick test: fetch 1 page
    const data = await fetchDevicesPage(0);
    console.log(`Total devices in EUDAMED: ${data.totalElements?.toLocaleString()}`);
    console.log("Sample device:");
    console.log(JSON.stringify(data.content[0], null, 2));
  })().catch(console.error);
}
