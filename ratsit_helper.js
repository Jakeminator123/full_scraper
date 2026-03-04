/**
 * ratsit_helper.js — Ratsit date-harvesting bridge for Phase 0
 *
 * Usage (from scraper.py via subprocess):
 *   echo '{"date":"1986-05-28","gender":"m"}' | node ratsit_helper.js --stdin
 *
 * Input JSON:
 *   { "date": "YYYY-MM-DD", "gender": "m" | "f" }
 *
 * Output JSON (stdout):
 *   [{ "pnr": "19860528-0299", "name": "...", "age": 39, "city": "...", "gender": "male" }, ...]
 *
 * For each date+gender combination this script:
 *   1. Searches Ratsit with the date string + gender filter (up to 3 pages = 30 results)
 *   2. For each hit, visits the profile page and intercepts
 *      GET /person/biluppgifter/ to extract the base64-encoded PNR from subjectUri
 *   3. Returns the full list with PNR resolved
 *
 * Errors are written to stderr and the hit is skipped (partial results returned).
 * An empty array is returned on total failure.
 */

const { chromium } = require("playwright");

const args      = process.argv.slice(2);
const USE_STDIN = args[0] === "--stdin";

const PHASE0_PAUSE_MS = Math.round(
  parseFloat(process.env.PHASE0_PAUSE || "1.0") * 1000
);

// ─── helpers ─────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function base64ToPnr(b64) {
  try {
    const d = Buffer.from(b64, "base64").toString("utf8");
    if (/^\d{12}$/.test(d)) return d.substring(0, 8) + "-" + d.substring(8);
  } catch {}
  return null;
}

function extractPnrFromSubjectUri(uri) {
  const m = uri?.match(/brukare\/([A-Za-z0-9+/=]+)/);
  return m ? base64ToPnr(m[1]) : null;
}

async function dismissCookies(page) {
  try {
    await page
      .getByRole("button", { name: "Tillåt alla cookies" })
      .click({ timeout: 3000 });
    await sleep(400);
  } catch {}
}

// ─── search one page, intercept /api/search/combined ─────────────────────────

function interceptSearchApi(page) {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  const timeout = setTimeout(() => resolve(null), 25000);
  const handler = async (resp) => {
    if (resp.url().includes("/api/search/combined")) {
      try {
        clearTimeout(timeout);
        resolve(await resp.json());
      } catch {
        resolve(null);
      }
    }
  };
  page.on("response", handler);
  return {
    promise,
    cleanup: () => page.off("response", handler),
  };
}

function buildSearchUrl(dateStr, gender, pageNum) {
  const params = new URLSearchParams({
    vem:  dateStr.replace(/-/g, ""),  // YYYYMMDD
    m:    gender === "m" ? "1" : "0",
    k:    gender === "f" ? "1" : "0",
    r:    "0",
    er:   "0",
    b:    "0",
    eb:   "0",
    amin: "16",
    amax: "120",
    fon:  "1",
    page: String(pageNum),
  });
  return `https://www.ratsit.se/sok/person?${params}`;
}

async function searchRatsit(page, dateStr, gender) {
  const hits = [];

  // Page 1
  const url1      = buildSearchUrl(dateStr, gender, 1);
  const intercept = interceptSearchApi(page);
  await page.goto(url1, { waitUntil: "domcontentloaded", timeout: 30000 });
  await dismissCookies(page);
  const data = await intercept.promise;
  intercept.cleanup();

  if (!data?.person) return hits;

  const pageCount = Math.min(data.person.pager?.pageCount || 1, 3);
  const rawHits   = (data.person.hits || []).filter((h) => !h.hidden);
  hits.push(...rawHits);

  // Pages 2-3
  for (let p = 2; p <= pageCount; p++) {
    await sleep(PHASE0_PAUSE_MS);
    const urlN      = buildSearchUrl(dateStr, gender, p);
    const interceptN = interceptSearchApi(page);
    await page.goto(urlN, { waitUntil: "domcontentloaded", timeout: 30000 });
    await dismissCookies(page);
    const nd = await interceptN.promise;
    interceptN.cleanup();
    if (nd?.person?.hits) {
      hits.push(...nd.person.hits.filter((h) => !h.hidden));
    } else {
      break;
    }
  }

  return hits;
}

// ─── resolve PNR from one profile page ───────────────────────────────────────

async function resolvePnr(page, profileUrl) {
  const url = profileUrl.startsWith("http")
    ? profileUrl
    : `https://www.ratsit.se${profileUrl}`;

  let pnr = null;
  const handler = async (resp) => {
    if (!resp.url().includes("/person/biluppgifter/")) return;
    try {
      const data = await resp.json();
      pnr = extractPnrFromSubjectUri(data?.subjectUri);
    } catch {}
  };

  page.on("response", handler);
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
  } catch {}
  page.off("response", handler);
  return pnr;
}

// ─── main ────────────────────────────────────────────────────────────────────

(async () => {
  // Read input
  let input = {};
  if (USE_STDIN) {
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    try {
      input = JSON.parse(chunks.join(""));
    } catch (e) {
      process.stderr.write(`JSON parse error: ${e.message}\n`);
      process.stdout.write("[]");
      process.exit(1);
    }
  } else {
    process.stderr.write("Usage: echo '{...}' | node ratsit_helper.js --stdin\n");
    process.exit(1);
  }

  const { date: dateStr, gender } = input;
  if (!dateStr || !gender) {
    process.stderr.write('Missing required fields: "date" and "gender"\n');
    process.stdout.write("[]");
    process.exit(1);
  }

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    locale: "sv-SE",
    extraHTTPHeaders: { "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8" },
  });
  const page = await ctx.newPage();

  const results = [];

  try {
    // 1. Search Ratsit for this date+gender
    const rawHits = await searchRatsit(page, dateStr, gender);
    process.stderr.write(
      `${dateStr} gender=${gender}: ${rawHits.length} raw hits\n`
    );

    // 2. Resolve PNR for each hit
    for (let i = 0; i < rawHits.length; i++) {
      const h = rawHits[i];
      if (!h.personUrl) continue;

      await sleep(PHASE0_PAUSE_MS);

      let pnr = null;
      try {
        pnr = await resolvePnr(page, h.personUrl);
      } catch (e) {
        process.stderr.write(`  ERR profile ${h.personUrl}: ${e.message}\n`);
        continue;
      }

      if (!pnr) {
        process.stderr.write(
          `  SKIP ${h.firstName || ""} ${h.lastName || ""} — skyddad\n`
        );
        continue;
      }

      results.push({
        pnr,
        name:   [h.firstName, h.lastName].filter(Boolean).join(" "),
        age:    h.age || 0,
        city:   h.city || "",
        gender: h.gender || "",
      });
    }

    process.stderr.write(
      `${dateStr} gender=${gender}: ${results.length} PNRs resolved\n`
    );
  } catch (e) {
    process.stderr.write(`Fatal error: ${e.message}\n`);
  } finally {
    await browser.close();
  }

  process.stdout.write(JSON.stringify(results));
})();
