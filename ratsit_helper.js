/**
 * ratsit_helper.js — Ratsit bridge for Phase 0 and Phase 3
 *
 * Mode 1 — date harvesting (Phase 0):
 *   Input:  { "date": "YYYY-MM-DD", "gender": "m"|"f" }
 *   Output: [{ pnr, name, givenName, age, ... phone, neighbours }, ...]
 *
 * Mode 2 — enrich existing people (Phase 3):
 *   Input:  { "mode": "enrich", "people": [{ "pnr": "...", "namn": "...", "stad": "..." }, ...] }
 *   Output: [{ pnr, gender, married, hasCorporateEngagements, lat, lng,
 *              givenName, phone, neighbours }, ...]
 *   Searches "namn PNR-datum" on Ratsit → 1 hit → profile visit → extra fields.
 *   Much faster than date-harvesting: 1 search + 1 profile per person.
 */

const { chromium } = require("playwright");

const args = process.argv.slice(2);
const USE_STDIN = args[0] === "--stdin";

const PHASE0_PAUSE_MS = Math.round(
  parseFloat(process.env.PHASE0_PAUSE || "1.5") * 1000
);

const BLOCK_PATTERNS = [
  "**/*.{png,jpg,jpeg,gif,svg,ico,webp,avif}",
  "**/*.{css,woff,woff2,ttf,eot}",
  "**/api.pirsch.io/**",
  "**/fonts.googleapis.com/**",
  "**/fonts.gstatic.com/**",
  "**/www.googletagmanager.com/**",
  "**/www.google-analytics.com/**",
  "**/analytics.tiktok.com/**",
  "**/sentry.io/**",
];

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
];

// ─── helpers ─────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jitter(baseMs) {
  const factor = 0.6 + Math.random() * 0.8;
  return Math.max(200, Math.round(baseMs * factor));
}

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
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
    await sleep(300);
  } catch {}
}

// ─── search: intercept /api/search/combined ─────────────────────────────────

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
  return { promise, cleanup: () => page.off("response", handler) };
}

function buildSearchUrl(dateStr, gender, pageNum) {
  const params = new URLSearchParams({
    vem: dateStr.replace(/-/g, ""),
    m: gender === "m" ? "1" : "0",
    k: gender === "f" ? "1" : "0",
    r: "0",
    er: "0",
    b: "0",
    eb: "0",
    amin: "16",
    amax: "120",
    fon: "0",
    page: String(pageNum),
  });
  return `https://www.ratsit.se/sok/person?${params}`;
}

async function searchRatsit(page, dateStr, gender) {
  const hits = [];

  const url1 = buildSearchUrl(dateStr, gender, 1);
  const intercept = interceptSearchApi(page);
  await page.goto(url1, { waitUntil: "domcontentloaded", timeout: 30000 });
  await dismissCookies(page);
  const data = await intercept.promise;
  intercept.cleanup();

  if (!data?.person) return hits;

  const pageCount = Math.min(data.person.pager?.pageCount || 1, 3);
  const rawHits = (data.person.hits || []).filter((h) => !h.hidden);
  hits.push(...rawHits);

  for (let p = 2; p <= pageCount; p++) {
    await sleep(jitter(PHASE0_PAUSE_MS));
    const urlN = buildSearchUrl(dateStr, gender, p);
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

// ─── resolve PNR + phone + grannar from profile page ─────────────────────────

async function resolveProfile(page, profileUrl) {
  const url = profileUrl.startsWith("http")
    ? profileUrl
    : `https://www.ratsit.se${profileUrl}`;

  let pnr = null;
  let bilData = null;
  let grannarData = null;

  const handler = async (resp) => {
    const rUrl = resp.url();
    try {
      if (rUrl.includes("/person/biluppgifter/")) {
        bilData = await resp.json();
        const m = bilData?.subjectUri?.match(/brukare\/([A-Za-z0-9+/=]+)/);
        if (m) pnr = base64ToPnr(m[1]);
      }
      if (rUrl.includes("/personer/grannar/")) {
        grannarData = await resp.json();
      }
    } catch {}
  };

  page.on("response", handler);
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
  } catch {}
  page.off("response", handler);

  let phone = "";
  try {
    const text = await page.evaluate(() => document.body.innerText);
    const pm = text.match(/Telefonnummer\s*\n([^\n]+)/);
    if (pm && !pm[1].includes("saknas")) {
      const raw = pm[1].trim();
      const numMatch = raw.match(/^(\d[\d\s-]{6,15}\d)/);
      phone = numMatch ? numMatch[1].trim() : "";
    }
  } catch {}

  return {
    pnr,
    phone,
    vehiclesOnAddress: bilData?.vehiclesOnAddress?.length ?? 0,
    neighbours: grannarData?.numberOfNeighbours ?? -1,
  };
}

// ─── enrich: search "name PNR-date" → 1 hit → profile ──────────────────────

function buildEnrichSearchUrl(query) {
  const params = new URLSearchParams({
    vem: query,
    m: "0", k: "0",
    r: "0", er: "0", b: "0", eb: "0",
    amin: "16", amax: "120", fon: "0",
    page: "1",
  });
  return `https://www.ratsit.se/sok/person?${params}`;
}

async function enrichOne(page, pnr, namn, stad) {
  const dateStr = pnr.replace("-", "").substring(0, 8);
  const query = `${namn} ${dateStr}`;
  const searchUrl = buildEnrichSearchUrl(query);

  let searchData = null;
  const searchHandler = async (resp) => {
    if (resp.url().includes("/api/search/combined")) {
      try { searchData = await resp.json(); } catch {}
    }
  };
  page.on("response", searchHandler);
  try {
    await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
    await dismissCookies(page);
    await sleep(jitter(1500));
  } catch (e) {
    page.off("response", searchHandler);
    return null;
  }
  page.off("response", searchHandler);

  if (!searchData?.person?.hits?.length) return null;

  const hit = searchData.person.hits.find((h) => !h.hidden) || searchData.person.hits[0];
  if (!hit?.personUrl) return null;

  await sleep(jitter(PHASE0_PAUSE_MS));
  const profile = await resolveProfile(page, hit.personUrl);

  return {
    pnr,
    gender: hit.gender || "",
    married: hit.married ?? null,
    hasCorporateEngagements: hit.hasCorporateEngagements ?? null,
    givenName: hit.givenName || "",
    lat: hit.coordinates?.lat || "",
    lng: hit.coordinates?.lng || "",
    phone: profile.phone,
    neighbours: profile.neighbours,
  };
}

// ─── main ────────────────────────────────────────────────────────────────────

(async () => {
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

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: pickRandom(USER_AGENTS),
    locale: "sv-SE",
    extraHTTPHeaders: { "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8" },
  });
  const page = await ctx.newPage();
  for (const pattern of BLOCK_PATTERNS) {
    await page.route(pattern, (route) => route.abort());
  }

  // ── Mode: enrich ────────────────────────────────────────────────────────
  if (input.mode === "enrich") {
    const people = input.people || [];
    const results = [];
    try {
      for (let i = 0; i < people.length; i++) {
        const p = people[i];
        if (!p.pnr || !p.namn) continue;
        process.stderr.write(`  enrich ${i + 1}/${people.length}: ${p.namn}\n`);
        try {
          const enriched = await enrichOne(page, p.pnr, p.namn, p.stad || "");
          if (enriched) results.push(enriched);
        } catch (e) {
          process.stderr.write(`  ERR ${p.pnr}: ${e.message}\n`);
        }
        if (i < people.length - 1) await sleep(jitter(PHASE0_PAUSE_MS));
      }
    } catch (e) {
      process.stderr.write(`Fatal: ${e.message}\n`);
    } finally {
      await browser.close();
    }
    process.stdout.write(JSON.stringify(results));
    return;
  }

  // ── Mode: date harvesting (default) ─────────────────────────────────────
  const { date: dateStr, gender } = input;
  if (!dateStr || !gender) {
    process.stderr.write('Missing required fields: "date" and "gender"\n');
    process.stdout.write("[]");
    await browser.close();
    process.exit(1);
  }

  const results = [];
  try {
    const rawHits = await searchRatsit(page, dateStr, gender);
    process.stderr.write(
      `${dateStr} gender=${gender}: ${rawHits.length} raw hits\n`
    );

    for (let i = 0; i < rawHits.length; i++) {
      const h = rawHits[i];
      if (!h.personUrl) continue;

      await sleep(jitter(PHASE0_PAUSE_MS));

      let profile = { pnr: null, phone: "", vehiclesOnAddress: 0, neighbours: -1 };
      try {
        profile = await resolveProfile(page, h.personUrl);
      } catch (e) {
        process.stderr.write(`  ERR profile ${h.personUrl}: ${e.message}\n`);
        continue;
      }

      if (!profile.pnr) {
        process.stderr.write(
          `  SKIP ${h.firstName || ""} ${h.lastName || ""} — skyddad\n`
        );
        continue;
      }

      results.push({
        pnr: profile.pnr,
        name: [h.firstName, h.lastName].filter(Boolean).join(" "),
        givenName: h.givenName || "",
        age: h.age || 0,
        streetAddress: h.streetAddress || "",
        city: h.city || "",
        gender: h.gender || "",
        married: h.married ?? null,
        hasCorporateEngagements: h.hasCorporateEngagements ?? null,
        lat: h.coordinates?.lat || "",
        lng: h.coordinates?.lng || "",
        phone: profile.phone,
        vehiclesOnAddress: profile.vehiclesOnAddress,
        neighbours: profile.neighbours,
      });
    }

    process.stderr.write(
      `${dateStr} gender=${gender}: ${results.length} PNRs resolved, ` +
        `${results.filter((r) => r.phone).length} phones\n`
    );
  } catch (e) {
    process.stderr.write(`Fatal error: ${e.message}\n`);
  } finally {
    await browser.close();
  }

  process.stdout.write(JSON.stringify(results));
})();
