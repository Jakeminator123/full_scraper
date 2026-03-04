/**
 * fetch_helper.js — High-performance Playwright HTTP bridge
 *
 * Mode 1 — single URL:
 *   node fetch_helper.js <URL>
 *
 * Mode 2 — batch via stdin (used by scraper.py):
 *   echo '["url1","url2"]' | node fetch_helper.js --stdin
 *
 * Optimizations vs original:
 *   - Blocks images, fonts, CSS, analytics (saves ~60% bandwidth)
 *   - Reads PAGE_PAUSE from env (default 0.3s)
 *   - Reuses single Chromium instance, new context per URL
 */

const { chromium } = require("playwright");

const args        = process.argv.slice(2);
const MODE_STDIN  = args[0] === "--stdin";
const MODE_SINGLE = !MODE_STDIN;

const pauseMs = Math.round(parseFloat(process.env.PAGE_PAUSE || "0.2") * 1000);

const CTX_OPTS = {
  userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  locale:    "sv-SE",
  extraHTTPHeaders: { "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8" },
};

const BLOCK_PATTERNS = [
  "**/*.{png,jpg,jpeg,gif,svg,ico,webp,avif}",
  "**/*.{css,woff,woff2,ttf,eot}",
  "**/api.pirsch.io/**",
  "**/fonts.googleapis.com/**",
  "**/fonts.gstatic.com/**",
  "**/www.googletagmanager.com/**",
  "**/www.google-analytics.com/**",
];

async function setupPage(ctx) {
  const page = await ctx.newPage();
  for (const pattern of BLOCK_PATTERNS) {
    await page.route(pattern, route => route.abort());
  }
  return page;
}

async function fetchOne(page, url) {
  const res = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 25000 });
  const status = res?.status() ?? 0;
  if (status === 0 || status >= 400) throw new Error(`HTTP ${status}`);
  return page.content();
}

(async () => {
  if (MODE_SINGLE) {
    const url = args[0];
    if (!url) {
      process.stderr.write("Usage: node fetch_helper.js <URL>\n");
      process.exit(1);
    }
    const browser = await chromium.launch({ headless: true });
    const ctx  = await browser.newContext(CTX_OPTS);
    const page = await setupPage(ctx);
    try {
      process.stdout.write(await fetchOne(page, url));
    } catch (e) {
      process.stderr.write(String(e) + "\n");
      process.exit(1);
    } finally {
      await browser.close();
    }
    return;
  }

  // Batch mode (--stdin)
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  const urls = JSON.parse(chunks.join(""));

  if (!urls || urls.length === 0) {
    process.stderr.write("No URLs\n");
    process.stdout.write("{}");
    process.exit(0);
  }

  const browser = await chromium.launch({ headless: true });
  const results = {};

  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    const ctx  = await browser.newContext(CTX_OPTS);
    const page = await setupPage(ctx);
    try {
      results[url] = await fetchOne(page, url);
    } catch (e) {
      results[url] = null;
      process.stderr.write(`ERR ${url}: ${e.message}\n`);
    } finally {
      await ctx.close();
    }
    if (i < urls.length - 1 && pauseMs > 0) {
      await new Promise(r => setTimeout(r, pauseMs));
    }
  }

  await browser.close();
  process.stdout.write(JSON.stringify(results));
})();
