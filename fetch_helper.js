/**
 * fetch_helper.js — Playwright HTTP bridge for biluppgifter.se
 *
 * Mode 1 — single URL:
 *   node fetch_helper.js <URL>
 *   → writes HTML to stdout
 *
 * Mode 2 — batch via stdin (used by scraper.py):
 *   echo '["url1","url2"]' | node fetch_helper.js --stdin
 *   → writes JSON { "<url>": "<html>"|null, ... } to stdout
 *
 * Config via environment variables:
 *   PAGE_PAUSE  — seconds between requests (default: 0.5)
 */

const { chromium } = require("playwright");

const args        = process.argv.slice(2);
const MODE_STDIN  = args[0] === "--stdin";
const MODE_SINGLE = !MODE_STDIN;

const pauseMs = Math.round(parseFloat(process.env.PAGE_PAUSE || "0.5") * 1000);

const CTX_OPTS = {
  userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  locale:    "sv-SE",
  extraHTTPHeaders: { "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8" },
};

async function fetchOne(page, url) {
  const res = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
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
    const page = await ctx.newPage();
    try {
      const html = await fetchOne(page, url);
      process.stdout.write(html);
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
    process.stderr.write("No URLs provided\n");
    process.stdout.write("{}");
    process.exit(0);
  }

  const browser = await chromium.launch({ headless: true });
  const results = {};

  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    const ctx  = await browser.newContext(CTX_OPTS);
    const page = await ctx.newPage();
    try {
      results[url] = await fetchOne(page, url);
    } catch (e) {
      results[url] = null;
      process.stderr.write(`ERR ${url}: ${e.message}\n`);
    } finally {
      await ctx.close();
    }
    if (i < urls.length - 1) {
      await new Promise(r => setTimeout(r, pauseMs));
    }
  }

  await browser.close();
  process.stdout.write(JSON.stringify(results));
})();
