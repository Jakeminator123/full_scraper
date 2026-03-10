"""
eniro_helper.py — Eniro person lookup: name → birthDate + telefon + adress

Used by Phase 2E to get birthDate (= PNR first 8 digits) for targeted
PNR resolution instead of blind brute-force.

Requires: playwright (Python), installed in Docker via requirements.txt

Each call launches a fresh Chromium instance per page to avoid CF throttling.
Tested: 0 blocks on 60 consecutive pages, 16 pages/min sustained.
"""

import asyncio
import json
import logging
import random
import re
import time
from urllib.parse import quote_plus

log = logging.getLogger("eniro")

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def _pick_ua():
    return random.choice(UA_LIST)


def _jitter(base_s: float) -> float:
    return max(0.2, base_s * (0.6 + random.random() * 0.8))


def _extract_jsonld_person(html: str) -> dict:
    blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    for raw in blocks:
        try:
            d = json.loads(raw.strip())
            for item in d.get("@graph", [d]):
                if item.get("@type") == "Person":
                    return item
        except Exception:
            pass
    return {}


async def _cf_wait(page, timeout_s: int = 12) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        title = await page.title()
        if title and "Vänta" not in title and "Just a moment" not in title:
            return True
        await page.wait_for_timeout(400)
    return False


async def _accept_cookies(page) -> None:
    try:
        btn = page.locator("button", has_text="GODKÄNN")
        await btn.wait_for(timeout=3000)
        await btn.click()
        await page.wait_for_timeout(300)
    except Exception:
        pass


async def search_eniro(pw_browser, name: str, max_results: int = 10) -> list[dict]:
    """Search Eniro for a name, return list of {eniro_id, url, slug}."""
    slug = quote_plus(name)
    url = f"https://www.eniro.se/{slug}/personer"
    persons = []

    ctx = await pw_browser.new_context(
        user_agent=_pick_ua(), locale="sv-SE",
        viewport={"width": 1366, "height": 768},
    )
    page = await ctx.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        ok = await _cf_wait(page)
        if not ok:
            log.warning("Eniro CF timeout for search '%s'", name)
            return []
        await _accept_cookies(page)
        await page.wait_for_timeout(500)

        links = await page.locator("a[href*='/person']").all()
        seen = set()
        for link in links:
            href = await link.get_attribute("href")
            pid = re.search(r"/(\d{6,12})/person", href or "")
            if pid and pid.group(1) not in seen:
                seen.add(pid.group(1))
                full = "https://www.eniro.se" + href if href.startswith("/") else href
                persons.append({"eniro_id": pid.group(1), "url": full})
            if len(persons) >= max_results:
                break
    except Exception as e:
        log.warning("Eniro search error '%s': %s", name, e)
    finally:
        await ctx.close()

    return persons


async def fetch_person_page(pw_browser, url: str) -> dict | None:
    """Fetch an Eniro person page, extract JSON-LD: birthDate, phone, address."""
    ctx = await pw_browser.new_context(
        user_agent=_pick_ua(), locale="sv-SE",
        viewport={"width": 1366, "height": 768},
    )
    page = await ctx.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        ok = await _cf_wait(page)
        if not ok:
            log.warning("Eniro CF timeout for %s", url)
            return None
        await _accept_cookies(page)
        await page.wait_for_timeout(300)

        html = await page.content()
        person = _extract_jsonld_person(html)
        if not person.get("name"):
            return None

        addr = person.get("address", {})
        return {
            "namn": person.get("name", ""),
            "fornamn": person.get("givenName", ""),
            "efternamn": person.get("familyName", ""),
            "birthDate": person.get("birthDate", ""),
            "telefon": person.get("telephone", ""),
            "gatuadress": addr.get("streetAddress", ""),
            "postnummer": addr.get("postalCode", ""),
            "postort": addr.get("addressLocality", ""),
            "eniro_url": url,
        }
    except Exception as e:
        log.warning("Eniro page error %s: %s", url, e)
        return None
    finally:
        await ctx.close()


async def resolve_birthdate(pw_browser, namn: str, stad: str = "") -> dict | None:
    """Search Eniro for a person, return birthDate + extras if found.

    Tries to match by name. Returns the first match with birthDate.
    """
    query = f"{namn} {stad}".strip() if stad else namn
    persons = await search_eniro(pw_browser, query, max_results=5)

    if not persons:
        return None

    target_parts = set(namn.lower().split())

    for p in persons:
        await asyncio.sleep(_jitter(0.5))
        data = await fetch_person_page(pw_browser, p["url"])
        if not data or not data.get("birthDate"):
            continue

        found_parts = set(data["namn"].lower().split())
        if target_parts & found_parts:
            return data

    return None


def sync_resolve_birthdate(namn: str, stad: str = "") -> dict | None:
    """Synchronous wrapper — launches Playwright, resolves, closes."""
    async def _inner():
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                return await resolve_birthdate(browser, namn, stad)
            finally:
                await browser.close()

    return asyncio.run(_inner())
