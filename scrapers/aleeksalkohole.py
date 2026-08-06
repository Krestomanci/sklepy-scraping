import asyncio
import re
import pandas as pd
from playwright.async_api import async_playwright

BASE = "https://aleeksalkohole.pl"

KATEGORIE = [
    {"nazwa": "Whisky",          "url": f"{BASE}/pl/c/Whisky/15",          "cat_id": "15"},
    {"nazwa": "Wina",            "url": f"{BASE}/pl/c/Wina/17",            "cat_id": "17"},
    {"nazwa": "Inne Alkohole",   "url": f"{BASE}/pl/c/Inne-Alkohole/18",   "cat_id": "18"},
    {"nazwa": "Wódka",           "url": f"{BASE}/pl/c/Wodka/16",           "cat_id": "16"},
    {"nazwa": "Likiery",         "url": f"{BASE}/pl/c/Likiery/34",         "cat_id": "34"},
    {"nazwa": "Rum",             "url": f"{BASE}/pl/c/Rum/91",             "cat_id": "91"},
]

OUTPUT_FILE = "OUTPUT_ALEEKSALKOHOLE.csv"
NAVIGATION_TIMEOUT = 45000


async def create_browser(playwright):
    return await playwright.chromium.launch(
        headless=True,
        args=[
            "--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
        ],
    )


async def create_context(browser):
    return await browser.new_context(
        viewport={"width": 1280, "height": 900},
        locale="pl-PL",
        timezone_id="Europe/Warsaw",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    )


async def accept_cookies(page):
    for text in ["Akceptuję", "Zgadzam się", "OK", "Rozumiem", "Zaakceptuj"]:
        try:
            await page.locator(f"button:has-text('{text}')").first.click(timeout=2000)
            await page.wait_for_timeout(500)
            return
        except Exception:
            pass


async def fetch_ean(context, url):
    page = await context.new_page()
    try:
        page.set_default_timeout(NAVIGATION_TIMEOUT)
        await page.goto(url, wait_until="networkidle", timeout=NAVIGATION_TIMEOUT)
        await page.wait_for_timeout(1500)

        try:
            text = await page.inner_text("body", timeout=5000)
            m = re.search(r'Kod\s*produktu\s*[:\s]*(\d{8,14})', text)
            if m:
                return m.group(1)
            m = re.search(r'EAN[:\s]*(\d{8,14})', text)
            if m:
                return m.group(1)
        except Exception:
            pass
        return ""
    except Exception:
        return ""
    finally:
        await page.close()


async def parse_listing(page, kategoria_nazwa):
    wyniki = []
    for item in await page.locator(".products .product").all():
        classes_str = await item.get_attribute("class") or ""
        if "product_inactive" in classes_str:
            continue

        try:
            link_el = item.locator("a[href*='/pl/p/']").first
            nazwa = (await link_el.inner_text(timeout=3000)).strip()
            href = await link_el.get_attribute("href") or ""
            if href and not href.startswith("http"):
                href = BASE + href
        except Exception:
            nazwa, href = "", ""

        try:
            price_el = item.locator(".price").first
            price_text = await price_el.inner_text(timeout=2000)
            m = re.search(r"([0-9]+[.,][0-9]{2})", price_text.replace("\xa0", " "))
            cena = m.group(1).replace(",", ".") if m else ""
        except Exception:
            cena = ""

        if href:
            wyniki.append({
                "Nazwa": nazwa or "Brak",
                "Cena_regularna": cena or "Brak",
                "Cena_promocyjna": "Brak",
                "EAN": "",
                "Link": href,
                "Kategoria": kategoria_nazwa,
            })

    return wyniki


async def get_next_page(page, cat_id, current_url):
    current_page = 1
    m = re.search(rf'/{cat_id}/(\d+)$', current_url.rstrip("/"))
    if m:
        current_page = int(m.group(1))

    next_page = current_page + 1
    pattern = re.compile(rf'/{cat_id}/{next_page}$')

    for a in await page.locator("a[href]").all():
        try:
            href = await a.get_attribute("href") or ""
            if pattern.search(href.rstrip("/")):
                if not href.startswith("http"):
                    href = BASE + href
                return href
        except Exception:
            pass
    return None


async def crawl_category(context, page, cat_name, cat_url, cat_id):
    wszystkie = []
    visited = set()
    url = cat_url
    page_idx = 1

    while url and url not in visited:
        visited.add(url)
        print(f"  {cat_name}: strona {page_idx} -> {url}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=NAVIGATION_TIMEOUT)
            if page_idx == 1:
                await accept_cookies(page)
            # Czekaj na produkty
            try:
                await page.locator(".products .product").first.wait_for(timeout=15000)
            except Exception:
                await page.wait_for_timeout(5000)
        except Exception as e:
            print(f"  ! Blad: {str(e)[:100]}")
            break

        records = await parse_listing(page, cat_name)

        # Fetch EAN per product
        for i, rec in enumerate(records):
            if rec["Link"]:
                ean = await fetch_ean(context, rec["Link"])
                rec["EAN"] = ean or "Brak"
                await asyncio.sleep(0.2)
            if (i + 1) % 10 == 0:
                print(f"    EAN: {i+1}/{len(records)}")

        wszystkie.extend(records)
        print(f"    Strona {page_idx}: {len(records)} produktow (total: {len(wszystkie)})")

        url = await get_next_page(page, cat_id, url)
        page_idx += 1
        await asyncio.sleep(0.3)

    print(f"  OK: {cat_name} ({len(wszystkie)} rekordow)")
    return wszystkie


async def main():
    all_results = []
    seen_links = set()

    async with async_playwright() as p:
        browser = await create_browser(p)
        context = await create_context(browser)
        page = await context.new_page()
        page.set_default_timeout(NAVIGATION_TIMEOUT)

        for kat in KATEGORIE:
            records = await crawl_category(context, page, kat["nazwa"], kat["url"], kat["cat_id"])

            # Deduplikacja
            new_records = [r for r in records if r["Link"] not in seen_links]
            for r in new_records:
                seen_links.add(r["Link"])
            all_results.extend(new_records)
            print(f"  Dopisano: {len(new_records)} z \"{kat['nazwa']}\"")

        await page.close()
        await context.close()
        await browser.close()

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"\nGOTOWE: {len(all_results)} produktow -> {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
