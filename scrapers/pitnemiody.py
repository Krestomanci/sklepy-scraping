import asyncio
import re
import pandas as pd
from playwright.async_api import async_playwright

BASE = "https://pitnemiody.pl"

KATEGORIE = [
    {"nazwa": "Miód Pitny Półtorak",   "url": f"{BASE}/kategoria/poltoraki"},
    {"nazwa": "Miód Pitny Dwójniak",   "url": f"{BASE}/kategoria/dwojniaki"},
    {"nazwa": "Miód Pitny Trójniak",   "url": f"{BASE}/kategoria/trojniaki"},
    {"nazwa": "Miód Pitny Czwórniak",  "url": f"{BASE}/kategoria/czworniaki"},
    {"nazwa": "Miody zagraniczne",     "url": f"{BASE}/kategoria/zagraniczne"},
    {"nazwa": "Zestawy butelek",       "url": f"{BASE}/kategoria/zestawy-butelek"},
    {"nazwa": "Wina miodowe",          "url": f"{BASE}/kategoria/wina-miodowo-owocowe"},
    {"nazwa": "Absynt",                "url": f"{BASE}/kategoria/absynt"},
    {"nazwa": "Armaniak",              "url": f"{BASE}/kategoria/armaniak"},
    {"nazwa": "Bimber",                "url": f"{BASE}/kategoria/bimber"},
    {"nazwa": "Brandy, winiaki",       "url": f"{BASE}/kategoria/brandy-winiaki"},
    {"nazwa": "Burbon",                "url": f"{BASE}/kategoria/burbon-bourbon"},
    {"nazwa": "Calvados",              "url": f"{BASE}/kategoria/calvados"},
    {"nazwa": "Gin",                   "url": f"{BASE}/kategoria/gin"},
    {"nazwa": "Grappa",                "url": f"{BASE}/kategoria/grappa"},
    {"nazwa": "Koniak",                "url": f"{BASE}/kategoria/koniak-cognac"},
    {"nazwa": "Okowita",               "url": f"{BASE}/kategoria/okowita"},
    {"nazwa": "Rum",                   "url": f"{BASE}/kategoria/rum"},
    {"nazwa": "Spirytus",              "url": f"{BASE}/kategoria/spirytus"},
    {"nazwa": "Śliwowica",             "url": f"{BASE}/kategoria/sliwowica"},
    {"nazwa": "Tequila",               "url": f"{BASE}/kategoria/tequila"},
    {"nazwa": "Wódka czysta",          "url": f"{BASE}/kategoria/wodka-czysta"},
    {"nazwa": "Wódka smakowa",         "url": f"{BASE}/kategoria/wodka-smakowa"},
    {"nazwa": "Whisky",                "url": f"{BASE}/kategoria/whisky"},
    {"nazwa": "Szampany",              "url": f"{BASE}/kategoria/szampany"},
    {"nazwa": "Prosecco",              "url": f"{BASE}/kategoria/prosecco"},
    {"nazwa": "Wino Porto",            "url": f"{BASE}/kategoria/wino-porto"},
    {"nazwa": "Wino Madera",           "url": f"{BASE}/kategoria/wino-madera"},
    {"nazwa": "Braggoty",              "url": f"{BASE}/kategoria/braggoty"},
    {"nazwa": "Cydry",                 "url": f"{BASE}/kategoria/cydry"},
    {"nazwa": "Bitter",                "url": f"{BASE}/kategoria/bitter"},
    {"nazwa": "Likiery",               "url": f"{BASE}/kategoria/likiery"},
    {"nazwa": "Nalewki",               "url": f"{BASE}/kategoria/nalewki"},
    {"nazwa": "Piwa miodowe",          "url": f"{BASE}/kategoria/piwa-miodowe"},
    {"nazwa": "Sake",                  "url": f"{BASE}/kategoria/sake"},
    {"nazwa": "OUTLET Wyprzedaż",      "url": f"{BASE}/kategoria/outlet"},
]

OUTPUT_FILE = "OUTPUT_PITNEMIODY.csv"
NAVIGATION_TIMEOUT = 45000


async def create_browser(playwright):
    return await playwright.chromium.launch(
        headless=True,
        args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu",
              "--disable-blink-features=AutomationControlled"],
    )


async def create_context(browser):
    return await browser.new_context(
        viewport={"width": 1280, "height": 900},
        locale="pl-PL",
        timezone_id="Europe/Warsaw",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    )


async def accept_cookies(page):
    for text in ["Akceptuję wszystkie", "Akceptuję", "Zgadzam się", "OK", "Rozumiem"]:
        try:
            await page.locator(f"button:has-text('{text}')").first.click(timeout=2000)
            await page.wait_for_timeout(500)
            return
        except Exception:
            pass


async def parse_listing(page, kategoria_nazwa):
    wyniki = []
    for item in await page.locator("article.product-miniature").all():
        try:
            name_el = item.locator("h2.product-title a").first
            nazwa = (await name_el.inner_text(timeout=3000)).strip()
            link = await name_el.get_attribute("href") or ""
        except Exception:
            nazwa, link = "", ""

        # EAN z .product-reference
        ean = ""
        try:
            ref_el = item.locator(".product-reference a, .product-reference").first
            ref_text = (await ref_el.inner_text(timeout=2000)).strip()
            m = re.search(r'(\d{8,14})', ref_text)
            if m:
                ean = m.group(1)
        except Exception:
            pass

        # Cena z span.product-price
        cena = ""
        try:
            price_el = item.locator("span.product-price").first
            content = await price_el.get_attribute("content")
            if content:
                cena = content
            else:
                price_text = await price_el.inner_text(timeout=2000)
                m = re.search(r'(\d+[.,]\d{2})', price_text.replace("\xa0", " "))
                if m:
                    cena = m.group(1).replace(",", ".")
        except Exception:
            pass

        # Promo
        cena_promo = ""
        try:
            old_el = item.locator("span.regular-price").first
            old_text = await old_el.inner_text(timeout=1000)
            m = re.search(r'(\d+[.,]\d{2})', old_text.replace("\xa0", " "))
            if m:
                cena_promo = cena
                cena = m.group(1).replace(",", ".")
        except Exception:
            pass

        # Kategoria z kafelka
        kat = kategoria_nazwa
        try:
            cat_el = item.locator(".product-category-name").first
            kat = (await cat_el.inner_text(timeout=1000)).strip() or kategoria_nazwa
        except Exception:
            pass

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena_regularna": cena or "Brak",
            "Cena_promocyjna": cena_promo or "Brak",
            "EAN": ean or "Brak",
            "Link": link,
            "Kategoria": kat,
        })

    return wyniki


async def get_next_page(page):
    try:
        next_el = page.locator("a[rel='next']").first
        href = await next_el.get_attribute("href")
        return href if href else None
    except Exception:
        return None


async def main():
    all_results = []
    seen_links = set()

    async with async_playwright() as p:
        browser = await create_browser(p)
        context = await create_context(browser)
        page = await context.new_page()
        page.set_default_timeout(NAVIGATION_TIMEOUT)

        first_cat = True
        for kat in KATEGORIE:
            visited = set()
            url = kat["url"]
            page_idx = 1
            cat_results = []

            while url and url not in visited:
                visited.add(url)
                print(f"  {kat['nazwa']}: strona {page_idx} -> {url}")

                try:
                    await page.goto(url, wait_until="networkidle", timeout=NAVIGATION_TIMEOUT)
                    if first_cat:
                        await accept_cookies(page)
                        first_cat = False
                    try:
                        await page.locator("article.product-miniature").first.wait_for(timeout=10000)
                    except Exception:
                        await page.wait_for_timeout(3000)
                except Exception as e:
                    print(f"  ! Blad: {str(e)[:100]}")
                    break

                records = await parse_listing(page, kat["nazwa"])
                cat_results.extend(records)
                print(f"    {len(records)} produktow")

                url = await get_next_page(page)
                page_idx += 1
                await asyncio.sleep(0.5)

            # Deduplikacja
            new = [r for r in cat_results if r["Link"] not in seen_links]
            for r in new:
                seen_links.add(r["Link"])
            all_results.extend(new)
            print(f"  OK: {kat['nazwa']} ({len(new)} rekordow)")

        await page.close()
        await context.close()
        await browser.close()

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"\nGOTOWE: {len(all_results)} produktow -> {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
