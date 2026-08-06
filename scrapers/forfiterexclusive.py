import asyncio
import re
import time
import csv
import pandas as pd
from playwright.async_api import async_playwright

BASE = "https://www.forfiterexclusive.pl"
KATEGORIA_URL = f"{BASE}/alkohole/"
OUTPUT_FILE = "OUTPUT_FORFITEREXCLUSIVE.csv"

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
        await page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
        await page.wait_for_timeout(1000)

        # dl.data-sheet -> dt "Identyfikator" -> dd
        try:
            dts = await page.locator("dl.data-sheet dt").all_inner_texts()
            dds = await page.locator("dl.data-sheet dd").all_inner_texts()
            for dt_text, dd_text in zip(dts, dds):
                if "identyfikator" in dt_text.strip().lower():
                    ean = re.sub(r"\D", "", dd_text.strip())
                    if 8 <= len(ean) <= 14:
                        return ean
        except Exception:
            pass

        # Fallback: regex on page text
        try:
            text = await page.inner_text("body", timeout=3000)
            m = re.search(r'Identyfikator\s*[:\s]*(\d{8,14})', text)
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
    for item in await page.locator("article.product-miniature").all():
        try:
            name_el = item.locator(".product-title a, h2 a, h3 a").first
            nazwa = (await name_el.inner_text(timeout=3000)).strip()
            link = await name_el.get_attribute("href") or ""
            if link and not link.startswith("http"):
                link = BASE + link
        except Exception:
            nazwa, link = "", ""

        try:
            price_el = item.locator(".product-price, span[content]").first
            content = await price_el.get_attribute("content")
            if content:
                cena = content
            else:
                cena_text = await price_el.inner_text(timeout=2000)
                m = re.search(r"([0-9]+[.,][0-9]{2})", cena_text.replace("\xa0", " "))
                cena = m.group(1).replace(",", ".") if m else ""
        except Exception:
            cena = ""

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena": cena or "Brak",
            "Link": link,
            "Kategoria": kategoria_nazwa,
        })

    return wyniki


async def get_next_page_url(page):
    try:
        next_el = page.locator("a[rel='next']").first
        href = await next_el.get_attribute("href")
        if href:
            if not href.startswith("http"):
                href = BASE + href
            return href
    except Exception:
        pass
    return None


async def main():
    wszystkie = []
    page_idx = 1
    visited = set()

    async with async_playwright() as p:
        browser = await create_browser(p)
        context = await create_context(browser)

        page = await context.new_page()
        page.set_default_timeout(NAVIGATION_TIMEOUT)

        url = KATEGORIA_URL
        while url and url not in visited:
            visited.add(url)
            print(f"  Strona {page_idx}: {url}")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
                await page.wait_for_timeout(2000)
                if page_idx == 1:
                    await accept_cookies(page)
            except Exception as e:
                print(f"  ! Blad nawigacji: {str(e)[:100]}")
                break

            records = await parse_listing(page, "Alkohole")

            # Fetch EAN per product
            for i, rec in enumerate(records):
                if rec["Link"]:
                    ean = await fetch_ean(context, rec["Link"])
                    rec["EAN"] = ean or "Brak"
                    await asyncio.sleep(0.3)
                else:
                    rec["EAN"] = "Brak"

            wszystkie.extend(records)
            print(f"    {len(records)} produktow (total: {len(wszystkie)})")

            # Autozapis co 100
            if len(wszystkie) % 100 < len(records) and wszystkie:
                pd.DataFrame(wszystkie).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

            url = await get_next_page_url(page)
            page_idx += 1
            await asyncio.sleep(0.5)

        await page.close()
        await context.close()
        await browser.close()

    if wszystkie:
        df = pd.DataFrame(wszystkie)
        df = df.drop_duplicates(subset=["Link"])
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"\nGOTOWE: {len(wszystkie)} produktow -> {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
