"""Wspolne funkcje Playwright dla scraperow cen (Frisco, Auchan, Mamyito)."""

import asyncio
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright

NAVIGATION_TIMEOUT = 45000
SCREENSHOT_TIMEOUT = 30000
DELAY_SECONDS = 2
RESTART_EVERY = 10


async def create_browser(playwright):
    return await playwright.chromium.launch(
        headless=True,
        args=[
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-blink-features=AutomationControlled",
            "--disable-extensions",
            "--no-first-run",
        ],
    )


async def create_context(browser, storage_state=None):
    kwargs = {
        "viewport": {"width": 1280, "height": 900},
        "locale": "pl-PL",
        "timezone_id": "Europe/Warsaw",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
    }
    if storage_state:
        kwargs["storage_state"] = storage_state

    context = await browser.new_context(**kwargs)

    async def block_resources(route, request):
        if request.resource_type in ("media", "font", "websocket"):
            await route.abort()
        else:
            await route.continue_()

    await context.route("**/*", block_resources)
    return context


async def accept_cookies(page):
    button_texts = [
        "Akceptuję wszystkie", "Zaakceptuj wszystkie", "Akceptuję",
        "Zgadzam się", "Zaakceptuj", "Zgadzam się i przechodzę do serwisu",
        "Przejdź do serwisu", "Rozumiem", "OK",
    ]
    for text in button_texts:
        try:
            await page.locator(f"button:has-text('{text}')").first.click(timeout=1500)
            await page.wait_for_timeout(500)
            return True
        except Exception:
            pass

    for text in button_texts:
        try:
            await page.get_by_role(
                "button", name=re.compile(text, re.IGNORECASE)
            ).first.click(timeout=1000)
            await page.wait_for_timeout(500)
            return True
        except Exception:
            pass

    css_selectors = [
        "#onetrust-accept-btn-handler",
        ".onetrust-accept-btn-handler",
        "[id*='cookie'][id*='accept']",
        "[class*='cookie'][class*='accept']",
        "[id*='consent'] button",
        "[class*='consent'] button",
        "[aria-label*='Akceptuj']",
        "[aria-label*='Accept']",
    ]
    for sel in css_selectors:
        try:
            await page.locator(sel).first.click(timeout=1000)
            await page.wait_for_timeout(500)
            return True
        except Exception:
            pass

    try:
        await page.evaluate("""
            const sels = [
                '[id*="cookie"]', '[class*="cookie-banner"]', '[class*="cookie-bar"]',
                '[id*="consent"]', '[class*="consent-banner"]',
                '[id*="onetrust"]', '[class*="onetrust"]',
                '[class*="gdpr"]', '[id*="gdpr"]',
            ];
            sels.forEach(s => {
                document.querySelectorAll(s).forEach(el => {
                    if (el.offsetHeight > 50) el.style.display = 'none';
                });
            });
        """)
        return True
    except Exception:
        return False


async def set_postal_code(page, cfg):
    kod = cfg["kod_pocztowy"]
    await page.wait_for_timeout(1500)

    pole = None
    for sel in cfg["postal_input_selectors"]:
        try:
            candidate = page.locator(sel).first
            if await candidate.is_visible(timeout=1500):
                pole = candidate
                break
        except Exception:
            continue

    if not pole:
        return False

    try:
        await pole.click()
        await page.wait_for_timeout(300)
        await pole.press("Control+a")
        await pole.press("Delete")
        await page.wait_for_timeout(200)
        await pole.type(kod.replace("-", ""), delay=120)
        await page.wait_for_timeout(800)

        clicked = False
        for text in cfg.get("postal_confirm_texts", []):
            try:
                btn = page.locator(f"button:has-text('{text}')").first
                if await btn.is_visible(timeout=1000):
                    for _ in range(10):
                        if await btn.get_attribute("disabled") is None:
                            break
                        await page.wait_for_timeout(300)
                    await btn.click()
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            await pole.press("Enter")

        await page.wait_for_timeout(3500)
        return True
    except Exception:
        return False


async def init_session(context, cfg, first_url=None):
    print(f"--- Inicjalizacja sesji: {cfg['name']} ---")
    page = await context.new_page()
    page.set_default_timeout(NAVIGATION_TIMEOUT)

    try:
        await page.goto(cfg["base_url"], wait_until="domcontentloaded",
                        timeout=NAVIGATION_TIMEOUT)
        await page.wait_for_timeout(2000)
        await accept_cookies(page)
        await page.wait_for_timeout(1500)

        if cfg.get("needs_postal_code"):
            ok = await set_postal_code(page, cfg)
            if not ok and first_url:
                await page.goto(first_url, wait_until="domcontentloaded",
                                timeout=NAVIGATION_TIMEOUT)
                await page.wait_for_timeout(3000)
                await set_postal_code(page, cfg)
    finally:
        await page.close()


async def read_price(page, cfg):
    all_sels = cfg.get("price_selectors", []) + cfg.get("price_selectors_fallback", [])
    for sel, label in all_sels:
        try:
            text = await page.locator(sel).first.inner_text(timeout=3000)
            if text.strip():
                return text.strip(), label
        except Exception:
            pass

    # JSON-LD fallback
    try:
        ld_jsons = await page.locator('script[type="application/ld+json"]').all_inner_texts()
        for ld in ld_jsons:
            try:
                data = json.loads(ld)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "Product":
                        offers = item.get("offers", {})
                        if isinstance(offers, list):
                            offers = offers[0] if offers else {}
                        p = offers.get("price") or offers.get("lowPrice")
                        if p:
                            return str(p).replace(".", ","), "JSON-LD"
            except (json.JSONDecodeError, AttributeError):
                continue
    except Exception:
        pass

    return None, None


async def scrape_product(context, url, idx, total, cfg):
    slug = cfg["slug_fn"](url)
    print(f"[{idx}/{total}] {slug}")

    result = {
        "url": url, "slug": slug, "store": cfg["name"],
        "status": "OK", "tytul": "", "wykryta_cena": "",
        "zrodlo_ceny": "", "cena_jednostkowa": "",
        "najnizsza_30dni": "", "blad": "",
    }

    page = None
    try:
        page = await context.new_page()
        page.set_default_timeout(NAVIGATION_TIMEOUT)
        await page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)

        first_sel = cfg["price_selectors"][0][0] if cfg["price_selectors"] else None
        if first_sel:
            try:
                await page.locator(first_sel).first.wait_for(timeout=15000)
            except Exception:
                pass

        await page.wait_for_timeout(1500)

        # Postal popup
        if cfg.get("needs_postal_code") and cfg.get("postal_popup_check"):
            try:
                if await page.locator(cfg["postal_popup_check"]).first.is_visible(timeout=1000):
                    await set_postal_code(page, cfg)
                    await page.wait_for_timeout(2000)
            except Exception:
                pass

        await accept_cookies(page)
        await page.wait_for_timeout(1000)

        try:
            result["tytul"] = await page.title()
        except Exception:
            pass

        price_text, price_source = await read_price(page, cfg)
        if price_text:
            if "zł" not in price_text.lower():
                price_text = f"{price_text} zł"
            result["wykryta_cena"] = price_text
            result["zrodlo_ceny"] = price_source
        else:
            result["wykryta_cena"] = "[nieodczytane]"

        try:
            unit = await page.locator(
                r"text=/\d+[,.]?\d*\s*(zł\s*)?\/\s*(kg|l|szt)/"
            ).first.inner_text(timeout=2000)
            result["cena_jednostkowa"] = unit.strip()
        except Exception:
            pass

        try:
            lowest = await page.locator(
                "text=/najniższa cena z 30 dni/i"
            ).first.inner_text(timeout=2000)
            result["najnizsza_30dni"] = lowest.strip()
        except Exception:
            pass

        print(f"  + {result['wykryta_cena']} ({result['zrodlo_ceny']})")

    except Exception as e:
        result["status"] = "BLAD"
        result["blad"] = str(e)[:200]
        print(f"  x BLAD: {result['blad']}")
        if any(s in str(e) for s in ["Target page", "browser has been closed", "Browser closed"]):
            raise
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass

    return result


async def run_store(cfg, urls):
    print(f"\n{'='*60}")
    print(f"SCRAPING: {cfg['name']} — {len(urls)} produktow")
    print(f"{'='*60}\n")

    results = []
    storage_state = None

    async with async_playwright() as p:
        browser = await create_browser(p)
        context = await create_context(browser)
        await init_session(context, cfg, first_url=urls[0] if urls else None)

        try:
            storage_state = await context.storage_state()
        except Exception:
            pass

        since_restart = 0

        try:
            for i, url in enumerate(urls, 1):
                if since_restart >= RESTART_EVERY:
                    try:
                        storage_state = await context.storage_state()
                        await context.close()
                        await browser.close()
                    except Exception:
                        pass
                    browser = await create_browser(p)
                    context = await create_context(browser, storage_state=storage_state)
                    since_restart = 0

                try:
                    r = await scrape_product(context, url, i, len(urls), cfg)
                    results.append(r)
                    since_restart += 1
                except Exception as e:
                    if any(s in str(e) for s in [
                        "Target page", "browser has been closed", "Browser closed"
                    ]):
                        try:
                            await context.close()
                            await browser.close()
                        except Exception:
                            pass
                        await asyncio.sleep(2)
                        browser = await create_browser(p)
                        context = await create_context(browser, storage_state=storage_state)
                        since_restart = 0
                        try:
                            r = await scrape_product(context, url, i, len(urls), cfg)
                            results.append(r)
                            since_restart += 1
                        except Exception as e2:
                            results.append({
                                "url": url, "slug": cfg["slug_fn"](url),
                                "store": cfg["name"], "status": "BLAD",
                                "blad": str(e2)[:200], "tytul": "",
                                "wykryta_cena": "", "zrodlo_ceny": "",
                                "cena_jednostkowa": "", "najnizsza_30dni": "",
                            })
                    else:
                        results.append({
                            "url": url, "slug": cfg["slug_fn"](url),
                            "store": cfg["name"], "status": "BLAD",
                            "blad": str(e)[:200], "tytul": "",
                            "wykryta_cena": "", "zrodlo_ceny": "",
                            "cena_jednostkowa": "", "najnizsza_30dni": "",
                        })

                if i < len(urls):
                    await asyncio.sleep(DELAY_SECONDS)
        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

    ok = sum(1 for r in results if r["status"] == "OK")
    print(f"\nGOTOWE: {ok}/{len(results)} OK")
    return results


def save_csv(results, output_path):
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Lp", "Slug", "Status", "Tytul", "Cena", "Zrodlo ceny",
            "Cena jednostkowa", "Najnizsza 30 dni", "URL", "Blad",
        ])
        for i, r in enumerate(results, 1):
            writer.writerow([
                i, r["slug"], r["status"], r["tytul"],
                r["wykryta_cena"], r.get("zrodlo_ceny", ""),
                r.get("cena_jednostkowa", ""),
                r.get("najnizsza_30dni", ""),
                r["url"], r["blad"],
            ])
    print(f"CSV: {output_path}")
