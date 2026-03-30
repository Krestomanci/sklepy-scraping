import re
import time
import json
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://alkoholeswiata24.pl"

KATEGORIE = [
    {"nazwa": "Alkohole mocne",           "url": f"{BASE}/alkohole-mocne"},
    {"nazwa": "Wina i szampany",          "url": f"{BASE}/wina-i-szampany"},
    {"nazwa": "Likiery i nalewki",        "url": f"{BASE}/likiery-i-nalewki"},
    {"nazwa": "Alkohole niskoprocentowe", "url": f"{BASE}/alkohole-niskoprocentowe"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

session = requests.Session()
session.headers.update(HEADERS)

# -------------- HELPERY --------------

def _clean_txt(s):
    return re.sub(r"\s+", " ", s or "").strip()

def _clean_price(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"([0-9]+[.,][0-9]{2})", text.replace("\xa0", " "))
    return m.group(1).replace(",", ".") if m else ""

def _find_next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    a = soup.select_one('a[rel="next"]')
    if a and a.get("href"):
        return urljoin(current_url, a["href"])
    for sel in ["a[aria-label*='Nast']", "a[aria-label*='nast']",
                "a[aria-label*='Next']", "a.next", "li.next a"]:
        a = soup.select_one(sel)
        if a and a.get("href"):
            return urljoin(current_url, a["href"])
    for a in soup.select("a[href]"):
        if a.get_text(strip=True).lower() in ("następna", "następny", "next", "›", ">"):
            return urljoin(current_url, a["href"])
    return None

def _extract_googlelist_items(soup: BeautifulSoup) -> dict:
    ean_map = {}
    scripts = soup.find_all("script", string=True)
    for sc in scripts:
        txt = sc.string or ""
        if "googleList.push" not in txt:
            continue
        m = re.search(r"googleList\.push\(\s*(\{.*?\})\s*\);", txt, flags=re.S)
        if not m:
            continue
        try:
            payload = json.loads(m.group(1))
            items = payload.get("event", {}).get("items", [])
            for it in items:
                loc = str(it.get("location_id", "")).strip()
                ean = str(it.get("item_id", "")).strip()
                if loc and ean:
                    ean_map[loc] = ean
        except Exception:
            try:
                txt_fixed = (m.group(1).replace("&quot;", '"').replace("&amp;", "&"))
                payload = json.loads(txt_fixed)
                items = payload.get("event", {}).get("items", [])
                for it in items:
                    loc = str(it.get("location_id", "")).strip()
                    ean = str(it.get("item_id", "")).strip()
                    if loc and ean:
                        ean_map[loc] = ean
            except Exception:
                pass
    return ean_map

def _fetch_ean_from_product(product_url: str) -> str:
    try:
        r = session.get(product_url, timeout=15)
        r.raise_for_status()
        psoup = BeautifulSoup(r.text, "html.parser")
        el = psoup.select_one("div.productCode .productCodeSwap")
        if el and el.get_text(strip=True):
            digits = re.sub(r"\D", "", el.get_text())
            if 8 <= len(digits) <= 14:
                return digits
        text = psoup.get_text(" ", strip=True)
        m = re.search(r"Kod produktu[:\s]*([0-9]{8,14})", text)
        return m.group(1) if m else ""
    except Exception:
        return ""

# -------------- PARSER LISTINGU --------------

def parse_listing_page(html: str, page_url: str, kategoria_nazwa: str):
    soup = BeautifulSoup(html, "html.parser")
    ean_map = _extract_googlelist_items(soup)

    wyniki = []
    for item in soup.select("div.colItems div.item.AjaxBasket"):
        loc_id = (item.get("data-id") or "").strip()

        a_name = (item.select_one(".productName a[href]") or
                  item.select_one("a.mainImage[href]") or
                  item.select_one("a.productText[href]"))
        link = urljoin(page_url, a_name["href"]) if (a_name and a_name.get("href")) else ""

        name_el = item.select_one(".productName a span") or item.select_one("a.productText .productDescription")
        nazwa = _clean_txt(name_el.get_text()) if name_el else ""

        price_old = item.select_one(".productPrices .priceOld.priceGross")
        price_discount = item.select_one(".productPrices .priceDiscount.priceGross")
        price_gross = item.select_one(".productPrices .price.priceGross")

        cena_regularna = _clean_price(price_old.get_text(" ", strip=True)) if price_old else ""
        cena_promocyjna = _clean_price(price_discount.get_text(" ", strip=True)) if price_discount else ""

        if not (cena_regularna or cena_promocyjna) and price_gross:
            cena_regularna = _clean_price(price_gross.get_text(" ", strip=True))

        ean = ean_map.get(loc_id, "")

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena_regularna": cena_regularna or "Brak",
            "Cena_promocyjna": cena_promocyjna or "Brak",
            "EAN": ean or "Brak",
            "Link": link or page_url,
            "Kategoria": kategoria_nazwa,
            "_location_id": loc_id
        })

    return wyniki, _find_next_page_url(soup, page_url)

# -------------- GŁÓWNE CRAWLOWANIE --------------

def crawl_category(category_name: str, category_url: str, throttle: float = 0.5) -> list[dict]:
    """Zwraca listę rekordów JEDNEJ kategorii (bez zapisu do pliku)."""
    wszystkie = []
    visited = set()
    url = category_url
    page_idx = 1

    while url and url not in visited:
        visited.add(url)
        print(f"📄 {category_name}: strona {page_idx} -> {url}")

        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"❌ Błąd pobierania {url}: {e}")
            break

        records, next_url = parse_listing_page(r.text, url, category_name)

        # dociąg EAN-ów z podstron (gdy brak)
        for rec in records:
            if (not rec["EAN"] or rec["EAN"] == "Brak") and rec["Link"].startswith(BASE):
                ean_try = _fetch_ean_from_product(rec["Link"])
                if ean_try:
                    rec["EAN"] = ean_try
                time.sleep(0.2)

        wszystkie.extend(records)

        url = next_url
        page_idx += 1
        time.sleep(throttle)

    print(f"✅ Zakończono kategorię: {category_name} (rekordów: {len(wszystkie)})")
    return wszystkie

def scrapuj_alkoholeswiata24(output_file="OUTPUT_alkoholeswiata24.csv"):
    # wyczyść na starcie (jeden raz)
    open(output_file, "w").close()
    first_write = True  # kontrola nagłówka

    for kat in KATEGORIE:
        cat_rows = crawl_category(kat["nazwa"], kat["url"], throttle=0.6)

        if not cat_rows:
            continue

        df = pd.DataFrame(cat_rows)
        if "_location_id" in df.columns:
            df = df.drop(columns=["_location_id"])
        # opcjonalnie deduplikacja po linku (na wypadek klonów)
        df = df.drop_duplicates(subset=["Link"])

        # dopisuj kategorię do wspólnego pliku
        df.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig",
            mode="a",                 # APPEND
            header=first_write        # nagłówek tylko raz
        )
        first_write = False

        print(f"💾 Dopisano: {len(df)} rekordów z kategorii „{kat['nazwa']}”.")

    print("\n🎉 Scraping zakończony – wszystkie kategorie w jednym pliku.")

# -------------- RUN --------------

if __name__ == "__main__":
    scrapuj_alkoholeswiata24()
