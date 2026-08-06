import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://www.forfiterexclusive.pl"

# Scrapujemy /alkohole/ (wszystko) — 228 stron, ~2733 produktow
# EAN pobierany z podstrony produktu (dl.data-sheet -> Identyfikator)
KATEGORIA_URL = f"{BASE}/alkohole/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

session = requests.Session()
session.headers.update(HEADERS)

OUTPUT_FILE = "OUTPUT_FORFITEREXCLUSIVE.csv"


def _clean_txt(s):
    return re.sub(r"\s+", " ", s or "").strip()


def _clean_price(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ").replace("\u00a0", " ")
    m = re.search(r"([0-9]+[.,][0-9]{2})", text)
    return m.group(1).replace(",", ".") if m else ""


def _fetch_ean(product_url: str) -> str:
    try:
        r = session.get(product_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        # dl.data-sheet -> dt "Identyfikator" -> dd z EAN
        ds = soup.select_one("dl.data-sheet")
        if ds:
            dts = ds.select("dt")
            dds = ds.select("dd")
            for dt, dd in zip(dts, dds):
                if "identyfikator" in dt.get_text(strip=True).lower():
                    ean = re.sub(r"\D", "", dd.get_text(strip=True))
                    if 8 <= len(ean) <= 14:
                        return ean
        # Fallback: regex on text
        text = soup.get_text(" ", strip=True)
        m = re.search(r'Identyfikator\s*[:\s]*(\d{8,14})', text)
        if m:
            return m.group(1)
        return ""
    except Exception:
        return ""


def parse_listing_page(html: str, page_url: str):
    soup = BeautifulSoup(html, "html.parser")
    wyniki = []

    for item in soup.select("article.product-miniature"):
        # Name
        name_el = item.select_one(".product-title a, h2 a, h3 a")
        nazwa = _clean_txt(name_el.get_text()) if name_el else ""
        link = ""
        if name_el and name_el.get("href"):
            link = name_el["href"]
            if not link.startswith("http"):
                link = urljoin(page_url, link)

        # Price
        price_el = item.select_one(".product-price, span[content]")
        if price_el and price_el.get("content"):
            cena = price_el["content"]
        elif price_el:
            cena = _clean_price(price_el.get_text())
        else:
            cena = ""

        # Old price (promo)
        old_el = item.select_one(".regular-price")
        cena_promo = ""
        if old_el:
            stara = _clean_price(old_el.get_text())
            if stara:
                cena_promo = cena
                cena = stara

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena": cena or "Brak",
            "Link": link or page_url,
        })

    # Next page
    next_url = None
    next_el = soup.select_one("a[rel='next']")
    if next_el and next_el.get("href"):
        next_url = urljoin(page_url, next_el["href"])

    return wyniki, next_url


def scrapuj_forfiterexclusive():
    open(OUTPUT_FILE, "w").close()
    wszystkie = []
    visited = set()
    url = KATEGORIA_URL
    page_idx = 1

    while url and url not in visited:
        visited.add(url)
        print(f"  Strona {page_idx}: {url}")

        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"  ! Blad: {e}")
            break

        records, next_url = parse_listing_page(r.text, url)

        # Fetch EAN from each product page
        for i, rec in enumerate(records):
            if rec["Link"].startswith("http"):
                ean = _fetch_ean(rec["Link"])
                rec["EAN"] = ean or "Brak"
                time.sleep(0.3)
            else:
                rec["EAN"] = "Brak"

        wszystkie.extend(records)
        print(f"    {len(records)} produktow (total: {len(wszystkie)})")

        # Autozapis co 100 produktow
        if len(wszystkie) % 100 < len(records):
            pd.DataFrame(wszystkie).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

        url = next_url
        page_idx += 1
        time.sleep(0.5)

    # Final save
    if wszystkie:
        df = pd.DataFrame(wszystkie)
        df = df.drop_duplicates(subset=["Link"])
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"\nGOTOWE: {len(wszystkie)} produktow -> {OUTPUT_FILE}")


if __name__ == "__main__":
    scrapuj_forfiterexclusive()
