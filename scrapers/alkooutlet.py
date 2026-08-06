import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://alkooutlet.pl"

# Kategorie nadrzedne — podkategorie sa w srodku
KATEGORIE = [
    {"nazwa": "Whisky",             "url": f"{BASE}/whisky"},
    {"nazwa": "Wina / Szampany",    "url": f"{BASE}/pl/c/WINA-SZAMPANY/278"},
    {"nazwa": "Rum",                "url": f"{BASE}/pl/c/RUM/19"},
    {"nazwa": "Koniak / Brandy",    "url": f"{BASE}/pl/c/KONIAK-BRANDY/284"},
    {"nazwa": "Wódka",              "url": f"{BASE}/pl/c/WODKA/279"},
    {"nazwa": "Gin",                "url": f"{BASE}/pl/c/GIN/17"},
    {"nazwa": "Bezalkoholowe 0%",   "url": f"{BASE}/pl/c/BEZALKOHOLOWE-0/249"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

session = requests.Session()
session.headers.update(HEADERS)

OUTPUT_FILE = "OUTPUT_ALKOOUTLET.csv"


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
        # meta itemprop="sku"
        sku = soup.select_one('meta[itemprop="sku"]')
        if sku and sku.get("content"):
            val = sku["content"].strip()
            if re.match(r'^\d{8,14}$', val):
                return val
        # .code span
        code = soup.select_one('.code span')
        if code:
            val = re.sub(r'\D', '', code.get_text(strip=True))
            if 8 <= len(val) <= 14:
                return val
        # Fallback: regex on text
        text = soup.get_text(" ", strip=True)
        m = re.search(r'Kod\s*produktu\s*[:\s]*(\d{8,14})', text)
        if m:
            return m.group(1)
        return ""
    except Exception:
        return ""


def _find_next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    # Shoper: /whisky/2, /pl/c/RUM/19/2
    a = soup.select_one("a.pagination_next")
    if a and a.get("href"):
        return urljoin(current_url, a["href"])
    # Fallback: next numbered page
    current_page = 1
    m = re.search(r'/(\d+)$', current_url.rstrip("/"))
    if m:
        current_page = int(m.group(1))

    all_pages = set()
    for a in soup.select(".paginator a, .pagination a"):
        href = a.get("href", "")
        pm = re.search(r'/(\d+)$', href.rstrip("/"))
        if pm:
            all_pages.add(int(pm.group(1)))

    next_page = current_page + 1
    if next_page in all_pages:
        base = re.sub(r'/\d+$', '', current_url.rstrip("/"))
        return f"{base}/{next_page}"

    return None


def parse_listing_page(html: str, page_url: str, kategoria_nazwa: str):
    soup = BeautifulSoup(html, "html.parser")
    wyniki = []

    for item in soup.select(".product-inner-wrap"):
        # Name
        name_el = item.select_one(".productname")
        nazwa = _clean_txt(name_el.get_text()) if name_el else ""

        # Link
        link_el = item.select_one("a[href*='/pl/p/']")
        link = ""
        if link_el:
            link = link_el.get("href", "")
            if link and not link.startswith("http"):
                link = BASE + link

        # Price
        price_el = item.select_one("em.main-price, div.price em")
        cena = _clean_price(price_el.get_text()) if price_el else ""

        # Old price (promo)
        old_el = item.select_one("del:not(.none)")
        cena_promo = ""
        if old_el and old_el.get_text(strip=True):
            stara = _clean_price(old_el.get_text())
            if stara:
                cena_promo = cena
                cena = stara

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena": cena or "Brak",
            "Link": link or page_url,
            "Kategoria": kategoria_nazwa,
        })

    return wyniki, soup


def crawl_category(category_name: str, category_url: str, throttle: float = 0.3) -> list[dict]:
    wszystkie = []
    visited = set()
    url = category_url
    page_idx = 1

    while url and url not in visited:
        visited.add(url)
        print(f"  {category_name}: strona {page_idx} -> {url}")

        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"  ! Blad: {e}")
            break

        records, soup = parse_listing_page(r.text, url, category_name)
        next_url = _find_next_page_url(soup, url)

        # Fetch EAN from each product page
        for i, rec in enumerate(records):
            if rec["Link"].startswith("http"):
                ean = _fetch_ean(rec["Link"])
                rec["EAN"] = ean or "Brak"
                time.sleep(0.2)
            else:
                rec["EAN"] = "Brak"
            if (i + 1) % 10 == 0:
                print(f"    EAN: {i+1}/{len(records)}")

        wszystkie.extend(records)
        print(f"    Strona {page_idx}: {len(records)} produktow (total: {len(wszystkie)})")

        url = next_url
        page_idx += 1
        time.sleep(throttle)

    print(f"  OK: {category_name} ({len(wszystkie)} rekordow)")
    return wszystkie


def scrapuj_alkooutlet():
    open(OUTPUT_FILE, "w").close()
    first_write = True
    total = 0
    seen_links = set()

    for kat in KATEGORIE:
        cat_rows = crawl_category(kat["nazwa"], kat["url"], throttle=0.3)
        if not cat_rows:
            continue

        df = pd.DataFrame(cat_rows)
        df = df[~df["Link"].isin(seen_links)]
        df = df.drop_duplicates(subset=["Link"])
        seen_links.update(df["Link"].tolist())

        df.to_csv(
            OUTPUT_FILE,
            index=False,
            encoding="utf-8-sig",
            mode="a",
            header=first_write,
        )
        first_write = False
        total += len(df)
        print(f"  Dopisano: {len(df)} z \"{kat['nazwa']}\"")

    print(f"\nGOTOWE: {total} produktow -> {OUTPUT_FILE}")


if __name__ == "__main__":
    scrapuj_alkooutlet()
