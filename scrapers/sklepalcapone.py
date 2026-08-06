import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://sklepalcapone.pl"

KATEGORIE = [
    {"nazwa": "Whisky",             "url": f"{BASE}/pl/c/WHISKY/7927",           "cat_id": "7927"},
    {"nazwa": "Rum",                "url": f"{BASE}/pl/c/RUM/15534",             "cat_id": "15534"},
    {"nazwa": "Gin",                "url": f"{BASE}/pl/c/GIN/15536",             "cat_id": "15536"},
    {"nazwa": "Wódki",              "url": f"{BASE}/pl/c/WODKI/15532",           "cat_id": "15532"},
    {"nazwa": "Wina i Szampany",    "url": f"{BASE}/pl/c/WINA-i-SZAMPANY/7920",  "cat_id": "7920"},
    {"nazwa": "Inne Alkohole",      "url": f"{BASE}/pl/c/INNE-ALKOHOLE/12849",   "cat_id": "12849"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

session = requests.Session()
session.headers.update(HEADERS)


def _clean_txt(s):
    return re.sub(r"\s+", " ", s or "").strip()


def _clean_price(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ").replace("\u00a0", " ")
    m = re.search(r"([0-9]+[.,][0-9]{2})", text)
    return m.group(1).replace(",", ".") if m else ""


def _find_next_page_url(soup: BeautifulSoup, current_url: str, cat_id: str) -> str | None:
    current_page = 1
    m = re.search(rf'/{cat_id}/(\d+)', current_url)
    if m:
        current_page = int(m.group(1))

    page_pattern = re.compile(rf'/{cat_id}/(\d+)')
    all_pages = set()
    for a in soup.select("a[href]"):
        pm = page_pattern.search(a.get("href", ""))
        if pm:
            all_pages.add(int(pm.group(1)))

    next_page = current_page + 1
    if next_page in all_pages:
        base = re.sub(rf'/{cat_id}(/\d+)?$', f'/{cat_id}', current_url.rstrip("/"))
        return f"{base}/{next_page}"

    return None


def parse_listing_page(html: str, page_url: str, kategoria_nazwa: str):
    soup = BeautifulSoup(html, "html.parser")
    wyniki = []

    tiles = soup.select(".product-tile")

    # Build EAN map from product-tile__code_sku spans (some tiles have them)
    sku_spans = soup.select(".product-tile__code_sku")
    # Map by position — sku_spans correspond to tiles that have codes
    # Better: iterate tiles and check each one

    for tile in tiles:
        # Name
        name_el = tile.select_one(".product-tile__name")
        nazwa = _clean_txt(name_el.get_text()) if name_el else ""

        # Link
        link_el = tile.select_one("a[href*='/pl/p/']")
        if not link_el:
            link_el = tile.select_one(".product-tile__name a")
        link = ""
        if link_el:
            link = link_el.get("href", "")
            if link and not link.startswith("http"):
                link = BASE + link

        # EAN from product-tile__code_sku
        ean = ""
        sku_el = tile.select_one(".product-tile__code_sku")
        if sku_el:
            ean_text = sku_el.get_text(strip=True)
            m = re.search(r'(\d{8,14})', ean_text)
            if m:
                ean = m.group(1)

        # Price
        price_el = tile.select_one("[class*='price']")
        cena_regularna = _clean_price(price_el.get_text()) if price_el else ""

        # Promo price (old/new)
        old_el = tile.select_one(".product-tile__price--old, del, .old-price")
        new_el = tile.select_one(".product-tile__price--new, ins, .new-price")
        cena_promocyjna = ""
        if old_el and new_el:
            stara = _clean_price(old_el.get_text())
            nowa = _clean_price(new_el.get_text())
            if stara and nowa:
                cena_regularna = stara
                cena_promocyjna = nowa

        # Producer
        prod_el = tile.select_one("[class*='producer']")
        producent = _clean_txt(prod_el.get_text()).replace("Producent", "").strip() if prod_el else ""

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena_regularna": cena_regularna or "Brak",
            "Cena_promocyjna": cena_promocyjna or "Brak",
            "EAN": ean or "Brak",
            "Link": link or page_url,
            "Kategoria": kategoria_nazwa,
        })

    return wyniki, soup


def crawl_category(category_name: str, category_url: str, cat_id: str, throttle: float = 0.5) -> list[dict]:
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
            print(f"  ! Blad pobierania {url}: {e}")
            break

        records, soup = parse_listing_page(r.text, url, category_name)
        next_url = _find_next_page_url(soup, url, cat_id)

        wszystkie.extend(records)
        print(f"    Strona {page_idx}: {len(records)} produktow (total: {len(wszystkie)})")

        url = next_url
        page_idx += 1
        time.sleep(throttle)

    print(f"  OK: {category_name} ({len(wszystkie)} rekordow)")
    return wszystkie


def scrapuj_sklepalcapone(output_file="OUTPUT_SKLEPALCAPONE.csv"):
    open(output_file, "w").close()
    first_write = True
    total = 0
    seen_links = set()

    for kat in KATEGORIE:
        cat_rows = crawl_category(kat["nazwa"], kat["url"], kat["cat_id"], throttle=0.5)
        if not cat_rows:
            continue

        df = pd.DataFrame(cat_rows)
        df = df[~df["Link"].isin(seen_links)]
        df = df.drop_duplicates(subset=["Link"])
        seen_links.update(df["Link"].tolist())

        df.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig",
            mode="a",
            header=first_write,
        )
        first_write = False
        total += len(df)
        print(f"  Dopisano: {len(df)} z \"{kat['nazwa']}\"")

    print(f"\nGOTOWE: {total} produktow -> {output_file}")


if __name__ == "__main__":
    scrapuj_sklepalcapone()
