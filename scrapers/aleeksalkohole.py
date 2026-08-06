import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://aleeksalkohole.pl"

KATEGORIE = [
    {"nazwa": "Whisky",          "url": f"{BASE}/pl/c/Whisky/15",          "cat_id": "15"},
    {"nazwa": "Wina",            "url": f"{BASE}/pl/c/Wina/17",            "cat_id": "17"},
    {"nazwa": "Inne Alkohole",   "url": f"{BASE}/pl/c/Inne-Alkohole/18",   "cat_id": "18"},
    {"nazwa": "Wódka",           "url": f"{BASE}/pl/c/Wodka/16",           "cat_id": "16"},
    {"nazwa": "Likiery",         "url": f"{BASE}/pl/c/Likiery/34",         "cat_id": "34"},
    {"nazwa": "Rum",             "url": f"{BASE}/pl/c/Rum/91",             "cat_id": "91"},
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


def _fetch_ean_from_product(product_url: str) -> str:
    try:
        r = session.get(product_url, timeout=15)
        r.raise_for_status()
        # Parse with BS4 to get clean text (strips HTML tags)
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        m = re.search(r'Kod\s*produktu\s*[:\s]*(\d{8,14})', text)
        if m:
            return m.group(1)
        m = re.search(r'EAN[:\s]*(\d{8,14})', text)
        if m:
            return m.group(1)
        return ""
    except Exception:
        return ""


def _find_next_page_url(soup: BeautifulSoup, current_url: str, cat_id: str) -> str | None:
    # URL pattern: /pl/c/Whisky/15/2 (cat_id=15, page=2)
    # Distinguish pagination links from filter/subcategory links
    current_page = 1
    m = re.search(rf'/{cat_id}/(\d+)$', current_url.rstrip("/"))
    if m:
        current_page = int(m.group(1))

    # Find all page number links for THIS category
    page_pattern = re.compile(rf'/{cat_id}/(\d+)$')
    all_pages = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "").rstrip("/")
        pm = page_pattern.search(href)
        if pm:
            all_pages.add(int(pm.group(1)))

    next_page = current_page + 1
    if next_page in all_pages:
        base_url = re.sub(rf'/{cat_id}(/\d+)?$', f'/{cat_id}', current_url.rstrip("/"))
        return f"{base_url}/{next_page}"

    return None


def parse_listing_page(html: str, page_url: str, kategoria_nazwa: str):
    soup = BeautifulSoup(html, "html.parser")
    wyniki = []

    for item in soup.select(".products .product"):
        # Name & link
        link_el = item.select_one("a[href*='/pl/p/']")
        if not link_el:
            continue

        nazwa_raw = link_el.get_text(strip=True)
        # Remove brand prefix (uppercase text before actual name)
        # e.g. "BALLANTINE'SBallantine's 10 yo" -> split at case change
        nazwa = _clean_txt(nazwa_raw)

        href = link_el.get("href", "")
        if href and not href.startswith("http"):
            href = BASE + href

        # Price
        price_el = item.select_one(".price")
        cena_regularna = _clean_price(price_el.get_text()) if price_el else ""

        # Promo price
        old_el = item.select_one(".price_old, .old-price, del")
        cena_promocyjna = ""
        if old_el:
            stara = _clean_price(old_el.get_text())
            if stara:
                cena_promocyjna = cena_regularna
                cena_regularna = stara

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena_regularna": cena_regularna or "Brak",
            "Cena_promocyjna": cena_promocyjna or "Brak",
            "EAN": "",  # will be fetched from product page
            "Link": href or page_url,
            "Kategoria": kategoria_nazwa,
        })

    return wyniki, soup


def crawl_category(category_name: str, category_url: str, cat_id: str, throttle: float = 0.3) -> list[dict]:
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

        # Fetch EAN from each product page
        for i, rec in enumerate(records):
            if rec["Link"] and rec["Link"].startswith("http"):
                ean = _fetch_ean_from_product(rec["Link"])
                if ean:
                    rec["EAN"] = ean
                else:
                    rec["EAN"] = "Brak"
                time.sleep(0.15)
            if (i + 1) % 10 == 0:
                print(f"    EAN: {i+1}/{len(records)}")

        wszystkie.extend(records)
        print(f"    Strona {page_idx}: {len(records)} produktow (total: {len(wszystkie)})")

        url = next_url
        page_idx += 1
        time.sleep(throttle)

    print(f"  OK: {category_name} ({len(wszystkie)} rekordow)")
    return wszystkie


def scrapuj_aleeksalkohole(output_file="OUTPUT_ALEEKSALKOHOLE.csv"):
    open(output_file, "w").close()
    first_write = True
    total = 0
    seen_links = set()

    for kat in KATEGORIE:
        cat_rows = crawl_category(kat["nazwa"], kat["url"], kat["cat_id"], throttle=0.3)
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
    scrapuj_aleeksalkohole()
