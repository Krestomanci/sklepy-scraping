import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://pitnemiody.pl"

KATEGORIE = [
    # Miody pitne (podkategorie)
    {"nazwa": "Miód Pitny Półtorak",   "url": f"{BASE}/kategoria/poltoraki"},
    {"nazwa": "Miód Pitny Dwójniak",   "url": f"{BASE}/kategoria/dwojniaki"},
    {"nazwa": "Miód Pitny Trójniak",   "url": f"{BASE}/kategoria/trojniaki"},
    {"nazwa": "Miód Pitny Czwórniak",  "url": f"{BASE}/kategoria/czworniaki"},
    {"nazwa": "Miody zagraniczne",     "url": f"{BASE}/kategoria/zagraniczne"},
    {"nazwa": "Zestawy butelek",       "url": f"{BASE}/kategoria/zestawy-butelek"},
    {"nazwa": "Wina miodowe",          "url": f"{BASE}/kategoria/wina-miodowo-owocowe"},
    # Destylaty (podkategorie)
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
    # Wina (podkategorie)
    {"nazwa": "Szampany",              "url": f"{BASE}/kategoria/szampany"},
    {"nazwa": "Prosecco",              "url": f"{BASE}/kategoria/prosecco"},
    {"nazwa": "Wino Porto",            "url": f"{BASE}/kategoria/wino-porto"},
    {"nazwa": "Wino Madera",           "url": f"{BASE}/kategoria/wino-madera"},
    # Inne alkohole (podkategorie)
    {"nazwa": "Braggoty",              "url": f"{BASE}/kategoria/braggoty"},
    {"nazwa": "Cydry",                 "url": f"{BASE}/kategoria/cydry"},
    {"nazwa": "Bitter",                "url": f"{BASE}/kategoria/bitter"},
    {"nazwa": "Likiery",               "url": f"{BASE}/kategoria/likiery"},
    {"nazwa": "Nalewki",               "url": f"{BASE}/kategoria/nalewki"},
    {"nazwa": "Piwa miodowe",          "url": f"{BASE}/kategoria/piwa-miodowe"},
    {"nazwa": "Sake",                  "url": f"{BASE}/kategoria/sake"},
    # OUTLET
    {"nazwa": "OUTLET Wyprzedaż",      "url": f"{BASE}/kategoria/outlet"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

session = requests.Session()
session.headers.update(HEADERS)


def _clean_txt(s):
    return re.sub(r"\s+", " ", s or "").strip()


def _clean_price(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"([0-9]+[.,][0-9]{2})", text.replace("\xa0", " "))
    return m.group(1).replace(",", ".") if m else ""


def _find_next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    # PrestaShop pagination: a[rel="next"] or li.next a
    a = soup.select_one('a[rel="next"]')
    if a and a.get("href"):
        return urljoin(current_url, a["href"])
    for sel in ["li.next a", "a.next", "a[aria-label*='Nast']"]:
        a = soup.select_one(sel)
        if a and a.get("href"):
            return urljoin(current_url, a["href"])
    return None


def parse_listing_page(html: str, page_url: str, kategoria_nazwa: str):
    soup = BeautifulSoup(html, "html.parser")
    wyniki = []

    for article in soup.select("article.product-miniature"):
        # Nazwa
        name_el = article.select_one("h2.product-title a")
        nazwa = _clean_txt(name_el.get_text()) if name_el else ""
        link = urljoin(page_url, name_el["href"]) if (name_el and name_el.get("href")) else ""

        # EAN z .product-reference
        ref_el = article.select_one(".product-reference a")
        if not ref_el:
            ref_el = article.select_one(".product-reference")
        ean_text = _clean_txt(ref_el.get_text()) if ref_el else ""
        ean = re.sub(r"\D", "", ean_text)
        if not (8 <= len(ean) <= 14):
            ean = ""

        # Cena z span.product-price (atrybut content lub tekst)
        price_el = article.select_one("span.product-price")
        if price_el and price_el.get("content"):
            cena_regularna = price_el["content"]
        elif price_el:
            cena_regularna = _clean_price(price_el.get_text())
        else:
            cena_regularna = ""

        # Cena promocyjna (jesli jest)
        price_old_el = article.select_one("span.regular-price")
        cena_promocyjna = ""
        if price_old_el:
            # Jesli jest stara cena, to regularna staje sie promo
            cena_stara = _clean_price(price_old_el.get_text())
            if cena_stara:
                cena_promocyjna = cena_regularna
                cena_regularna = cena_stara

        # Kategoria z kafelka
        cat_el = article.select_one(".product-category-name")
        kategoria_produktu = _clean_txt(cat_el.get_text()) if cat_el else kategoria_nazwa

        wyniki.append({
            "Nazwa": nazwa or "Brak",
            "Cena_regularna": cena_regularna or "Brak",
            "Cena_promocyjna": cena_promocyjna or "Brak",
            "EAN": ean or "Brak",
            "Link": link or page_url,
            "Kategoria": kategoria_produktu,
        })

    return wyniki, _find_next_page_url(soup, page_url)


def crawl_category(category_name: str, category_url: str, throttle: float = 0.5) -> list[dict]:
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

        records, next_url = parse_listing_page(r.text, url, category_name)
        wszystkie.extend(records)

        url = next_url
        page_idx += 1
        time.sleep(throttle)

    print(f"  OK: {category_name} ({len(wszystkie)} rekordow)")
    return wszystkie


def scrapuj_pitnemiody(output_file="OUTPUT_PITNEMIODY.csv"):
    open(output_file, "w").close()
    first_write = True
    total = 0

    for kat in KATEGORIE:
        cat_rows = crawl_category(kat["nazwa"], kat["url"], throttle=0.6)
        if not cat_rows:
            continue

        df = pd.DataFrame(cat_rows)
        df = df.drop_duplicates(subset=["Link"])

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
    scrapuj_pitnemiody()
