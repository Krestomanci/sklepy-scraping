import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://www.sklepzwhisky.pl"

KATEGORIE = [
    {"nazwa": "Whisky",          "url": f"{BASE}/kategoria/whisky/"},
    {"nazwa": "Inne alkohole",   "url": f"{BASE}/kategoria/inne-alkohole/"},
    {"nazwa": "Wina",            "url": f"{BASE}/kategoria/wina/"},
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


def _find_next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    # WooCommerce: a.next
    a = soup.select_one("a.next.page-numbers")
    if a and a.get("href"):
        return a["href"]
    # Fallback
    for sel in ["a[rel='next']", "li.next a"]:
        a = soup.select_one(sel)
        if a and a.get("href"):
            return urljoin(current_url, a["href"])
    return None


def parse_listing_page(html: str, page_url: str, kategoria_nazwa: str):
    soup = BeautifulSoup(html, "html.parser")
    wyniki = []

    for item in soup.select("li.product"):
        # Nazwa — span.toshort + span.ext
        name_el = item.select_one(".woocommerce-loop-product__title a")
        toshort = item.select_one(".woocommerce-loop-product__title span.toshort")
        ext = item.select_one(".woocommerce-loop-product__title span.ext")
        nazwa = _clean_txt(toshort.get_text()) if toshort else ""
        if ext and ext.get_text(strip=True):
            nazwa += " " + _clean_txt(ext.get_text())
        nazwa = nazwa.strip()

        # Link
        link_el = item.select_one("a.woocommerce-LoopProduct-link")
        link = link_el.get("href", "") if link_el else ""
        if not link and name_el:
            link = name_el.get("href", "")

        # EAN z data-product_sku na przycisku "Dodaj do koszyka"
        sku_el = item.select_one("a[data-product_sku]")
        ean = sku_el.get("data-product_sku", "").strip() if sku_el else ""

        # Cena — sprawdz czy jest promo (del + ins)
        del_el = item.select_one("del .woocommerce-Price-amount bdi")
        ins_el = item.select_one("ins .woocommerce-Price-amount bdi")

        if del_el and ins_el:
            cena_regularna = _clean_price(del_el.get_text())
            cena_promocyjna = _clean_price(ins_el.get_text())
        else:
            price_el = item.select_one(".woocommerce-Price-amount bdi")
            cena_regularna = _clean_price(price_el.get_text()) if price_el else ""
            cena_promocyjna = ""

        # Kategorie z klas CSS (product_cat-*)
        classes = item.get("class", [])
        cats = [c.replace("product_cat-", "").replace("-", " ")
                for c in classes if c.startswith("product_cat-")]
        kategoria_produktu = ", ".join(cats) if cats else kategoria_nazwa

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


def scrapuj_sklepzwhisky(output_file="OUTPUT_SKLEPZWHISKY.csv"):
    open(output_file, "w").close()
    first_write = True
    total = 0
    seen_links = set()

    for kat in KATEGORIE:
        cat_rows = crawl_category(kat["nazwa"], kat["url"], throttle=0.6)
        if not cat_rows:
            continue

        df = pd.DataFrame(cat_rows)
        # Deduplikacja — produkty moga byc w wielu podkategoriach
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
    scrapuj_sklepzwhisky()
