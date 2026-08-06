import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://www.forfiterexclusive.pl"
LISTING_URL = f"{BASE}/alkohole/"
OUTPUT_FILE = "OUTPUT_FORFITEREXCLUSIVE.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pl-PL,pl;q=0.9",
}

session = requests.Session()
session.headers.update(HEADERS)


def _fetch_ean(product_url: str) -> str:
    try:
        r = session.get(product_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        ds = soup.select_one("dl.data-sheet")
        if ds:
            dts = ds.select("dt")
            dds = ds.select("dd")
            for dt, dd in zip(dts, dds):
                if "identyfikator" in dt.get_text(strip=True).lower():
                    ean = re.sub(r"\D", "", dd.get_text(strip=True))
                    if 8 <= len(ean) <= 14:
                        return ean
        return ""
    except Exception:
        return ""


def fetch_page(page_num: int) -> tuple[list[dict], int]:
    """Pobiera strone produktow przez PrestaShop XHR JSON API."""
    url = f"{LISTING_URL}?from-xhr&resultsPerPage=100&page={page_num}"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  ! Blad strony {page_num}: {e}")
        return [], 0

    products = data.get("products", [])
    total = data.get("pagination", {}).get("total_items", 0)

    wyniki = []
    for p in products:
        wyniki.append({
            "Nazwa": p.get("name", "Brak"),
            "Cena": str(p.get("price_amount", "")) or "Brak",
            "Link": p.get("link", "") or p.get("url", ""),
            "Kategoria": p.get("category_name", ""),
            "Producent": p.get("manufacturer_name", ""),
        })

    return wyniki, total


def scrapuj_forfiterexclusive():
    open(OUTPUT_FILE, "w").close()
    wszystkie = []
    page = 1

    # Pierwsza strona — poznaj total
    records, total = fetch_page(1)
    wszystkie.extend(records)
    print(f"  Strona 1: {len(records)} produktow (total w sklepie: {total})")

    # Reszta stron
    total_pages = (total + 99) // 100
    for page_num in range(2, total_pages + 1):
        records, _ = fetch_page(page_num)
        if not records:
            break
        wszystkie.extend(records)
        print(f"  Strona {page_num}: {len(records)} produktow (total: {len(wszystkie)})")
        time.sleep(0.5)

    # Deduplikacja
    seen = set()
    unique = []
    for r in wszystkie:
        if r["Link"] not in seen:
            seen.add(r["Link"])
            unique.append(r)
    wszystkie = unique

    print(f"\n  Pobieranie EAN z podstron ({len(wszystkie)} produktow)...")

    # Fetch EAN per product
    for i, rec in enumerate(wszystkie):
        if rec["Link"]:
            ean = _fetch_ean(rec["Link"])
            rec["EAN"] = ean or "Brak"
            time.sleep(0.3)
        else:
            rec["EAN"] = "Brak"

        if (i + 1) % 50 == 0:
            print(f"    EAN: {i+1}/{len(wszystkie)}")
            # Autozapis
            pd.DataFrame(wszystkie[:i+1]).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    # Final save
    if wszystkie:
        df = pd.DataFrame(wszystkie)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"\nGOTOWE: {len(wszystkie)} produktow -> {OUTPUT_FILE}")


if __name__ == "__main__":
    scrapuj_forfiterexclusive()
