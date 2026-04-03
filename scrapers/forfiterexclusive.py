import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE_URL = "https://www.forfiterexclusive.pl/alkohole/"
LICZBA_STRON = 192   # zostaw jak było – działa
OUTPUT_FILE = "output_forfiterexclusive.csv"

def pobierz_ean(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "pl-PL,pl;q=0.9"
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)  # było 10 → 20
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        ean_tag = soup.find("td", {"class": "col data", "data-th": "Identyfikator"})
        if ean_tag:
            return ean_tag.get_text(strip=True)
        return "Brak"
    except Exception as e:
        print(f"❌ Błąd pobierania EAN z {url}: {e}")
        return "Brak"

def pobierz_produkty_z_strony(nr_strony):
    url = BASE_URL + f"?p={nr_strony}"
    print(f"🔄 Przetwarzam stronę {nr_strony}: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "pl-PL,pl;q=0.9"
    }
    produkty = []
    try:
        r = requests.get(url, headers=headers, timeout=20)  # było 15 → 20
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for prod in soup.select("div.product-item-info.type1"):
            a_tag = prod.select_one("a.product-item-link")
            nazwa = a_tag.get_text(strip=True) if a_tag else "Brak"
            link = a_tag['href'] if a_tag else None
            cena_tag = prod.select_one(".price-box .price")
            cena = cena_tag.get_text(strip=True).replace('\xa0', ' ') if cena_tag else "Brak"
            produkty.append({
                "Nazwa": nazwa,
                "Cena": cena,
                "Link": link
            })
    except Exception as e:
        print(f"❌ Błąd pobierania strony {url}: {e}")
    time.sleep(1.5)  # było 1 → 1.5
    return produkty

def scrapuj_forfiterexclusive():
    wszystkie = []
    licznik = 0
    for strona in range(1, LICZBA_STRON + 1):
        produkty = pobierz_produkty_z_strony(strona)
        if not produkty:
            print(f"✅ Brak produktów na stronie {strona} – koniec.")
            break
        print(f"🔗 Znaleziono {len(produkty)} produktów na stronie {strona}")
        for prod in produkty:
            kod_ean = pobierz_ean(prod["Link"])
            prod["EAN"] = kod_ean
            wszystkie.append(prod)
            licznik += 1
            if licznik % 40 == 0:
                pd.DataFrame(wszystkie).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
                print(f"💾 [Autozapis] Zapisano {len(wszystkie)} rekordów do {OUTPUT_FILE}")
            time.sleep(1.0)  # było 0.5 → 1.0
        pd.DataFrame(wszystkie).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print("\n✅ Scraping zakończony! Wszystko zapisane do:", OUTPUT_FILE)

if __name__ == "__main__":
    scrapuj_forfiterexclusive()
