import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import sys

KATEGORIE = [
    {"nazwa": "Wino",          "url": "https://dobreflaszki.pl/pl/c/WINO/608",              "stron": 86},
    {"nazwa": "Whisky",        "url": "https://dobreflaszki.pl/pl/c/WHISKY/214",            "stron": 43},
    {"nazwa": "Rum",           "url": "https://dobreflaszki.pl/pl/c/RUM/215",               "stron": 18},
    {"nazwa": "Inne alkohole", "url": "https://dobreflaszki.pl/pl/c/INNE-ALKOHOLE/604",     "stron": 68},
    {"nazwa": "Bezalkoholowe", "url": "https://dobreflaszki.pl/napoje-bezalkoholowe/16",    "stron": 3},
]

# Podział na dwie części
CZESCI = {
    "1": KATEGORIE[:3],   # Wino, Whisky, Rum
    "2": KATEGORIE[3:],   # Inne alkohole, Bezalkoholowe
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

def pobierz_kod_produktu(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        meta = soup.find("meta", itemprop="sku")
        if meta and meta.get("content"):
            return meta.get("content").strip()
        div_code = soup.find("div", class_="row code")
        if div_code:
            span = div_code.find("span")
            if span and span.get_text(strip=True).isdigit():
                return span.get_text(strip=True)
        return "Brak"
    except Exception as e:
        print(f"❌ Błąd pobierania kodu produktu z {url}: {e}")
        return "Brak"

def pobierz_produkty_z_kategorii(base_url, max_pages):
    produkty = []
    for page in range(1, max_pages + 1):
        url = f"{base_url}/{page}" if page > 1 else base_url
        print(f"🔄 Strona {page}: {url}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            for prod in soup.select("div.product_view-extended"):
                a_name = prod.select_one("a.prodname")
                nazwa  = a_name.get_text(strip=True) if a_name else "Brak"
                link   = a_name.get("href") if a_name else None
                if link and not link.startswith("http"):
                    link = "https://dobreflaszki.pl" + link
                cena_tag = prod.select_one(".price-new") or prod.select_one(".price")
                cena     = cena_tag.get_text(strip=True) if cena_tag else "Brak"
                produkty.append({"Nazwa": nazwa, "Cena": cena, "Link": link})
        except Exception as e:
            print(f"❌ Błąd przy ładowaniu {url}: {e}")
        time.sleep(1.5)
    return produkty

def scrapuj_dobreflaszki(czesc, output_file):
    kategorie = CZESCI.get(czesc, KATEGORIE)
    wszystkie = []
    licznik   = 0

    for kat in kategorie:
        print(f"\n📂 Kategoria: {kat['nazwa']}")
        produkty = pobierz_produkty_z_kategorii(kat["url"], kat["stron"])
        print(f"🔗 Znaleziono {len(produkty)} produktów w kategorii {kat['nazwa']}")

        for prod in produkty:
            kod = pobierz_kod_produktu(prod["Link"])
            prod["Kod_produktu"] = kod
            prod["Kategoria"]    = kat["nazwa"]
            wszystkie.append(prod)
            licznik += 1

            if licznik % 40 == 0:
                pd.DataFrame(wszystkie).to_csv(output_file, index=False, encoding="utf-8-sig")
                print(f"💾 [Autozapis] Zapisano {len(wszystkie)} rekordów")

            time.sleep(1.0)

        pd.DataFrame(wszystkie).to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"💾 Zapisano kategorię: {kat['nazwa']}")

    print(f"\n✅ Część {czesc} zakończona! Zapisano {len(wszystkie)} rekordów do: {output_file}")

if __name__ == "__main__":
    # Pobierz numer części z argumentu: python dobreflaszki.py 1 lub 2
    czesc = sys.argv[1] if len(sys.argv) > 1 else "1"
    output_file = f"OUTPUT_DOBREFLASZKI_czesc{czesc}.csv"
    scrapuj_dobreflaszki(czesc, output_file)
