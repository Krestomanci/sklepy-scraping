import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE_URL = "https://sklep-domwhisky.pl"

# Twoje podstrony z maksymalną liczbą stron
KATEGORIE = [
    {"nazwa": "Scotch Whisky", "url": "/pol_m_Scotch-Whisky-175.html", "max_stron": 87},
    {"nazwa": "World Whisky", "url": "/pol_m_World-Whisky-150.html", "max_stron": 39},
    {"nazwa": "Wina", "url": "/pol_m_Wina-2308.html", "max_stron": 17},
    {"nazwa": "Szampany", "url": "/pol_m_Szampany-2307.html", "max_stron": 5},
    {"nazwa": "Old & Rare", "url": "/pol_m_Old-Rare-1171.html", "max_stron": 30},
    {"nazwa": "Inne alkohole", "url": "/pol_m_Inne-alkohole-440.html", "max_stron": 105},
    {"nazwa": "Pozostałe", "url": "/pol_m_Pozostale-1267.html", "max_stron": 12},
]

def pobierz_linki_z_kategorii(relative_url, max_pages):
    wszystkie_linki = []
    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}{relative_url}?counter={page}"
        print(f"🔄 Przetwarzam stronę {page} → {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept-Language": "pl-PL,pl;q=0.9"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"❌ Błąd ładowania strony {url}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            produkty = soup.find_all("a", class_="product__name")

            if not produkty:
                print("✅ Brak produktów – koniec tej kategorii.")
                break

            for produkt in produkty:
                href = produkt.get("href")
                if href:
                    link = href if href.startswith("http") else BASE_URL + href
                    wszystkie_linki.append(link)

        except Exception as e:
            print(f"❌ Błąd: {e}")

        time.sleep(1)

    return wszystkie_linki

def pobierz_dane_produktu(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "pl-PL,pl;q=0.9"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        nazwa = soup.find("h1", class_="product_name__name")
        cena = soup.find("strong", id="projector_price_value")
        ean = soup.find("span", class_="dictionary__producer_code")

        return {
            "Nazwa": nazwa.get_text(strip=True) if nazwa else "Brak",
            "Cena": cena.get_text(strip=True).replace('\xa0', ' ') if cena else "Brak",
            "EAN": ean.get_text(strip=True) if ean else "Brak",
            "Link": url
        }

    except Exception as e:
        print(f"❌ Błąd przy produkcie {url}: {e}")
        return None

def scrapuj_wiele_kategorii(output_file, zapis_co=20):
    wyniki = []

    for kategoria in KATEGORIE:
        print(f"\n📂 Rozpoczynam kategorię: {kategoria['nazwa']}")
        linki = pobierz_linki_z_kategorii(kategoria["url"], kategoria["max_stron"])
        print(f"🔗 Znaleziono {len(linki)} produktów w kategorii: {kategoria['nazwa']}")

        for index, link in enumerate(linki, 1):
            print(f"🔍 [{index}/{len(linki)}] {link}")
            dane = pobierz_dane_produktu(link)
            if dane:
                dane["Kategoria"] = kategoria["nazwa"]
                wyniki.append(dane)

            # 🔄 Zapis co 'zapis_co' produktów
            if len(wyniki) % zapis_co == 0:
                df = pd.DataFrame(wyniki)
                df.to_csv(output_file, index=False, encoding="utf-8-sig")
                print(f"💾 [Autozapis] Zapisano {len(wyniki)} produktów do pliku.")

            time.sleep(0.5)

    # 🔚 Zapis końcowy
    df = pd.DataFrame(wyniki)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n✅ Gotowe! Zapisano wszystkie dane do pliku: {output_file}")

# ▶️ Start
if __name__ == "__main__":
    scrapuj_wiele_kategorii("OUTPUT_DOMWHISKY_WIELE.csv", zapis_co=20)
