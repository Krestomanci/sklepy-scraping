import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE_URL = "https://winodlakazdego.pl"

KATEGORIE = [
    {"nazwa": "Wino", "url": "/rodzaj/wino/", "max_stron": 112},
    {"nazwa": "Whisky", "url": "/rodzaj/whisky/", "max_stron": 16},
]

def pobierz_dane_z_listy(relative_url, max_pages):
    wyniki = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}{relative_url}page/{page}/"
        print(f"🔄 Przetwarzam stronę {page}: {url}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept-Language": "pl-PL,pl;q=0.9"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"❌ Błąd ładowania strony: {url}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            karty = soup.find_all("div", class_="member-info")

            for karta in karty:
                a_tag = karta.find("a")
                h4 = a_tag.find("h4") if a_tag else None
                nazwa = h4.get_text(strip=True) if h4 else "Brak"
                link = a_tag.get("href") if a_tag and a_tag.get("href") else "Brak"

                # Znajdź odpowiadającą sekcję cenową
                cena_div = karta.find_next("div", class_="card-price")
                cena_tag = cena_div.find("i", itemprop="price") if cena_div else None
                cena = cena_tag.get_text(strip=True).replace('\xa0', ' ') if cena_tag else "Brak"

                # Wrzucamy dane do listy
                wyniki.append({
                    "Nazwa": nazwa,
                    "Cena": cena,
                    "Link": link
                })

        except Exception as e:
            print(f"❌ Błąd: {e}")

        time.sleep(1)

    return wyniki


def scrapuj_winodlakazdego(output_file):
    wszystkie_wyniki = []

    for kategoria in KATEGORIE:
        print(f"\n📂 Przetwarzam kategorię: {kategoria['nazwa']}")
        dane = pobierz_dane_z_listy(kategoria["url"], kategoria["max_stron"])
        for rekord in dane:
            rekord["Kategoria"] = kategoria["nazwa"]
            wszystkie_wyniki.append(rekord)

    # 🔧 Finalna korekta: połącz co dwa wiersze w jeden
    poprawione_wiersze = []
    for i in range(0, len(wszystkie_wyniki) - 1, 2):
        gorny = wszystkie_wyniki[i]
        dolny = wszystkie_wyniki[i + 1]

        poprawiony = {
            "Nazwa": dolny["Nazwa"],
            "Cena": gorny["Cena"],
            "Link": dolny["Link"],
            "Kategoria": dolny["Kategoria"]
        }
        poprawione_wiersze.append(poprawiony)

    # Zapis do pliku
    df = pd.DataFrame(poprawione_wiersze)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n✅ Gotowe! Zapisano {len(poprawione_wiersze)} poprawnych rekordów do pliku: {output_file}")


# ▶️ Start
if __name__ == "__main__":
    scrapuj_winodlakazdego("OUTPUT_WINODLAKAZDEGO_NOWY.csv")
