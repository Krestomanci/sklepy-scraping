import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE_URL    = "https://www.forfiterexclusive.pl/alkohole/"
OUTPUT_FILE = "output_forfiterexclusive.csv"

def stwórz_sesje():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    })
    # Wejdź najpierw na stronę główną żeby dostać cookies
    try:
        print("🔐 Inicjalizacja sesji...")
        session.get("https://www.forfiterexclusive.pl/", timeout=20)
        time.sleep(2)
        session.headers.update({"Referer": "https://www.forfiterexclusive.pl/"})
        print("✅ Sesja gotowa")
    except Exception as e:
        print(f"⚠️ Błąd inicjalizacji sesji: {e}")
    return session

def pobierz_ean(session, url):
    if not url:
        return "Brak"
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        ean_tag = soup.find("td", {"class": "col data", "data-th": "Identyfikator"})
        if ean_tag:
            return ean_tag.get_text(strip=True)
        return "Brak"
    except Exception as e:
        print(f"❌ Błąd EAN z {url}: {e}")
        return "Brak"

def pobierz_produkty_z_strony(session, nr_strony):
    url = BASE_URL + f"?p={nr_strony}"
    print(f"🔄 Strona {nr_strony}: {url}")
    produkty = []
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for prod in soup.select("div.product-item-info.type1"):
            a_tag    = prod.select_one("a.product-item-link")
            nazwa    = a_tag.get_text(strip=True) if a_tag else "Brak"
            link     = a_tag["href"] if a_tag else None
            cena_tag = prod.select_one(".price-box .price")
            cena     = cena_tag.get_text(strip=True).replace("\xa0", " ") if cena_tag else "Brak"
            produkty.append({"Nazwa": nazwa, "Cena": cena, "Link": link})
    except Exception as e:
        print(f"❌ Błąd strony {url}: {e}")
    time.sleep(1.5)
    return produkty

def scrapuj_forfiterexclusive():
    session   = stwórz_sesje()
    wszystkie = []
    licznik   = 0
    strona    = 1

    while True:
        produkty = pobierz_produkty_z_strony(session, strona)

        if not produkty:
            print(f"✅ Brak produktów na stronie {strona} – koniec.")
            break

        print(f"🔗 Znaleziono {len(produkty)} produktów na stronie {strona}")

        for prod in produkty:
            kod_ean = pobierz_ean(session, prod["Link"])
            prod["EAN"] = kod_ean
            wszystkie.append(prod)
            licznik += 1

            if licznik % 40 == 0:
                pd.DataFrame(wszystkie).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
                print(f"💾 [Autozapis] Zapisano {len(wszystkie)} rekordów")

            time.sleep(1.0)

        pd.DataFrame(wszystkie).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        strona += 1

    print(f"\n✅ Scraping zakończony! Zapisano {len(wszystkie)} rekordów do: {OUTPUT_FILE}")

if __name__ == "__main__":
    scrapuj_forfiterexclusive()
