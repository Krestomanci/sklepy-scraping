import re
import json
import time
import asyncio
import aiohttp
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://darwina.pl"

KATEGORIE = [
    {"nazwa": "Wina",           "slug": "wina,c329"},
    {"nazwa": "Whisky",         "slug": "whisky,c322"},
    {"nazwa": "Alkohole mocne", "slug": "alkohole-mocne,c318"},
    {"nazwa": "Rum",            "slug": "rum,c321"},
    {"nazwa": "Gin",            "slug": "gin,c323"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE,
}

CONCURRENT = 30   # ile produktów pobieramy jednocześnie
MAX_STRON  = None # None = wszystkie strony, liczba = limit (np. 5 do testów)

# -------------- URL PAGINACJI --------------

def _build_url(slug, page):
    if page == 1:
        return f"{BASE}/{slug}.html"
    return f"{BASE}/{slug},s{page - 1}.html"

# -------------- LINKI Z LISTINGU (synchronicznie) --------------

def pobierz_linki(slug):
    session = requests.Session()
    session.headers.update(HEADERS)
    linki = []
    page  = 1

    while True:
        if MAX_STRON and page > MAX_STRON:
            break

        url = _build_url(slug, page)
        print(f"  📋 Listing strona {page}: {url}")

        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"  ❌ Błąd listingu: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # Linki produktów – mają wzorzec ,idNUMER.html
        a_tagi = [
            a for a in soup.select("a[href]")
            if re.search(r",id\d+\.html$", a.get("href", ""))
        ]

        if not a_tagi:
            print(f"  ✅ Koniec listingu na stronie {page}.")
            break

        for a in a_tagi:
            href = a["href"]
            link = href if href.startswith("http") else f"{BASE}/{href.lstrip('/')}"
            if link not in linki:
                linki.append(link)

        page += 1
        time.sleep(0.4)

    return linki

# -------------- DANE Z PODSTRONY PRODUKTU (asynchronicznie) --------------

async def pobierz_produkt(session, url, kategoria):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            html = await r.text()
    except Exception as e:
        print(f"  ❌ Błąd {url}: {e}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # --- Nazwa ---
    nazwa_el = soup.select_one("h1.product__name") or soup.select_one("h1")
    nazwa = nazwa_el.get_text(strip=True) if nazwa_el else "Brak"

    # --- EAN z meta itemprop="gtin13" ---
    # <meta itemprop="gtin13" content="8713475101944">
    ean = "Brak"
    meta = soup.find("meta", itemprop="gtin13")
    if meta and meta.get("content"):
        val = meta["content"].strip()
        if re.match(r"^\d{8,14}$", val):
            ean = val

    # --- Cena regularna (przekreślona) ---
    # <del class="price-box__prev-price">169.90 zł</del>
    cena_reg = "Brak"
    el = soup.select_one("del.price-box__prev-price") or soup.select_one(".price-box__prev-price")
    if el:
        m = re.search(r"[\d]+[.,][\d]{2}", el.get_text())
        cena_reg = m.group().replace(",", ".") if m else "Brak"

    # --- Cena aktualna / promocyjna ---
    # <span itemprop="price">158.50</span>
    cena_akt = "Brak"
    el2 = soup.select_one("span[itemprop='price']") or soup.select_one("meta[itemprop='price']")
    if el2:
        val = el2.get("content") or el2.get_text()
        m = re.search(r"[\d]+[.,][\d]{2}", str(val).replace(",", "."))
        cena_akt = m.group() if m else "Brak"

    # Logika cen:
    # - jest stara i nowa → stara=regularna, nowa=promocyjna
    # - jest tylko nowa   → nowa=regularna, brak promocji
    if cena_reg != "Brak":
        cena_promocyjna = cena_akt
    else:
        cena_reg        = cena_akt
        cena_promocyjna = "Brak"

    return {
        "Nazwa":           nazwa,
        "Cena_regularna":  cena_reg,
        "Cena_promocyjna": cena_promocyjna,
        "EAN":             ean,
        "Link":            url,
        "Kategoria":       kategoria,
    }

# -------------- RUNNER ASYNC --------------

async def pobierz_wszystkie(linki, kategoria):
    wyniki = []
    connector = aiohttp.TCPConnector(limit=CONCURRENT)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        # Dzielimy na paczki po CONCURRENT
        for i in range(0, len(linki), CONCURRENT):
            paczka = linki[i:i + CONCURRENT]
            taski  = [pobierz_produkt(session, url, kategoria) for url in paczka]
            wyniki_paczki = await asyncio.gather(*taski)
            for w in wyniki_paczki:
                if w:
                    wyniki.append(w)
            print(f"  ⚡ Pobrano {min(i + CONCURRENT, len(linki))}/{len(linki)} produktów")
            await asyncio.sleep(0.5)  # krótka pauza między paczkami
    return wyniki

# -------------- GŁÓWNA FUNKCJA --------------

def crawl_category(nazwa, slug):
    print(f"\n📂 Kategoria: {nazwa}")
    linki = pobierz_linki(slug)
    print(f"  🔗 Znaleziono {len(linki)} produktów – pobieranie async...")
    wyniki = asyncio.run(pobierz_wszystkie(linki, nazwa))
    print(f"  ✅ {nazwa}: {len(wyniki)} rekordów")
    return wyniki

def scrapuj_darwina(output_file="OUTPUT_darwina.csv"):
    open(output_file, "w").close()
    first = True

    for kat in KATEGORIE:
        rows = crawl_category(kat["nazwa"], kat["slug"])
        if not rows:
            continue
        df = pd.DataFrame(rows).drop_duplicates(subset=["Link"])
        df.to_csv(output_file, index=False, encoding="utf-8-sig",
                  mode="a", header=first)
        first = False
        print(f"  💾 Zapisano {len(df)} rekordów z kategorii: {kat['nazwa']}")

    print("\n🎉 Darwina.pl – scraping zakończony!")

if __name__ == "__main__":
    scrapuj_darwina()
