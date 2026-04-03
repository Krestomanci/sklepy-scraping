import os
import glob
import pandas as pd
import smtplib
from email.message import EmailMessage
from datetime import date

GMAIL_USER = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
RECIPIENT = os.environ["RECIPIENT_EMAIL"]


# Połącz dwie części dobreflaszki w jeden plik
czesc1 = "OUTPUT_DOBREFLASZKI_czesc1.csv"
czesc2 = "OUTPUT_DOBREFLASZKI_czesc2.csv"
merged = "OUTPUT_DOBREFLASZKI.csv"
if os.path.exists(czesc1) or os.path.exists(czesc2):
    parts = [pd.read_csv(f, encoding="utf-8-sig") for f in [czesc1, czesc2] if os.path.exists(f)]
    if parts:
        pd.concat(parts, ignore_index=True).to_csv(merged, index=False, encoding="utf-8-sig")
        print(f"✅ Połączono dobreflaszki → {merged}")
    # Usuń pliki częściowe
    for f in [czesc1, czesc2]:
        if os.path.exists(f):
            os.remove(f)

csv_files = glob.glob("*.csv")  # zbiera wszystkie CSV-y z katalogu głównego

msg = EmailMessage()
msg["Subject"] = f"📦 Wyniki scrapingu – {date.today().strftime('%d.%m.%Y')}"
msg["From"] = GMAIL_USER
msg["To"] = RECIPIENT

if csv_files:
    body = f"Dzień dobry,\n\nW załączniku wyniki scrapingu z dnia {date.today().strftime('%d.%m.%Y')}.\n\nPliki:\n"
    body += "\n".join(f"  • {f}" for f in csv_files)
    body += "\n\n-- Automatyczny raport GitHub Actions"
else:
    body = "⚠️ Żaden scraper nie wygenerował pliku CSV. Sprawdź logi w GitHub Actions."

msg.set_content(body)

# Dołącz każdy CSV jako załącznik
for csv_path in csv_files:
    with open(csv_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=os.path.basename(csv_path)
        )

# Wyślij przez Gmail SMTP
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
    smtp.send_message(msg)
    print(f"✅ Mail wysłany do {RECIPIENT} z {len(csv_files)} plikami.")
