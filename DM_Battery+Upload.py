import requests
from datetime import datetime, timedelta
from telegram import Bot
import pytz
import pandas as pd
from bs4 import BeautifulSoup
import os
import urllib.parse
from io import BytesIO
from telegram.ext import Updater
import asyncio

TELEGRAM_BOT_TOKEN = '6845653655:AAGc__iu9HKR-jfQHNxQ8ukWuYHD-JNjoaE'
TELEGRAM_CHAT_ID = '-1002079278580'

async def check_remote_file_update(file_url, threshold_minutes=16):
    try:
        response = requests.head(file_url)
        last_modified_header = response.headers.get('Last-Modified')
        last_modified_time = datetime.strptime(last_modified_header, '%a, %d %b %Y %H:%M:%S %Z')
        last_modified_time = last_modified_time.replace(tzinfo=pytz.utc)
        last_modified_time = last_modified_time.astimezone(pytz.timezone('Australia/Sydney'))
        time_difference = datetime.now(pytz.timezone('Australia/Sydney')) - last_modified_time
        minutes_difference = time_difference.total_seconds() / 60

        return minutes_difference <= threshold_minutes
    except Exception as e:
        print(f"Errore durante il controllo dell'aggiornamento del file {file_url}: {e}")
        return False

async def scarica_e_unisci_csv(url, cartella_destinazione, file_excel_destinazione):
    if not os.path.exists(cartella_destinazione):
        os.makedirs(cartella_destinazione)

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=lambda href: (href and href.endswith('.csv')))
    df_dict = {}
    messaggi = []

    for link in links:
        file_csv_url = link['href']
        if 'UNKNOWN.csv' in file_csv_url:
            continue

        if not urllib.parse.urlparse(file_csv_url).scheme:
            file_csv_url = urllib.parse.urljoin(url, file_csv_url)

        file_csv_content = requests.get(file_csv_url).content
        df = pd.read_csv(BytesIO(file_csv_content))
        print(f"DataFrame per {file_csv_url}:\n{df}")

        nome_foglio = os.path.splitext(os.path.basename(file_csv_url))[0]
        df_dict[nome_foglio] = df

        try:
            ultimo_valore = df.iloc[-1, 2]
            print(f"Ultimo valore per {nome_foglio}: {ultimo_valore}")
            messaggio = f"Batteria {nome_foglio} {'carica' if ultimo_valore > 3700 else 'scarica'}, Ultimo valore: {ultimo_valore}"
            messaggi.append(messaggio)
        except IndexError:
            print(f"La colonna non è presente nel DataFrame.")

    file_excel_path = os.path.join(cartella_destinazione, file_excel_destinazione)
    with pd.ExcelWriter(file_excel_path, engine='xlsxwriter') as writer:
        for nome_foglio, df in df_dict.items():
            df.to_excel(writer, sheet_name=nome_foglio, index=False)

    await invia_notifica_telegram("\n".join(messaggi))

async def invia_notifica_telegram(messaggio):
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    chat_id = TELEGRAM_CHAT_ID

    try:
        updater.bot.send_message(chat_id=chat_id, text=messaggio)
    except Exception as e:
        print(f"Errore nell'invio del messaggio a Telegram: {e}")

async def main():
    csv_file_urls = [
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/C1.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/C2.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/C3.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/C4.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/C5.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/P1.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/P2.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/P3.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/P4.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/P5.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/S1.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/S2.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/S3.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/S4.csv',
        'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/S5.csv'
    ]

    tasks = [check_remote_file_update(file_url) for file_url in csv_file_urls]
    results = await asyncio.gather(*tasks)

    all_files_updated = all(results)

    if all_files_updated:
        print("Tutte le schede sono in funzione.")
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        message = "Tutte le schede sono in funzione:)"
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
    else:
        for file_url in csv_file_urls:
            if not await check_remote_file_update(file_url):
                print(f"Il file {file_url} non è stato aggiornato da più di 16 minuti. Invio messaggio Telegram.")
                bot = Bot(token=TELEGRAM_BOT_TOKEN)
                message = f"Attenzione! Il file {file_url} non è stato aggiornato da più di 16 minuti."
                bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)

    # Esempio di utilizzo
    url_sito = 'https://www.bosl.com.au/IoT/wsudwatch/FYP_SGI/'
    cartella_destinazione = r'C:\Users\cmare\Desktop'
    file_excel_destinazione = 'Download_separate_sheets.xlsx'

    await asyncio.gather(
        scarica_e_unisci_csv(url_sito, cartella_destinazione, file_excel_destinazione)
    )

if __name__ == "__main__":
    asyncio.run(main())
