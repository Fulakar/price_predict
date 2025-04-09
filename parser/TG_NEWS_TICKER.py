from telethon import TelegramClient
import telethon
import pandas as pd
import asyncio
from datetime import datetime, timezone, timedelta
import pytz
from typing import List
from dotenv import load_dotenv
import os
import sys
import psycopg2
import psycopg2.extras
sys.path.append(sys.path[0][:-7])
load_dotenv()
lock = asyncio.Lock()

API_ID = os.environ.get('API_ID')
API_HASH = os.environ.get('API_HASH')
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM news_with_ticker")
        responce = cur.fetchall()

# Возможность задать временные рамки
if responce[0][0] == None:
    from_date = (datetime.strptime('2024-08-31 23:00:00', '%Y-%m-%d %H:%M:%S') - timedelta(days=365 * 3 + 3)).replace(tzinfo=pytz.timezone('UTC')).replace(microsecond=0)
else:
    from_date = responce[0][0].replace(tzinfo=pytz.utc)

date = str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
path_to_save = f'news_ticker_{date}.csv' # Путь для сохранения csv 

async def search_messages(client:TelegramClient, names:List[str], tickers:List[str]) -> pd.DataFrame:
    dataset = pd.DataFrame({'channel':[], 'ticker':[], 'date':[], 'text':[]})
    dialogs = await client.get_dialogs()

    for index in range(len(names)):
        for dialog in dialogs:
            if isinstance(dialog.entity, telethon.types.Channel):
                async for message in client.iter_messages(dialog, search=names[index], offset_date=from_date):
                    if message.message.strip() and message.date > from_date:
                        async with lock:
                            Channel = dialog.entity.title
                            Ticker = tickers[index]
                            Date = message.date
                            Text = message.message
                            dataset.loc[len(dataset.index)] = [Channel, Ticker, Date, Text]
                print(f'Закончил парсить {dialog.entity.title}')
        print(f'ЗАКОНЧИЛ ПАРСИТЬ ПО {names[index]}')

    print(dataset.loc[:5])
    return dataset


with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker, name FROM ticker_describe")
        responce = cur.fetchall()
ticker = [r[0] for r in responce]
name = [r[1] for r in responce]

with TelegramClient('MTS_acc', API_ID, API_HASH, system_version="4.16.30-vxCUSTOM") as client:
    client.loop.run_until_complete(client.send_message('@Fima_Herosimovich', f'Поиск начат по : {" ".join(name)}'))
    dataset = client.loop.run_until_complete(search_messages(client, name, ticker))
    client.loop.run_until_complete(client.send_message('@Fima_Herosimovich', f'Поиск закончен'))

dataset.drop_duplicates(inplace=True)
dataset.dropna(inplace=True)
# CSV
dataset.to_csv(path_to_save, index=False)
# POSTGRESQL
with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    query = "INSERT INTO news_with_ticker (channel, ticker, date, text) VALUES %s"
    data = [row for row in dataset.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, data)
