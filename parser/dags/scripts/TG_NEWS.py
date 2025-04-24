from telethon import TelegramClient
import telethon
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
import psycopg2
import psycopg2.extras
from tqdm import tqdm
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
        cur.execute("SELECT MAX(date) FROM news")
        responce = cur.fetchall()

# Возможность задать временные рамки
if responce[0][0] == None:
    from_date = (datetime.strptime('2024-08-31 23:00:00', '%Y-%m-%d %H:%M:%S') - timedelta(days=365 * 3 + 3)).replace(tzinfo=pytz.timezone('UTC')).replace(microsecond=0)
else:
    from_date = responce[0][0].replace(tzinfo=pytz.utc)

date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
path_to_save = f'news_{date}.csv' # Путь для сохранения csv 

channel_with_news = ["РБК. Новости. Главное"]

async def search_messages(client:TelegramClient, channel_with_news) -> pd.DataFrame:
    print("Парсинг начат")
    data = []
    dialogs = await client.get_dialogs()

    news_counter = tqdm(desc="Added news", unit="news", leave=True)
    for dialog in dialogs:
        if isinstance(dialog.entity, telethon.types.Channel) and dialog.entity.title in channel_with_news:
            async for message in client.iter_messages(dialog):
                if message.date > from_date and message.message is not None and message.message.strip():
                    async with lock:
                        data.append({
                            "channel":dialog.entity.title,
                            "date":message.date,
                            "text":message.message
                        })
                        news_counter.update(1)
                if message.date < from_date:
                    break
    news_counter.close()
    print(f"Закончил парсить. Всего добавлено новостей: {news_counter.n}")
    dataset = pd.DataFrame(data, columns=['channel', 'date', 'text'])
    print(dataset.head(5))
    return dataset

script_dir = Path(__file__).parent
session_path = script_dir / 'MTS_acc.session'

with TelegramClient(str(session_path), API_ID, API_HASH, system_version="4.16.30-vxCUSTOM") as client:
    client.loop.run_until_complete(client.send_message('@Fima_Herosimovich', f'Поиск начат'))
    dataset = client.loop.run_until_complete(search_messages(client, channel_with_news))
    client.loop.run_until_complete(client.send_message('@Fima_Herosimovich', f'Поиск закончен'))

dataset.drop_duplicates(inplace=True)
dataset.dropna(inplace=True)
# CSV
# dataset.to_csv(path_to_save, index=False)
# POSTGRESQL
with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    query = "INSERT INTO news (channel, date, text) VALUES %s"
    data = [row for row in dataset.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, data)
