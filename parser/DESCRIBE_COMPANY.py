from bs4 import BeautifulSoup
import requests
import psycopg2
import psycopg2.extras
from tinkoff.invest import Client
from tinkoff.invest.schemas import InstrumentStatus 
from dotenv import load_dotenv
import time
import os
from tqdm import tqdm
from TINKOFF_FUNC import rename_name, rename_sector
load_dotenv()

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
TOKEN = os.environ.get('TOKEN')

# Получение всех акций
with Client(TOKEN) as client:
    resp = client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE)
# Фильтрация по инструментов
resp = [resp.instruments[index] for index in range(len(resp.instruments)) if resp.instruments[index].class_code == 'TQBR']
resp = [resp[index] for index in range(len(resp)) if resp[index].first_1day_candle_date.year < 2022 and resp[index].share_type == 1]
# Списки секторов, тикеров, названий
all_ticker = [resp[index].ticker for index in range(len(resp))]
all_sector = [resp[index].sector for index in range(len(resp))]
all_name = [resp[index].name for index in range(len(resp))]
headers = {
    "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0"
}
data = []
for index, ticker in tqdm(enumerate(all_ticker)):
    try:
        sector = rename_sector(all_sector[index])
        name = rename_name(all_name[index])
        describe = ""

        url = f"https://www.tbank.ru/invest/stocks/{ticker}/"
        response = requests.get(url, headers=headers)
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        for text in soup.find('div', class_="TruncateHTML__lineClamp_dx9Qy").find_all("p"):
            describe += text.text.encode('latin1').decode('utf-8')

        data.append((ticker, sector, name, describe))
        time.sleep(5)
    except Exception as e:
        print(e, ticker)
# Описание отсутсвует на сайте
data.append(("RENI", "Финансовый сектор", "Ренессанс Страхование", "Ренессанс Страхование» (ПАО «Группа Ренессанс Страхование») — крупная универсальная российская страховая компания. Относится к категории системообразующих российских страховых компаний. 15 Компания работает на рынке с 1997 года. Имеет лицензии на более чем 60 видов страхования, в том числе добровольное медицинское страхование, автомобильное страхование (каско и ОСАГО), страхование выезжающих за рубеж, страхование имущества и различных видов ответственности (в том числе, для владельцев опасных объектов и для туроператоров), а также страхование грузов и строительно-монтажных рисков."))


query = "INSERT INTO ticker_describe (ticker, sector, name, describe) VALUES %s"
with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, data)
