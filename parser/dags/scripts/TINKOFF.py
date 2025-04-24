import pandas as pd
import numpy as np
import pytz
import datetime
from datetime import timedelta, datetime, timezone
from tinkoff.invest import CandleInterval, Client
import time
from tqdm import tqdm
from dotenv import load_dotenv
import os
import sys
import psycopg2
import psycopg2.extras
from TINKOFF_FUNC import candles_to_df, get_figi_by_tickers
from tinkoff.invest.exceptions import RequestError


sys.path.append(os.getcwd()[:-7])
load_dotenv()
TOKEN_TINKOFF_API = os.environ.get('TOKEN_TINKOFF_API')
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")


query = "SELECT ticker, MAX(date) FROM price_hour GROUP BY ticker"
with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        responce = cur.fetchall()


# Дата конца сбора свечей
# to_date = datetime.strptime('2024-08-31 23:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
to_date = datetime.now(timezone.utc).replace(microsecond=0)
# Дата начала сбора свечей
if responce[0][0] == None:
    from_date = (datetime.strptime('2024-08-31 23:00:00', '%Y-%m-%d %H:%M:%S') - timedelta(days=365 * 3 + 3)).replace(tzinfo=pytz.timezone('UTC')).replace(microsecond=0)
else:
    ticker_date = {row[0]:row[1].replace(tzinfo=pytz.timezone('UTC')) for row in responce}
# Интервал по которому будут браться свечи
interval = CandleInterval.CANDLE_INTERVAL_HOUR

# # Количество запросов в минуту ограничено, поэтому между запросом по разным figi делается пауза
time_sleep = 61

# Пусть сохранения csv
date = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
path_to_save = f"csv/hour_price_{date}.csv"

# Получение всех акций
if responce[0][0] == None:
    query = "SELECT ticker FROM ticker_describe"
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            responce = cur.fetchall()
# Получаем figi по тикеру
all_ticker_list = [t[0] for t in responce]
ticker_figi = get_figi_by_tickers(all_ticker_list, TOKEN_TINKOFF_API)


# Конечная таблица
history_price = pd.DataFrame({
        'open': [],
        'low': [],
        'high': [],
        'close':[] ,
        'volume': [],
        'date': [],
        'ticker':[]
    })
# Подключение по токену
with Client(TOKEN_TINKOFF_API) as client:
    # Обработка ограничения по количеству запросов через while и try
    while all_ticker_list:
        ticker = next(iter(all_ticker_list))
        candles = []
        # Логика для пустой бд
        if responce[0][0] == None:
            from_ = from_date
        else:
            from_ = ticker_date[ticker]
        # Обработка ограничения по количеству запросов через while и try
        try:
            resp = client.get_all_candles(
                instrument_id=ticker_figi[ticker],
                from_ = from_,
                to = to_date,
                interval = interval
            )
            for r in tqdm(resp, desc=f"{ticker}"):
                    candles.append(r)
            # candles_to_df возвращает DataFrame pandas для конкретного figi, который конкатенируется с таблицей
            # так как свечи находятся в неудобном формате
            history_price = pd.concat((history_price, candles_to_df(candles, ticker)), ignore_index=True)
            all_ticker_list.remove(ticker)
        except RequestError as e:
            print(f"RequestError при получении данных для {ticker}: {e}")
            time.sleep(time_sleep)
        except Exception as e:
            print(f"Неожиданная ошибка {ticker}: {e}")
            break

history_price.to_csv(path_to_save, index=False)

history_price[['open', 'low', 'high', 'close']] = history_price[['open', 'low', 'high', 'close']].astype(np.float32)
history_price['volume'] = history_price['volume'].astype(np.int32)
history_price['date'] = pd.to_datetime(history_price['date'])
history_price['ticker'] = history_price['ticker'].astype(str)
# CSV
# history_price.to_csv(path_to_save, index=False)
# POSTGRESQL
query = f"INSERT INTO price_hour (open, low, high, close, volume, date, ticker) VALUES %s"
with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    with conn.cursor() as cur:
        data = [row for row in history_price[["open", "low", "high", "close", "volume", "date", "ticker"]].itertuples(index=False, name=None)]
        psycopg2.extras.execute_values(cur, query, data)
    