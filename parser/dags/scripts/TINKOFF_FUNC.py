import pandas as pd
import numpy as np
from tinkoff.invest import Client
from tinkoff.invest.schemas import InstrumentStatus, HistoricCandle 
from typing import List

def get_figi_by_tickers(tickers: List[str], token: str) -> dict:
    figi_dict = {}
    with Client(token) as client:
        for ticker in tickers:
            try:
                instruments = client.instruments.find_instrument(query=ticker).instruments
                if instruments:
                    figi_dict[ticker] = instruments[0].figi
                else:
                    figi_dict[ticker] = None  # Если инструмент не найден
            except Exception as e:
                print(f"Ошибка при обработке тикера {ticker}: {e}")
                figi_dict[ticker] = None
    return figi_dict


def candles_to_df(candles: list[HistoricCandle], ticker:str) -> pd.DataFrame:
    n_col = 7
    df = pd.DataFrame(np.full((len(candles), n_col), None), columns=['open',
                                                               'low',
                                                               'high',
                                                               'close',
                                                               'volume',
                                                               'date',
                                                               'ticker'])
    
    for i in range(len(candles)):
        df.loc[i, :] = [
            candles[i].open.units + candles[i].open.nano / 1e9,
            candles[i].low.units + candles[i].low.nano / 1e9,
            candles[i].high.units + candles[i].high.nano / 1e9,
            candles[i].close.units + candles[i].close.nano / 1e9,
            candles[i].volume,
            candles[i].time,
            ticker,
        ]
    return df

def rename_sector(row:str):
    r = {'utilities':'Электроэнергетика',
         'telecom':'Телекоммуникации',
         'real_estate':'Недвижимость',
         'materials':'Сырьевая промышленность',
         'it':'Информационные технологии',
         'industrials':'Машиностроение и транспорт',
         'health_care':'Здравоохранение',
         'financial':'Финансовый сектор',
         'energy':'Энергетика',
         'consumer':'Потребительские товары и услуги'}
    for key, value in r.items():
        row = row.replace(key, value)
    return row

def rename_name(row:str):
    r = {
         'Магнитогорский металлургический комбинат': 'ММК',
         'Банк ВТБ':'ВТБ',
         'Группа Черкизово':'Черкизово',
         'АбрауДюрсо':'Абрау Дюрсо',
         'Трубная Металлургическая Компания':'ТМК',
         'Сбер Банк':'Сбербанк',
         'Норильский никель':'Норникель'
         }
    for key, value in r.items():
        row = row.replace(key, value)
    return row