import ccxt
import re
import pandas as pd
import time
from multiprocessing import Pool, cpu_count
import numpy as np
import time
start_time = time.time()

start_time = time.time()
exchange = ccxt.binance()
exchange.load_markets()

def process_Pandas_data(func, df, num_processes=None):
    ''' Apply a function separately to each column in a dataframe, in parallel.'''
    print("process_data")
    # If num_processes is not specified, default to minimum(#columns, #machine-cores)
    if num_processes == None:
        num_processes = min(df.shape[1], cpu_count())

    # 'with' context manager takes care of pool.close() and pool.join() for us
    with Pool(num_processes) as pool:
        # we need a sequence to pass pool.map; this line creates a generator (lazy iterator) of columns
        seq = [df[col_name] for col_name in df.columns]

        # pool.map returns results as a list
        results_list = pool.map(func, seq)

        # return list of processed columns, concatenated together as a new dataframe
        return pd.concat(results_list, axis=1)


def tokenize_column(data_series):


    try:
        data = exchange.fetch_ohlcv(data_series.loc['symbol'], '1h')
        df = pd.DataFrame(data)
        if not df.empty:
            df[0] = pd.to_datetime(df[0], unit='ms')
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

            df["open"] = pd.to_numeric(df["open"])
            df["open"] = pd.to_numeric(df["open"])
            df["high"] = pd.to_numeric(df["high"])
            df["low"] = pd.to_numeric(df["low"])
            df["close"] = pd.to_numeric(df["close"])
            df["volume"] = round(pd.to_numeric(df["volume"]))

            df['pchange1h'] = df.close.diff(1).fillna(0)  # diff can has if for different timeperiods
            df['pchange1hpct'] = round((df['pchange1h'] / df["close"]) * 100, 2)

            df['pchange24h'] = df.close.diff(23).fillna(0)  # diff can has if for different timeperiods
            df['pchange24hpct'] = round((df['pchange24h'] / df["close"]) * 100, 2)

            df['v1h'] = df.volume.rolling(window=1).sum().fillna(0)  # .shift()

            df['vchange1h'] = df.v1h.diff(1).fillna(0)  # diff can has if for different timeperiods
            df['vchange1hpct'] = round((df['vchange1h'] / df["volume"]) * 100, 2)

            df['v4h'] = df.volume.rolling(window=4).sum().fillna(0)  # .shift()
            df['vchange4h'] = df.v4h.diff(4).fillna(0)  # diff can has if for different timeperiods
            df['vchange4hpct'] = round((df['vchange4h'] / df["volume"]) * 100, 2)

            df['v24'] = df.volume.rolling(window=23).sum().fillna(0)  # .shift()
            df['vchange24h'] = df.v24.diff(23).fillna(0)  # diff can has if for different timeperiods
            df['vchange24hpct'] = round((df['vchange24h'] / df["volume"]) * 100, 2)

            df['pricegain'] = (df.open.pct_change() * 100).fillna(0)


            data_series.loc['lastPrice'] = (list(df.close.tail(1)))[0]  # lastprice
            data_series.loc['priceChangePercent_1h'] = (list(df.pchange1hpct.tail(1)))[0]  # Pchange1H_pct
            data_series.loc['priceChangePercent_24h'] = (list(df.pchange24hpct.tail(1)))[0]  # Pchange24H_pct
            data_series.loc['volumeChange_1h'] = (list(df.vchange1h.tail(1)))[0]  # Vchange1H
            data_series.loc['volumeChangePercent_1h'] = (list(df.vchange1hpct.tail(1)))[0]  # vchange1hpct
            data_series.loc['volumeChange_4h'] = (list(df.vchange4h.tail(1)))[0]  # Vchange4H
            data_series.loc['volumeChangePercent_4h'] = (list(df.vchange4hpct.tail(1)))[0]  # Vchange4H
            data_series.loc['volumeChange_24h'] = (list(df.vchange24h.tail(1)))[0]  # vchange24h
            data_series.loc['volumeChangePercent_24h'] = (list(df.vchange24hpct.tail(1)))[0]  # Vchange24H_pct
            data_series.loc['volume_24h'] = (list(df.v24.tail(1)))[0]  # vchange24h
            data_series.loc['interval'] = '1h'
    except ccxt.base.errors.BadSymbol as err:
        print(data_series.loc['symbol'])

    return (data_series)


if __name__ == '__main__':
    df = pd.read_csv('VolumeGainers1.csv')
    print(df.transpose())
    df1 = process_Pandas_data(tokenize_column,df.transpose())
    print(df1.transpose())
    print("--- %s seconds ---" % (time.time() - start_time))