from pandas_datareader import data
import pandas as pd

import datetime
import json
import os
import warnings
warnings.filterwarnings("ignore")


TODAY = datetime.date.today()


def main():
    cwd = os.getcwd()
    create_dirs(cwd, 'output', TODAY.strftime("%Y%m%d"))

    raw_all_tickers = open(os.path.join(cwd, 'intermediate', 'all.json'), 'r', encoding='UTF-8')
    all_tickers = json.load(raw_all_tickers)

    # append '.T' to JP tickers
    for market in ['jpx400', 'nikkei225', 'topix', 'line-a', 'line-b']:
        all_tickers[market] = [(t[0] + '.T', t[1]) for t in all_tickers[market]]

    # bulk
    for market, data in all_tickers.items():
        monitor_lists = {
                'day_long': [],
                'day_short': [],
                'week_long': [],
                'week_short': [],
                'month_long': [],
                'month_short': [],
                }
        for ticker_set in data:
            filter_ticker(monitor_lists, ticker_set[0], TODAY)

        market_dir = os.path.join(cwd, 'output', TODAY.strftime("%Y%m%d"), market)
        os.makedirs(market_dir, exists=True)
        write_monitor_symbol_list(monitor_lists, market_dir)


def create_dirs(*paths):
    paths = os.path.join(*paths)
    os.makedirs(paths, exist_ok=True)


def filter_ticker(monitor_lists, ticker, end):
    try:
        # 株価取得（ティッカーシンボル、取得元、開始日、終了日）
        df = data.DataReader(ticker, 'yahoo', start='1980-01-01', end=end)
    except Exception as e:
        print("ticker: %s\nerror: %s" % (ticker, e))

    # day
    day = df
    organize_ticker(monitor_lists, ticker, day['Close'], 'day')

    # week
    agg_dict = {
            "Open"  : "first",
            "High"  : "max",
            "Low"   : "min",
            "Close" : "last",
            "Volume": "sum",
            }
    week = df.copy(deep=True).resample("W", loffset=pd.Timedelta(days=-6)).agg(agg_dict)
    organize_ticker(monitor_lists, ticker, week['Close'], 'week')

    # month
    agg_dict['Adj Close'] = 'last'
    month = df.copy(deep=True).resample("MS").agg(agg_dict)
    organize_ticker(monitor_lists, ticker, month['Close'], 'month')


def organize_ticker(monitor_lists, ticker, price, period):
    avg_5 = float(price.rolling(window=5).mean().tail(1))
    avg_20 = float(price.rolling(window=20).mean().tail(1))
    avg_60 = float(price.rolling(window=60).mean().tail(1))

    # ロングトレンド
    if avg_5 > avg_20 > avg_60:
        monitor_lists["%s_long" % period].append(ticker)
    # ショートトレンド
    elif avg_5 < avg_20 < avg_60:
        monitor_lists["%s_short" % period].append(ticker)


def write_monitor_symbol_list(monitor_lists, output_dir):
    for name, tickers in monitor_lists.items():
        if len(tickers) == 0: return
        fpath = os.path.join(output_dir, "%s_list.txt" % name)
        with open(fpath, mode='a') as f:
            f.write('\n'.join(tickers))


if __name__ == "__main__":
    main()

