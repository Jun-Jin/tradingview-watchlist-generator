from pandas_datareader import data
import pandas as pd

import datetime
import json
import os
import sys
import warnings
warnings.filterwarnings("ignore")


def main(in_dir):
    green = Green(json_path=os.path.join(in_dir, 'all.json'))
    green.calculate()
    green.write_monitor_symbol_list(green.output_dir)


class Green(object):
    def __init__(self, json_path):
        self.today = datetime.date.today()
        self.output_dir = self.__create_dirs(os.getcwd(), 'output', self.today.strftime("%Y%m%d"))

        self.all = json.load(open(json_path, 'r', encoding='UTF-8'))
        self.mini = []
        for data in self.all.values():
            self.mini += [t[0] for t in data]

        self.monitor_lists = {}
        self.img_list = {}

    def calculate(self):
        # bulk
        for market, data in self.all.items():
            self.monitor_lists[market] = {
                    'day_long': [],
                    'day_short': [],
                    'week_long': [],
                    'week_short': [],
                    'month_long': [],
                    'month_short': [],
                    }
            for ticker_set in data:
                self.__filter_ticker(market, ticker_set[0])

            self.monitor_lists[market] = self.__intersect(self.monitor_lists[market])

    def write_monitor_symbol_list(self, output_dir):
        for market, data in self.monitor_lists.items():
            market_dir = self.__create_dirs(output_dir, market)
            for name, tickers in data.items():
                if len(tickers) == 0: continue
                fpath = os.path.join(market_dir, "%s_list.txt" % name)
                with open(fpath, mode='a') as f:
                    f.write('\n'.join(tickers))

    def __create_dirs(self, *paths):
        paths = os.path.join(*paths)
        os.makedirs(paths, exist_ok=True)
        return paths

    def __filter_ticker(self, market, ticker):
        try:
            # 株価取得（ティッカーシンボル、取得元、開始日、終了日）
            df = data.DataReader(ticker, 'yahoo', start='1980-01-01', end=self.today)

        except Exception as e:
            print("ticker: %s\nerror: %s" % (ticker, e))

        # day
        day = df
        day_flg = self.__organize_ticker(market, ticker, day['Close'], 'day')

        # week
        agg_dict = {
                "Open"  : "first",
                "High"  : "max",
                "Low"   : "min",
                "Close" : "last",
                "Volume": "sum",
                }
        week = df.copy(deep=True).resample("W", loffset=pd.Timedelta(days=-6)).agg(agg_dict)
        week_flg = self.__organize_ticker(market, ticker, week['Close'], 'week')

        # month
        agg_dict['Adj Close'] = 'last'
        month = df.copy(deep=True).resample("MS").agg(agg_dict)
        month_flg = self.__organize_ticker(market, ticker, month['Close'], 'month')
        self.mini.remove(ticker)
        print('progress: ', len(self.mini))
        return day_flg, week_flg, month_flg

    def __organize_ticker(self, market, ticker, price, period):
        avg_5 = float(price.rolling(window=5).mean().tail(1))
        avg_20 = float(price.rolling(window=20).mean().tail(1))
        avg_60 = float(price.rolling(window=60).mean().tail(1))

        # ロングトレンド
        if avg_5 > avg_20 > avg_60:
            self.monitor_lists[market]["%s_long" % period].append(ticker)
            return 'long'
        # ショートトレンド
        elif avg_5 < avg_20 < avg_60:
            self.monitor_lists[market]["%s_short" % period].append(ticker)
            return 'short'
        return None

    def __intersect(self, li):
        tmp = {}
        tmp['day_week_long'] = set(li['day_long']) & set(li['week_long'])
        tmp['day_month_long'] = set(li['day_long']) & set(li['month_long'])
        tmp['week_month_long'] = set(li['week_long']) & set(li['month_long'])
        tmp['day_week_month_long'] = set(li['day_long']) & set(li['week_long']) & set(li['month_long'])
        tmp['day_week_short'] = set(li['day_short']) & set(li['week_short'])
        tmp['day_month_short'] = set(li['day_short']) & set(li['month_short'])
        tmp['week_month_short'] = set(li['week_short']) & set(li['month_short'])
        tmp['day_week_month_short'] = set(li['day_short']) & set(li['week_short']) & set(li['month_short'])

        for name, value in tmp.items():
            if len(value) > 0:
                li[name] = list(value)
        return li


if __name__ == "__main__":
    main(sys.argv[1])

