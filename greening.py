import matplotlib.pyplot as plt
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
    green.write_monitor_symbol_list(green.csv_dir)


class Green(object):
    def __init__(self, json_path):
        self.today = datetime.date.today()
        self.csv_dir = self.__create_dirs(os.getcwd(), 'csv', self.today.strftime("%Y%m%d"))
        self.img_dir = self.__create_dirs(os.getcwd(), 'img', self.today.strftime("%Y%m%d"))

        self.all = json.load(open(json_path, 'r', encoding='UTF-8'))
        self.mini = []
        for data in self.all.values():
            self.mini += [t[0] for t in data]

        self.monitor_dict = {}
        self.web_dict = {}

    def calculate(self):
        # bulk
        for market, data in self.all.items():
            self.monitor_dict[market] = {
                    'day_long': [],
                    'day_short': [],
                    'week_long': [],
                    'week_short': [],
                    'month_long': [],
                    'month_short': [],
                    }
            for ticker_set in data:
                self.__filter_ticker(market, ticker_set[0])

            self.monitor_dict[market] = self.__intersect(self.monitor_dict[market])

    def write_monitor_symbol_list(self, output_dir):
        for market, data in self.monitor_dict.items():
            market_dir = self.__create_dirs(output_dir, market)
            for name, tickers in data.items():
                if len(tickers) == 0: continue
                fpath = os.path.join(market_dir, "%s_list.txt" % name)
                if market in ['jpx400', 'nikkei225', 'topix', 'line-a', 'line-b']:
                    tickers = ['TSE:' + t.replace('.T','') for t in tickers]
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
        day_means = self.__get_means(day['Close'])
        self.__export_svg(ticker, 'day', day_means, day.index)
        self.__extract_trend(market, ticker, 'day', day_means)

        # week
        agg_dict = {
                "Open"  : "first",
                "High"  : "max",
                "Low"   : "min",
                "Close" : "last",
                "Volume": "sum",
                }
        week = df.copy(deep=True).resample("W", loffset=pd.Timedelta(days=-6)).agg(agg_dict)
        week_means = self.__get_means(week['Close'])
        self.__export_svg(ticker, 'week', week_means, week.index)
        self.__extract_trend(market, ticker, 'week', week_means)


        # month
        agg_dict['Adj Close'] = 'last'
        month = df.copy(deep=True).resample("MS").agg(agg_dict)
        month_means = self.__get_means(month['Close'])
        self.__export_svg(ticker, 'month', month_means, month.index)
        self.__extract_trend(market, ticker, 'month', month_means)

        self.__stdout_progress(ticker)

    def __get_means(self, price):
        return [
                price.rolling(window=5).mean(),
                price.rolling(window=20).mean(),
                price.rolling(window=60).mean(),
                ]

    def __extract_trend(self, market, ticker, period, means):

        # take last value to check long & short
        last_5 = float(means[0].tail(1))
        last_20 = float(means[1].tail(1))
        last_60 = float(means[2].tail(1))

        # long trend
        if last_5 > last_20 > last_60:
            self.monitor_dict[market]["%s_long" % period].append(ticker)
        # short trend
        elif last_5 < last_20 < last_60:
            self.monitor_dict[market]["%s_short" % period].append(ticker)

    def __export_svg(self, ticker, period, means, date):
        plt.figure(figsize=(16,8))
        plt.plot(date,means[0],label='sma: 5')
        plt.plot(date,means[1],label='sma: 20')
        plt.plot(date,means[2],label='sma: 60')
        plt.legend(loc='upper left')
        img_path = os.path.join(self.img_dir, "%s_%s.svg" % (ticker, period))
        plt.savefig(img_path)

    def __intersect(self, li):
        tmp = {}
        tmp['day_week_long'] = set(li['day_long']) & set(li['week_long'])
        tmp['day_month_long'] = set(li['day_long']) & set(li['month_long'])
        tmp['week_month_long'] = set(li['week_long']) & set(li['month_long'])
        tmp['day_week_month_long'] = tmp['day_week_long'] & set(li['month_long'])
        tmp['day_week_short'] = set(li['day_short']) & set(li['week_short'])
        tmp['day_month_short'] = set(li['day_short']) & set(li['month_short'])
        tmp['week_month_short'] = set(li['week_short']) & set(li['month_short'])
        tmp['day_week_month_short'] = tmp['day_week_short'] & set(li['month_short'])

        for name, value in tmp.items():
            if len(value) > 0:
                li[name] = list(value)
        return li

    def __stdout_progress(self, ticker):
        self.mini.remove(ticker)
        print('progress: ', len(self.mini))


if __name__ == "__main__":
    main(sys.argv[1])

