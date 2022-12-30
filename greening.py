from pandas_datareader import data  as pdr
from bs4 import BeautifulSoup
import pandas as pd
import requests
import matplotlib.pyplot as plt
import pandas as pd

import datetime
import json
import os
from operator import itemgetter
import unicodedata
import warnings
import yfinance as yf
yf.pdr_override()
import datetime as dt
warnings.filterwarnings("ignore")


def main():
    green = Green(src_dir=os.path.join(os.getcwd(), 'resources', 'market'))
    green.calculate()
    green.write_monitor_symbol_list(green.csv_dir)


class Green(object):
    def __init__(self, src_dir, is_full_version=False):
        # input files
        self.standard = json.load(open(os.path.join(src_dir, 'standard.json'), 'r'))
        if is_full_version:
            self.line = json.load(open(os.path.join(src_dir, 'line.json'), 'r'))

        # date values for calculation
        self.today = datetime.date.today()
        self.data_start_date = self.today - datetime.timedelta(days=365 * 5)
        self.img_start_date = self.today - datetime.timedelta(days=365)

        # output files
        self.csv_dir = self.__create_dirs(os.getcwd(), 'csv', self.today.strftime("%Y%m%d"))
        self.img_dir = self.__create_dirs(os.getcwd(), 'img', self.today.strftime("%Y%m%d"))

        # output variables
        self.all, self.mini = self.get_all_symbols()
        self.monitor_dict = {}

    def get_all_symbols(self, is_full_version=False):
        self.all = {}
        self.mini = set()

        # stock every market from table html
        __rich_len = 0
        for name, info in self.standard.items():
            self.all[name] = self.get_symbol_list_from_table_html(name, info)
            __rich_len += len(self.all[name])
            self.all[name] = self.appnend_postfix_jp(name, self.all[name])
            self.mini |= set([t[0] for t in self.all[name]])

        # stock every market from table html
        if is_full_version:
            for name, info in self.line.items():
                self.all[name] = self.get_symbol_list_from_raw_html(info)
                self.all[name] = self.appnend_postfix_jp(name, self.all[name])

        print("self.all include %d tickers" % __rich_len)
        print("self.mini include %d tickers" % len(self.mini))
        return self.all, self.mini

    def get_symbol_list_from_table_html(self, name, info):
        url, columns = itemgetter('url', 'columns')(info)

        tb_list = pd.read_html(url)
        # get table
        if name == 'topix':
            tb = pd.concat(tb_list[1:3])
        elif name == 'sp500':
            tb = tb_list[0]
        elif name == 'nasdaq100':
            tb = tb_list[4]
        else:
            tb = tb_list[1]

        tb = tb[columns].astype(str)
        return list(map(tuple, tb.to_numpy()))

    def get_symbol_list_from_raw_html(self, info):
        url, indices = itemgetter('url', 'indices')(info)

        r = requests.get(url)
        if r.status_code != 200:
            print("Error fetching page")
            exit()
        soup = BeautifulSoup(r.content, "html.parser")
        links = soup.find_all('a')[slice(*indices)]
        symbols_list = [tuple(unicodedata.normalize("NFKD", l.string).split(' / ')) for l in links]
        return symbols_list

    def appnend_postfix_jp(self, name, symbol_set_list):
        if name in ['jpx400', 'nikkei225', 'topix', 'line-a', 'line-b']:
            return [(t[0] + '.T', t[1]) for t in symbol_set_list]
        return symbol_set_list

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
            df = pdr.get_data_yahoo(ticker, start=self.data_start_date, end=self.today)

        except Exception as e:
            print("ticker: %s\nerror: %s" % (ticker, e))

        # day
        day = df
        day_means = self.__get_means(day['Close'], 246)
        self.__export_svg(ticker, 'day', day_means)
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
        week_means = self.__get_means(week['Close'], 52)
        self.__export_svg(ticker, 'week', week_means)
        self.__extract_trend(market, ticker, 'week', week_means)


        # month
        agg_dict['Adj Close'] = 'last'
        month = df.copy(deep=True).resample("MS").agg(agg_dict)
        month_means = self.__get_means(month['Close'], 12)
        self.__export_svg(ticker, 'month', month_means)
        self.__extract_trend(market, ticker, 'month', month_means)

        self.__stdout_progress(ticker)

    def __get_means(self, price, count):
        return [
                price.rolling(window=5).mean().tail(count),
                price.rolling(window=20).mean().tail(count),
                price.rolling(window=60).mean().tail(count),
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

    def __export_svg(self, ticker, period, means):
        plt.figure(figsize=(16,8))
        plt.plot(means[0].index, means[0], label='sma: 5', color='red')
        plt.plot(means[1].index, means[1], label='sma: 20', color='green')
        plt.plot(means[2].index, means[2], label='sma: 60', color='blue')
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
        self.mini.discard(ticker)
        print('progress: ', len(self.mini))


if __name__ == "__main__":
    main()

