#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ftplib
import requests
import pandas as pd


def main():
    # Write TSE ticker symbol codes to txt file.
    tse_xls = download_tse_tickers_info()
    tse_df = pd.read_excel(tse_xls)
    tse_codes = tse_df['コード'].astype(str).tolist()
    write_to_file(
            fname = 'tse_symbols.txt',
            code_list = tse_codes,
            seperator = '\n')

    # Write NASDAQ ticker symbol codes to txt file.
    nasdaq_list = download_nasdaq_tickers_info_string().split('\n')[1:-2]
    nasdaq_codes = extract_codes_only(nasdaq_list)
    write_to_file(
            fname = 'nasdaq_symbols.txt',
            code_list = nasdaq_codes,
            seperator = '\n')


def download_nasdaq_tickers_info_string(target = 'nasdaq'):
    # Download & assign to variable from ftp server.
    # TODO: Also can add others by pass the argumet as "other" or "all".
    ftp = ftplib.FTP(
            host = 'ftp.nasdaqtrader.com',
            user = 'anonymous',
            passwd = 'anonymous@debian.org')
    ftp.cwd('/SymbolDirectory')
    byte_array = bytearray()
    ftp.retrbinary('RETR ' + "%slisted.txt" % target, byte_array.extend)
    ftp.quit()

    # To String.
    return byte_array.decode()


def extract_codes_only(tickers_list):
    return [l.split('|')[0] for l in tickers_list]


def download_tse_tickers_info():
    url = 'https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls'
    return requests.get(url).content


def write_to_file(fname, code_list, seperator=','):
    open(fname, 'w').write(seperator.join(code_list))


if __name__ == '__main__':
    main()
