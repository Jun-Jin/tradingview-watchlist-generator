#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ftplib

DELIMITER = '|'

def main():
    nasdaq_list = download_nasdaq_tickers_info_string().split("\n")[1:-2]
    nasdaq_symbols = extract_symbols_only(nasdaq_list)
    open('nasdaq_tickers.txt', 'w').write("\n".join(nasdaq_symbols))

def download_nasdaq_tickers_info_string(target = 'nasdaq'):
    # Download & assign to variable from ftp server.
    ftp = ftplib.FTP(host = 'ftp.nasdaqtrader.com', user = 'anonymous', passwd = 'anonymous@debian.org')
    ftp.cwd('/SymbolDirectory')
    byte_array = bytearray()
    ftp.retrbinary('RETR ' + "%slisted.txt" % target, byte_array.extend)
    ftp.quit()

    # youcan add others by pass the argumet as "other" or "all".

    # To String.
    return byte_array.decode()

def extract_symbols_only(tickers_list):
    return [l.split(DELIMITER)[0] for l in tickers_list]

if __name__ == '__main__':
    main()
