#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ftplib

DELIMITER = '|'

def main():
    getNasdaqTickerInfo()

def getNasdaqTickerInfo():
    # Download & assign to variable from ftp server.
    ftp = ftplib.FTP(host = 'ftp.nasdaqtrader.com', user = 'anonymous', passwd = 'anonymous@debian.org')
    ftp.cwd("/SymbolDirectory")
    byte_array = bytearray()
    ftp.retrbinary('RETR ' + "nasdaqlisted.txt", byte_array.extend)
    ftp.quit()

    # To String.
    lines = byte_array.decode().split("\n")[1:-2]
    # To list.
    tickers = [l.split(DELIMITER)[0] for l in lines]

    # Write to file.
    open("tickers.txt","w").write("\n".join(tickers))

if __name__ == '__main__':
    main()
