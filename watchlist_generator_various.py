#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from operator import itemgetter
import pandas as pd
import requests
from bs4 import BeautifulSoup


# the market informations
MARKETS = {
    'nikkei255': {
        'region' : 'jp',
        'url': 'https://site1.sbisec.co.jp/ETGate/?OutSide=on&_ControlID=WPLETmgR001Control&_PageID=WPLETmgR001Mdtl20&_DataStoreID=DSWPLETmgR001Control&_ActionID=DefaultAID&getFlg=on&burl=search_market&cat1=market&cat2=none&dir=info&file=market_meigara_225.html',
        'column': 0,
    },
    'nikkei400': {
        'region': 'jp',
        'url': 'https://www.sbisec.co.jp/ETGate/?OutSide=on&_ControlID=WPLETmgR001Control&_PageID=WPLETmgR001Mdtl20&_DataStoreID=DSWPLETmgR001Control&_ActionID=DefaultAID&getFlg=on&burl=search_market&cat1=market&cat2=none&dir=info&file=market_meigara_400.html', 
        'column': 0,
    },
    'topix': {
        'region': 'jp',
        'url':    'https://search.sbisec.co.jp/v2/popwin/info/stock/pop690_topix100.html',
        'column': 0,
    },
    'sp500': {
        'region': 'na',
        'url': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        'column': 0,
    },
    'nasdaq100':{
        'region': 'na',
        'url': 'https://en.wikipedia.org/wiki/Nasdaq-100',
        'column': 1,
    },
    'dow':  {    
        'region': 'na',
        'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
        'column': 2,
    },
}

MARKETS_LINE = {
    'line-a': {
        'region': 'jp',
        'url': 'https://line-sec.co.jp/contents/stock-1005-A.html',
        'indices': slice(2, 585),
    },
    'line-b': {
        'region': 'jp',
        'url': 'https://line-sec.co.jp/contents/stock-1005-B.html',
        'indices': slice(2, 955),
    },
}


def main():
    # create 'output' dir if not exists
    output_dir = os.path.join(os.getcwd(), 'output')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # loop for every market from table html
    for name, info in MARKETS.items():
        li = get_symbol_list_from_table_html(name, info)
        write_to_file(output_dir, name, li)

    # loop for every market from raw html
    for name, info in MARKETS_LINE.items():
        li = get_symbol_list_from_raw_html(info)
        write_to_file(output_dir, name, li)
        

def get_symbol_list_from_table_html(name, info):
    """
    Get ticker symbol tables from url, and converting it into list type object.
    Args:
        name: str  : Name of market.
        info: dict : Informations of specific market. Include region, url, column.
    Returns:
        A list of ticker symbols
    """

    region, url, column = itemgetter('region', 'url', 'column')(info)
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
    symbol_list = list(tb.iloc[:, column].apply(str))
    return _append_postfix_to_symbol(symbol_list, region)


def get_symbol_list_from_raw_html(info):
    """
    Get ticker symbol tables from url(raw html), and scraping & convert it into list type object.
    Args:
        name: str  : Name of market.
        info: dict : Informations of specific market. Include region, url, indices.
    Returns:
        A list of ticker symbols
    """

    region, url, indices = itemgetter('region', 'url', 'indices')(info)
    r = requests.get(url)
    if r.status_code != 200:
        print("Error fetching page")
        exit()
    soup = BeautifulSoup(r.content, "html.parser")
    links = soup.find_all('a')

    symbol_list = [l.string.split(' ')[0] for l in links[indices]]
    return _append_postfix_to_symbol(symbol_list, region)


def write_to_file(output_dir, market_name, symbol_list, seperator='\n'):
    """
    Write out txt file from ticker symbol list
    Args:
        output_dir:  str       : path of output directory
        market_name: str       : name of market
        symbol_list: list<str> : the ticker symbol list
        seperator:   str       : seperator when list object is converted to string for file writing(default = '\n')
    Returns:
        None
    """

    fname = os.path.join(os.path.join(output_dir, "%s.txt" % market_name))
    open(fname, 'w').write(seperator.join(symbol_list))

def _append_postfix_to_symbol(symbol_list, region, postfix = '.T'):
    if region == 'jp':
        symbol_list = [ i + postfix for i in symbol_list]
    return symbol_list

if __name__ == '__main__':
    main()

