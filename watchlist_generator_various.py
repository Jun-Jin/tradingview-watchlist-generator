#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd


# the market informations
MARKETS = {
    'nikkei255': {
        'region' : 'jp',
        'url': 'https://site1.sbisec.co.jp/ETGate/?OutSide=on&_ControlID=WPLETmgR001Control&_PageID=WPLETmgR001Mdtl20&_DataStoreID=DSWPLETmgR001Control&_ActionID=DefaultAID&getFlg=on&burl=search_market&cat1=market&cat2=none&dir=info&file=market_meigara_225.html',
        'col': 0,
    },
    'nikkei400': {
        'region': 'jp',
        'url': 'https://www.sbisec.co.jp/ETGate/?OutSide=on&_ControlID=WPLETmgR001Control&_PageID=WPLETmgR001Mdtl20&_DataStoreID=DSWPLETmgR001Control&_ActionID=DefaultAID&getFlg=on&burl=search_market&cat1=market&cat2=none&dir=info&file=market_meigara_400.html', 
        'col': 0,
    },
    'topix': {
        'region': 'jp',
        'url':    'https://search.sbisec.co.jp/v2/popwin/info/stock/pop690_topix100.html',
        'col': 0,
    },
    'sp500': {
        'region': 'na',
        'url': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        'col': 0,
    },
    'nasdaq100':{
        'region': 'na',
        'url': 'https://en.wikipedia.org/wiki/Nasdaq-100',
        'col': 1,
    },
    'dow':  {    
        'region': 'na',
        'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
        'col': 2,
    },
}


def main():
    # create 'output' dir if not exists
    output_dir = os.path.join(os.getcwd(), 'output')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # loop for every market
    for name, info in MARKETS.items():
        li = get_symbol_list(name, info['url'], info['col'])
        if info['region'] == 'jp':
            li = [ i + '.T' for i in li]
        write_to_file(output_dir, name, li)


def get_symbol_list(name, url, column):
    """
    Get ticker symbol tables from url, and convert it into list type object.
    Args:
        name: str : name of market
        url:  str : URL of symbol list web page
    Returns:
        A list of ticker symbols
    """

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
    return list(tb.iloc[:, column].apply(str))


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


if __name__ == '__main__':
    main()

