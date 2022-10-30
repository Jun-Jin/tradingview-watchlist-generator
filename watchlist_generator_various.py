#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
from operator import itemgetter
import unicodedata

from bs4 import BeautifulSoup
import pandas as pd
import requests


def main(is_full_version=False):
    # read from json files
    src_dir = os.path.join(os.getcwd(), 'resources', 'market')

    # create 'output' dir if not exists
    output_dir = os.path.join(os.getcwd(), 'intermediate')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    rich_output = {}
    mini_output = []

    # stock every market from table html
    __rich_len = 0
    standard = read_json(src_dir, 'standard.json')
    for name, info in standard.items():
        rich_output[name] = get_symbol_list_from_table_html(name, info)
        __rich_len += len(rich_output[name])
        rich_output[name] = appnend_postfix_jp(name, rich_output[name])
        mini_output = list(set(mini_output + [t[0] for t in rich_output[name]]))

    if is_full_version:
        # stock every market from table html
        line = read_json(src_dir, 'line.json')
        for name, info in line.items():
            rich_output[name] = get_symbol_list_from_raw_html(info)
            rich_output[name] = appnend_postfix_jp(name, rich_output[name])

    # write everything
    for market, data in rich_output.items():
        symbol_list = [i[0] for i in data]
        write_to_file(output_dir, market, symbol_list)

    fname = os.path.join(output_dir, "all.json")
    open(fname, 'w').write(json.dumps(rich_output, ensure_ascii=False))
    print("intermediate/all.json include %d tickers" % __rich_len)
    fname = os.path.join(output_dir, "mini.json")
    open(fname, 'w').write(json.dumps(mini_output, ensure_ascii=False))
    print("intermediate/mini.json include %d tickers" % len(mini_output))
        

def read_json(src_dir, fname):
    raw_json = open(os.path.join(src_dir, fname), 'r')
    return json.load(raw_json)


def get_symbol_list_from_table_html(name, info):
    """
    Get ticker symbol tables from url, and converting it into list type object.
    Args:
        name: str  : Name of market.
        info: dict : Informations of specific market. Include url, columns.
    Returns:
        A list of ticker symbols
    """

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


def get_symbol_list_from_raw_html(info):
    """
    Get ticker symbol tables from url(raw html), and scraping & convert it into list type object.
    Args:
        name: str  : Name of market.
        info: dict : Informations of specific market. Include url, indices.
    Returns:
        A list of ticker symbols
    """

    url, indices = itemgetter('url', 'indices')(info)

    r = requests.get(url)
    if r.status_code != 200:
        print("Error fetching page")
        exit()
    soup = BeautifulSoup(r.content, "html.parser")
    links = soup.find_all('a')[slice(*indices)]
    symbols_list = [tuple(unicodedata.normalize("NFKD", l.string).split(' / ')) for l in links]
    return symbols_list


def appnend_postfix_jp(name, symbol_set_list):
    if name in ['jpx400', 'nikkei225', 'topix', 'line-a', 'line-b']:
        return [(t[0] + '.T', t[1]) for t in symbol_set_list]
    return symbol_set_list


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
    if len(sys.argv) > 1:
        if sys.argv[1] == '--full':
            main(is_full_version=True)
            exit()
    main()

