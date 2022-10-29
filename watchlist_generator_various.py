#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
from operator import itemgetter
import sys
import unicodedata

from bs4 import BeautifulSoup
import pandas as pd
import requests


def main(intermediate=False):
    # read from json files
    src_dir = os.path.join(os.getcwd(), 'resources', 'market')

    standard = read_json(src_dir, 'standard.json')
    line = read_json(src_dir, 'line.json')

    # create 'output' dir if not exists
    output_dir = os.path.join(os.getcwd(), 'output')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    rich_output = {}

    # stock every market from table html
    for name, info in standard.items():
        rich_output[name] = get_symbol_list_from_table_html(name, info)

    # stock every market from table html
    for name, info in line.items():
        rich_output[name] = get_symbol_list_from_raw_html(info)

    # write everything
    for market, data in rich_output.items():
        li = [i[0] for i in data]
        write_to_file(output_dir, market, li)

    if intermediate == True:
        fname = os.path.join(os.path.join(output_dir, "all.json"))
        open(fname, 'w').write(json.dumps(rich_output, ensure_ascii=False))
        

def read_json(src_dir, fname):
    raw_json = open(os.path.join(src_dir, fname), 'r')
    return json.load(raw_json)


def get_symbol_list_from_table_html(name, info):
    """
    Get ticker symbol tables from url, and converting it into list type object.
    Args:
        name: str  : Name of market.
        info: dict : Informations of specific market. Include region, url, columns.
    Returns:
        A list of ticker symbols
    """

    region, url, columns = itemgetter('region', 'url', 'columns')(info)

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
    if region == 'jp':
        tb.iloc[:, 0] = tb.iloc[:, 0].apply(lambda x: x + '.T')
    return list(map(tuple, tb.to_numpy()))


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
    links = soup.find_all('a')[slice(*indices)]
    symbols_list = [tuple(unicodedata.normalize("NFKD", l.string).split(' / ')) for l in links]
    return [tuple([s[0] + '.T', s[1]]) for s in symbols_list]


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
    if len(sys.argv)  > 1 and sys.argv[1] == '-i':
        main(True)
    else:
        main()

