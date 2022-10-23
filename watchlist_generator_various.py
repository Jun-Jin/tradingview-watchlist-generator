#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
from operator import itemgetter

from bs4 import BeautifulSoup
import pandas as pd
import requests


def main():
    # read from json files
    src_dir = os.path.join(os.getcwd(), 'resources', 'market')

    standard = read_json(src_dir, 'standard.json')
    line = read_json(src_dir, 'line.json')

    # create 'output' dir if not exists
    output_dir = os.path.join(os.getcwd(), 'output')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # loop for every market from table html
    for name, info in standard.items():
        li = get_symbol_list_from_table_html(name, info)
        write_to_file(output_dir, name, li)

    # loop for every market from raw html
    for name, info in line.items():
        li = get_symbol_list_from_raw_html(info)
        write_to_file(output_dir, name, li)
        

def read_json(src_dir, fname):
    raw_json = open(os.path.join(src_dir, fname), 'r')
    return json.load(raw_json)


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

    symbol_list = [l.string.split(' ')[0] for l in links[slice(*indices)]]
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

