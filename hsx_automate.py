import os
import zipfile
import logging
import pandas as pd
import gc
import argparse
import sys
import requests
import io
from datetime import datetime, timedelta

pd.options.mode.chained_assignment = None

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

link_char={'/':'%2F',' ':'%20','(':'%28',')':'%29'}

def get_t2_backward(date):
        if date.weekday() in [0,1]:
            return date - timedelta(days=4)
        return date - timedelta(days=2)

def replace_special_char(text:str, to_change:dict):
    for i, j in to_change.items():
        text = text.replace(i, j)
    return text

def get_file_path(entry_url):
    folder_path = '/I.6. TKGD NDTNN (Foreign trading)'
    today = datetime.now()
    t2 = get_t2_backward(today)
    file_name = f'/{t2.strftime("%Y%m%d")} - TKGD NDTNN (Foreign Trading).xls'

    if today.month != t2.month:
        folder_path += f'/{t2.month}.{t2.year}'

    file_path = folder_path + file_name
    _LOGGER.info('Internal file path: ' + file_path)
    call_path = replace_special_char(file_path, link_char)
    
    return entry_url + 'dl=' + call_path

def transform_df(file_name,stock_list):
    df = pd.read_excel(file_name,sheet_name='2',skiprows=9)
    df_dropna = df.dropna()
    df_sell = df_dropna.iloc[:,[1,10,11,12]]
    df_sell.columns = ['code', 'ato', 'cont', 'atc']
    df_sell['total_sell'] = df_sell.ato + df_sell.cont + df_sell.atc
    df_shortlist = df_sell[df_sell['code'].isin(stock_list)]
    df_final = df_shortlist.loc[:,['code','total_sell']]
    df_final.to_excel(file_name,index=False,header=False)
    _LOGGER.info("File cleaning succeeded")

def main():
    payload = {
    "login": 1,
    "ftp_user":"VNDIRECT011",
    "ftp_pass":"123"}

    stock_list = ['ACB','FPT','MBB','MWG','PNJ','REE','TCB','MSB','VIB','VPB','TPB']

    login_url='https://datafeed.hsx.vn/?'

    file_link = get_file_path(login_url)

    with requests.Session() as s:
        post = s.post(login_url, data=payload)
        if "forget your password or have problem with" in post.text:
            _LOGGER.error("Wrong credentials or another problem occurred")
            raise Exception("Program ended. Task failed!")
        _LOGGER.info("Successfully logged in!")
        resp = s.get(file_link)
        file_name = f'{get_t2_backward(datetime.now()).strftime("%Y%m%d")}-FT.xls'
        with open(file_name, 'wb') as output:
            output.write(resp.content)
        _LOGGER.info("Internal file download succeeded")
        s.close()

    transform_df(file_name,stock_list)
    _LOGGER.info("Task succeeded!")


if __name__ == '__main__':
    _LOGGER.info("Program started")
    main()
 