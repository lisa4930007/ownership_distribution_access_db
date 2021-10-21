#!/usr/bin/env python
# coding: utf-8

from urllib.request import urlretrieve
from datetime import datetime
import os
from os.path import join
import pandas as pd
import configparser

# local func
from access import SqlConnection

ini_file = 'config.ini'
config = configparser.ConfigParser()
if os.path.exists(ini_file):
    config.read(ini_file, encoding='utf-8') 
    access_path = config['DEFAULT']['accesspath']
else:
    print('Config File Lost, Quit the crawler.')

filename = 'TDCC_OD_1-5.csv'
os.makedirs('csv/', exist_ok=True)  # 建立目錄存放檔案
url = 'https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5'
urlretrieve(url, join('csv', filename))

try:
    df = pd.read_csv(join('csv', filename))
except FileNotFoundError:
    print('Cannot find ', filename)
    raise

csv_date = df[:1]['資料日期'].apply(str).values[0]
date_filename = csv_date + '.csv'

if not os.path.exists(join('csv', date_filename)):
    os.rename(join('csv', filename), join('csv', date_filename))  # 強制覆蓋已有的檔案
    print('股權分散 ' + date_filename + ' downloaded.')

    # save to table (D_股權分散CSV)
    downloaded_time = datetime.now().strftime('%Y%m%d%H%M%S')
    try:
        sqlConn = SqlConnection(access_path)
        sqlConn.save_to_table(table_name='D_股權分散CSV',
                              columns=['filename', 'downloaded_time'],
                              save_data=[date_filename, downloaded_time])
    except Exception as e:
        os.remove(join('csv', date_filename))
        print(e)
        raise
else:
    os.remove(join('csv', filename))
    print('股權分散 csv has been downloaded for this week!')
