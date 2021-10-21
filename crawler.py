#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta
import time
import json
import re
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import chromedriver_autoinstaller


def dateROC(date):
    return f'{date.year-1911}{date:/%m/%d}'


def dateW4Slash(date):
    return f'{date:%Y/%m/%d}'


def adjust_to_friday(date):
    weekday_index = date.weekday()
    # index of Friday: 4
    if weekday_index == 4:
        pass
    else:
        date += timedelta(days=4-weekday_index)
    print(f'{date:%Y/%m/%d} is Friday.')
    return date


def chromedriver_autoinstall():
    chromedriver_autoinstaller.install()


def get_web_content(resp_url):
    print(resp_url)

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('blink-settings=imagesEnabled=false')
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(resp_url)
    time.sleep(random.randint(2, 4))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    return soup


def get_TWSE_content(stock_id, target_date, resp_url):  # log 資訊 stock_id
    closing_price = None
    reg = re.compile('<[^>]*>')
    match_date_ROC = dateROC(target_date)

    resp = get_web_content(resp_url)
    # print ('resp:', resp)
    resp = json.loads(reg.sub('', str(resp)))
    if resp['stat'] == 'OK':
        for data in resp['data'][::-1]:
            if data[0] == match_date_ROC:
                if '--' in data[6]:
                    break
                closing_price = data[6]
                if ',' in closing_price:
                    closing_price = closing_price.replace(',', '')
                break
    if closing_price is None:
        print('Cannot find data from TWSE!')

    return closing_price


def get_cnyes_content_from_api(resp_url):
    closing_price = None
    reg = re.compile('<[^>]*>')

    resp = get_web_content(resp_url)
    resp = json.loads(reg.sub('', str(resp)))
    print(resp)
    if resp['statusCode'] == 200 and resp['data']['c'] != []:
        closing_price = resp['data']['c'][0]
        print(closing_price)
    if closing_price is None:
        print('Cannot find data from TWSE!')

    return closing_price


def get_cnyes_content(stock_id, target_date, resp_url):  # log 資訊 stock_id
    closing_price = None
    match_date_slash = dateW4Slash(target_date)

    resp = get_web_content(resp_url)
    table = resp.find('table')
    tr_data = table.find_all('tr')
    for n in range(1, len(tr_data)):
        find_date = tr_data[n].find('td', 'cr').text
        if find_date == match_date_slash:
            closing_price = tr_data[n].find_all('td', 'rt')[3].text
            break
    if closing_price is None:
        print('Cannot find data from TWSE!')

    return closing_price


def get_closing_price(stock_id, target_date):
    closing_price = -1  # 無資料(沒收盤價)則以-1表示
    target_datetime = datetime.strptime(target_date, '%Y%m%d')
    target_datetime = adjust_to_friday(target_datetime)

    # Source 1: TWSE 臺灣證券交易所
    # EX: https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20210304&stockNo=0050
    TWSE_URL = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json'
    resp_TWSE_url = f'{TWSE_URL}&date={target_date}&stockNo={stock_id}'
    resp_TWSE = get_TWSE_content(stock_id, target_datetime, resp_TWSE_url)
    if resp_TWSE is not None:
        print('resp_TWSE: ', resp_TWSE)
        return resp_TWSE

    # Source 2: anue 鉅亨 from api
    tmp = target_datetime.replace(hour=8, minute=0)  # ex: 20210831 08:00:00
    timestamp = int(datetime.timestamp(tmp))
    resp_cnyes_api_url = f'https://ws.api.cnyes.com/ws/api/v1/charting/history?resolution=D&symbol=TWS:{stock_id}:STOCK&from={timestamp}&to={timestamp}&quote=1'
    resp_cnyes_api = get_cnyes_content_from_api(resp_cnyes_api_url)
    if resp_cnyes_api is not None:
        print('resp_cnyes_api: ', resp_cnyes_api)
        return resp_cnyes_api

    # Source 3: anue 鉅亨 舊版
    resp_cnyes_url = f'https://www.cnyes.com/archive/twstock/ps_historyprice/{stock_id}.htm'
    resp_cnyes = get_cnyes_content(stock_id, target_datetime, resp_cnyes_url)

    if resp_cnyes is not None:
        print('resp_cnyes: ', resp_cnyes)
        return resp_cnyes
    else:
        print('No price: ', closing_price)
        return closing_price


def main():
    pass


if __name__ == '__main__':
    main()
