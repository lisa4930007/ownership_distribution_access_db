#!/usr/bin/env python
# coding: utf-8

import configparser
import sys
import os
from datetime import datetime
import time
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.select import Select

# local func
from access import SqlConnection
import crawler

try:
    ini_file = 'config.ini'
    config = configparser.ConfigParser()
    if os.path.exists(ini_file):
        config.read(ini_file, encoding='utf-8') 
        access_path = config['DEFAULT']['accesspath']
    else:
        print('Config File Lost, Quit the crawler.')
    access_path = config['DEFAULT']['accesspath']

    sqlConn = SqlConnection(access_path)
    sql_selected_stocks = "SELECT stock_id FROM A_股票代號表"

    stock_id = input("請輸入欲加入觀察之股票代號:")
    stock_id = stock_id.strip()  # 去除前後空白
    ids = []
    for data in sqlConn.select_from_table_generator(sql_selected_stocks):
        ids.append(data[0])
    if stock_id in ids:
        raise Exception(f"資料庫已有此代號 {stock_id}")

    resp_url = 'https://www.tdcc.com.tw/portal/zh/smWeb/qryStock'
    crawler.chromedriver_autoinstall()
    option = webdriver.ChromeOptions()
    option.add_argument('--headless')
    option.add_argument('--log-level=3')
    option.add_argument('--disable-dev-shm-usage')
    option.add_argument('--disable-gpu')
    option.add_argument('blink-settings=imagesEnabled=false')
    driver = webdriver.Chrome(options=option)
    driver.get(resp_url)
    time.sleep(random.randint(3, 6))

    # 測試能否爬到網站資料
    elem_stockid = driver.find_element_by_id('StockNo')
    elem_stockid.clear()
    elem_stockid.send_keys(stock_id)
    time.sleep(random.randint(2, 4))
    search_input = driver.find_element_by_xpath("//input[@type='submit']")
    search_input.click()
    time.sleep(random.randint(2, 4))
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    bodydata = soup.find('table', 'table').tbody
    if '查無此資料' in bodydata.text:
        raise Exception(
            f'TDCC網站查無此股票代號 {stock_id}，可上網站確認: https://www.tdcc.com.tw/portal/zh/smWeb/qryStock')

    # 取得"證券名稱"
    stock_text = '證券名稱'
    rows = soup.find_all('p')
    for row in rows:
        if stock_text in row.text:
            stock_name = row.text.split('：')[2].strip()
    # print('證券名稱: ', stock_name)
except Exception as e:
    print('輸入異常: ', e)
    print('請再操作一次，謝謝!')
    sys.exit()

time.sleep(random.randint(2, 4))
driver.refresh()
selectA = driver.find_element_by_id('scaDate')
select_date = Select(selectA)
whole_dates = []
for i in select_date.options:
    whole_dates.append(str(i.text))
now_time = datetime.now().strftime('%Y%m%d%H%M%S')  # 匯入時間

try:
    for date in whole_dates:
        # print(date)
        time.sleep(random.randint(2, 4))
        driver.refresh()

        # 選擇日期/輸入股票代號/查詢
        selectA = driver.find_element_by_id('scaDate')
        select_date = Select(selectA)
        select_date.select_by_value(date)
        time.sleep(random.randint(2, 4))
        elem_stockid = driver.find_element_by_id('StockNo')
        elem_stockid.clear()
        elem_stockid.send_keys(stock_id)
        time.sleep(random.randint(2, 4))
        search_input = driver.find_element_by_xpath("//input[@type='submit']")
        search_input.click()
        time.sleep(random.randint(2, 4))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        rows = soup.find('table', 'table').tbody.find_all('tr')
        ratios, total = [], []
        # normal format [ex: 20210911, 1258]
        if len(rows) == 16:
            for i, row in enumerate(rows):
                all_tds = row.find_all('td')
                if i == 15:
                    total = all_tds[3].text.replace(',', '')
                    break
                ratios.append(float(all_tds[4].text))
        # special format [ex: 20210709, 9958]
        elif len(rows) == 17:
            for i, row in enumerate(rows):
                all_tds = row.find_all('td')
                if i == 15:
                    continue
                elif i == 16:
                    total = all_tds[3].text.replace(',', '')
                    break
                ratios.append(float(all_tds[4].text))

        if len(ratios) != 15 or total == []:
            raise Exception('網頁格式更動，需重新檢查網站html!')

        # 存取收盤價
        closing_price = crawler.get_closing_price(stock_id, date)
        # print('closing_price: ', closing_price)

        save_data = [stock_id, date]
        save_data.extend(ratios)
        save_data.append(total)
        save_data.append(closing_price)
        save_data.append(now_time)
        # print('save_data: ', save_data)

        B_columns = [row.column_name for row in sqlConn.get_all_columns_from_table(
            'B_股權分散表') if row.column_name != 'id']
        sqlConn.save_to_table(table_name='B_股權分散表',
                              columns=B_columns, save_data=save_data)
except Exception as e:
    # 如果遇到 error 則刪除此次的匯入
    conditions = f"created_time = '{now_time}'"
    sqlConn.delete_rows(table_name='B_股權分散表', conditions=conditions)
    print(f'程式異常: [{stock_id}] ', e)
    print('請回報錯誤，謝謝!')
    sys.exit()
else:
    print('所有一年內資料 匯入 B_股權分散表 成功!')
    driver.quit()

# 全部匯入成功後，將股票代號加入至觀察中清單「A_股票代號表」
A_columns = [
    row.column_name for row in sqlConn.get_all_columns_from_table('A_股票代號表')]
A_data = [stock_id, stock_name, now_time]
sqlConn.save_to_table(table_name='A_股票代號表',
                      columns=A_columns, save_data=A_data)
sqlConn.close_cursor()
sqlConn.close_conn()
print(f'{stock_id} 匯入 A_股票代號表 成功!')
print('程式已匯入完成!')
