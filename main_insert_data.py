#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import datetime
import sys
import os
from os.path import join, abspath
import shutil
import configparser

# local func
from access import SqlConnection
import crawler


def main():
    ini_file = 'config.ini'
    config = configparser.ConfigParser()
    if os.path.exists(ini_file):
        config.read(ini_file, encoding='utf-8')
        access_path = config['DEFAULT']['accesspath']
    else:
        print('Config File Lost, Quit the crawler.')
    access_path = config['DEFAULT']['accesspath']

    # 如果執行時間落在每月第一週，則備份當天資料庫
    backup_day = datetime.now()
    backup_day_str = backup_day.strftime('%Y%m%d')
    if 1 < backup_day.day < 8:
        try:
            print('執行時間落在每月第一週，備份當天資料庫至 db_backup')
            shutil.copy('access_stock_ratio.accdb', 
                        join('db_backup', f'access_stock_ratio_{backup_day_str}.accdb'))
        except IOError as e:
            print(f'Unable to copy file. {e}')
        except:
            print('Unexpected error:', sys.exc_info())

    crawler.chromedriver_autoinstall()

    sqlConn = SqlConnection(access_path)
    sql_unimported_files = "SELECT filename FROM D_股權分散CSV WHERE imported_time =''"

    sql_selected_stocks = 'SELECT stock_id FROM A_股票代號表'
    for file in sqlConn.select_from_table_generator(sql_unimported_files):
        filename = file[0]
        csv_fname = join(abspath(os.getcwd()), 'csv', filename)
        try:
            df = pd.read_csv(csv_fname)
            print('Read ', csv_fname)
        except FileNotFoundError:
            print('Cannot find ', csv_fname)
            raise

        csv_date = df[:1]['資料日期'].apply(str).values[0]
        now_time = datetime.now().strftime('%Y%m%d%H%M%S')  # 匯入時間

        try:
            for stock_info in sqlConn.select_from_table_generator(sql_selected_stocks):
                stock = stock_info[0]
                print(stock)

                closing_price = crawler.get_closing_price(stock, csv_date)
                print('closing_price: ', closing_price)

                df_ = df.loc[df['證券代號'] == stock]
                ratios = df_[:-2]['占集保庫存數比例%'].values
                total = str(df_[-1:]['股數'].values[0])

                save_data = [stock, csv_date]
                save_data.extend(ratios)
                save_data.append(total)
                save_data.append(closing_price)
                save_data.append(now_time)
                print('save_data: ', save_data)

                B_columns = [row.column_name for row in sqlConn.get_all_columns_from_table(
                    'B_股權分散表') if row.column_name != 'id']
                sqlConn.save_to_table(table_name='B_股權分散表',
                                      columns=B_columns, save_data=save_data)
        except Exception as e:
            # 如果遇到 error 則刪除此次的匯入
            conditions = f"created_time = '{now_time}'"
            sqlConn.delete_rows(table_name='B_股權分散表', conditions=conditions)
            print(f'程式異常: [{stock}] ', e)
            print('請回報錯誤，謝謝!')
            sys.exit()
        else:
            imported_time = datetime.now().strftime('%Y%m%d%H%M%S')
            conditions = f"filename = '{filename}'"
            sqlConn.update_table(table_name='D_股權分散CSV',
                                 columns=['imported_time'],
                                 conditions=conditions,
                                 save_data=[imported_time])

    sqlConn.close_cursor()
    sqlConn.close_conn()
    print('資料已匯入完成!')


if __name__ == '__main__':
    main()
