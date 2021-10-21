#!/usr/bin/env python
# coding: utf-8

import sys
import os
import configparser

# local func
from access import SqlConnection


def main():
    try:
        ini_file = 'config.ini'
        config = configparser.ConfigParser()
        if os.path.exists(ini_file):
            config.read(ini_file, encoding='utf-8')
            access_path = config['DEFAULT']['accesspath']
        else:
            print('Config File Lost, Quit the crawler.')
        sqlConn = SqlConnection(access_path)

        check_again = True
        while (check_again):
            print('如欲新增報表用區間序列，請依序輸入參數(區間名稱、區間序列)')
            interval_name = input('(1/3) 請輸入區間名稱:')
            interval_name = interval_name.strip()  # 去除前後空白
            # interval_name 名稱不得重複 db取值檢查
            for name in sqlConn.select_from_table_generator('SELECT interval_name FROM C_統計區間'):
                if interval_name == name[0]:
                    raise Exception('與其他名稱重複! 請再思考其他命名')

            intervals = input(
                '(2/3) 區間序列範例格式: 1,20,100,600,1000\n請輸入區間序列，以逗號分隔:')
            error = '請再確認是否如以下範例格式: 1,20,100,600,1000 或 1,10,50,600,1000'
            intervals = intervals.split(',')
            intervals = [int(i.strip()) for i in intervals]  # 去除前後空白
            if intervals[0] != 1 or intervals[-1] != 1000:  # min = 1, max = 1000
                raise Exception(error)
            # 檢查是否由小到大
            if all(x < y for x, y in zip(intervals, intervals[1:])) is False:
                raise Exception(error)
            intervals = [str(i) for i in intervals]
            intervals_str = ','.join(intervals)
            # intervals 不得重複 db取值檢查
            for interval in sqlConn.select_from_table_generator('SELECT intervals FROM C_統計區間'):
                if intervals_str == interval[0]:
                    raise Exception('此區間已登錄過!')

            recheck = input(
                f'(3/3) 請再次確認區間名稱及序列:{interval_name}  {intervals_str}\n正確請填 y: ')
            if recheck == 'y':
                check_again = False
    except Exception as e:
        print('輸入異常: ', e)
        print('請再操作一次，謝謝!')
        sys.exit()
    else:
        # 假使檢查皆無誤則存入Access!
        sqlConn.save_to_table(table_name='C_統計區間',
                              columns=['interval_name', 'intervals'],
                              save_data=[interval_name, intervals_str])
    finally:
        sqlConn.close_cursor()
        sqlConn.close_conn()
        print('Finished.')


if __name__ == '__main__':
    main()
