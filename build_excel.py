#!/usr/bin/env python
# coding: utf-8

import os
import numpy as np
from os.path import join
import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
from matplotlib.font_manager import FontProperties
import seaborn as sns
from datetime import datetime
import configparser

# local func
from access import SqlConnection


class StockRatioDataframe:
    def __init__(self, stock_id, interval_name, interval_str, raw_df):
        self.stock_id = stock_id
        self.intervals = self.form_intervals(interval_str)
        self.df = self.form_df_for_excel(raw_df)

        self.df_date = self.df.loc[:, 'date_time']
        self.df_total_share = self.df.loc[:, 'total_shares']
        self.df_closing_price = self.df.loc[:, 'closing_price']

        self.img_title = self.form_img_title(interval_name)

    def form_intervals(self, interval_str):
        # ex: [1,20,100,600,'1000_above']
        intervals = interval_str.split(',')
        intervals[-1] = str(intervals[-1]) + '_above'
        return intervals

    def form_df_for_excel(self, raw_df):
        # 合併 ratio 資料
        df_ratios = raw_df.loc[:, 'ratio_1':'ratio_15']
        aggregated_ratios = []
        for ratios in df_ratios.values:
            aggregated_ratios.append(self.form_aggregated_data_with_range(ratios))

        columns = self.form_aggregate_intervals_columns()
        self.df_aggregated_ratios = pd.DataFrame(aggregated_ratios, columns=columns)

        # 組成新的 df 作為 excel 資料來源
        raw_df = raw_df.drop(raw_df.loc[:, 'ratio_1':'ratio_15'].columns, axis=1)
        df = pd.concat([raw_df.loc[:, 'date_time'],
                        self.df_aggregated_ratios,
                        raw_df.loc[:, 'total_shares':'closing_price']],
                       axis=1)
        return df

    def form_aggregated_data_with_range(self, ratios):
        interval_fixed = ['1', '5', '10', '15', '20', '30', '40', '50',
                          '100', '200', '400', '600', '800', '1000', '1000_above']
        data_mixed = dict(zip(interval_fixed, ratios))

        result_aggregated = []
        for key, value in data_mixed.items():
            if key == '1':
                tmp = value
            elif key in self.intervals:
                # print('Key matched at: ', key)
                tmp += value
                result_aggregated.append(tmp)
                tmp = 0
            else:
                tmp += value
        # print('result_aggregated: ', result_aggregated)
        return result_aggregated

    def form_aggregate_intervals_columns(self):
        columns = []
        for i in range(len(self.intervals)):
            if i == len(self.intervals) - 2:
                columns.append(f'{self.intervals[i]}_above')
                break
            columns.append(f'{self.intervals[i]}_{self.intervals[i+1]}')
        return columns

    def form_img_title(self, interval_name):
        fromdate, enddate = list(self.df_date)[0], list(self.df_date)[-1]
        # print(f'fromdate: {fromdate}, enddate: {enddate}')
        # ex: 1436(20210703-20211231)[低股價表]
        return f'{self.stock_id}({fromdate}-{enddate})[{interval_name}]'

    def create_img_file(self, ratio_y, imgname, y_locator=None, color_init=None):
        x_seq = np.arange(len(self.df))
        xlabels = self.df_date
        # (1) 如果收盤價是-1，則以0取代，因為圖表無法畫出 y = -1 (2) 直接拿掉這筆資料 (先不做)
        price_y = self.df_closing_price.copy()
        cond = (price_y == -1)
        price_y.loc[cond] = 0
        
        # 設定中文字型路徑
        myfont = FontProperties(fname=join(os.getcwd(), 'NotoSansCJK-Medium.ttc'), size=12)
        sns.set_context('paper', font_scale=1.5, rc={'lines.linewidth': 2.5})
        palette = plt.get_cmap('Set1')
        blue, = sns.color_palette('muted', 1)

        fig, ax = plt.subplots(figsize=(20, 8), facecolor=(1, 1, 1))
        # plot1
        n = color_init
        for column in ratio_y:
            n += 1
            # print('ratio_y column:', column)
            ax.plot(x_seq, ratio_y[column], marker='', color=palette(n), linewidth=2, alpha=0.9, label=column)

        ax.set_xlabel('Date')
        ax.set(xlim=(0, len(x_seq) - 1), xticks=x_seq)
        ax.set_xticklabels(xlabels, rotation=45, ha='right')
        ax.set_ylabel('Ratio (%)')
        y_major_locator = MultipleLocator(y_locator)
        ax.yaxis.set_major_locator(y_major_locator)  # 以 y_major_locator 為區隔

        # 設定 y 軸最大值
        tmp = max(ratio_y.values.flatten())
        if tmp < 10:
            ylim_post = tmp + 2
        else:
            ylim_post = tmp + tmp / 10
        ax.set(ylim=(0, ylim_post))
        ax.grid(axis='both')
        ax.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')

        # plot2
        ax2 = ax.twinx()
        ax2.plot(x_seq, price_y, color=blue, lw=0.5, label='Closing Price')
        ax2.fill_between(x_seq, 0, price_y, alpha=.3)
        ax2.set(ylim=(0, None))

        # Show the graph
        plt.title(imgname, fontsize=24, fontweight=2, color='black', fontproperties=myfont)
        plt.savefig(join('img', imgname), bbox_inches='tight')
        # plt.show()
        plt.close()


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
            print('如欲匯出Excel報表，請依序輸入參數(區間名稱id、查找日期範圍、股票代碼)')
            print("(1/4) 以下為目前擁有的區間名稱，請輸入該id")
            ids = []
            for data in sqlConn.select_from_table_generator("SELECT * FROM C_統計區間"):
                print(data[0], data[1], data[2])
                ids.append(data[0])
            interval_name_id = input(":")
            interval_name_id = int(interval_name_id.strip())  # 去除前後空白
            if interval_name_id not in ids:
                raise Exception("id輸入錯誤，請重新操作一次")

            print("(2/4) 日期區間範例格式，以逗號分隔 ex: 20210101,20211231")
            dates = input("請輸入查找日期範圍:")
            dates = dates.strip().split(',')
            # 去除前後空白, 轉成datetime object，可驗證錯誤的日期格式
            dates = [datetime.strptime(d.strip(), '%Y%m%d') for d in dates]
            if dates[0] > dates[1]:  # 檢查日期是否由小到大
                raise Exception('日期需要由小到大，請重新操作一次')
            dates = [f'{d:%Y%m%d}' for d in dates]  # 轉成需要的日期字串
            dates_str = ','.join(dates)

            stock_ids = input("(3/4) 請輸入股票代號，以逗號分隔:")
            stock_ids = stock_ids.strip().split(',')
            stock_ids = [i.strip() for i in stock_ids]
            db_ids = []
            for db_id in sqlConn.select_from_table_generator("SELECT stock_id FROM A_股票代號表"):
                db_ids.append(db_id[0])
            for stock_id in stock_ids:
                if stock_id not in db_ids:
                    raise Exception(f"{stock_id} 資料庫無此股票代號，請重新操作一次")

            print(f'(4/4) 請再次確認參數是否正確，\n區間名稱: {interval_name_id}\n查找日期範圍: {dates_str}\n股票代碼: {stock_ids}')
            recheck = input('正確請填 y:')
            if recheck == 'y':
                check_again = False
    except Exception as e:
        print("輸入異常: ", e)
        print('請再操作一次，謝謝!')
        sys.exit()

    # delete duplicate rows
    print('刪除資料庫內重複的資料')
    sqlConn.delete_duplicate_rows()

    interval_tmp = list(sqlConn.select_from_table_generator(
        f"SELECT interval_name, intervals FROM C_統計區間 WHERE id={interval_name_id}"))
    interval_name, interval_str = interval_tmp[0]
    # print(interval_name, interval_str)

    B_columns = [row.column_name for row in sqlConn.get_all_columns_from_table('B_股權分散表')]
    for stock_id in stock_ids:
        from_date, end_date = dates[0], dates[1]
        sql = f"SELECT * FROM B_股權分散表 WHERE stock_id = '{stock_id}' AND (date_time BETWEEN '{from_date}' AND '{end_date}') ORDER BY date_time ASC"
        raw_data = []
        for data in sqlConn.select_from_table_generator(sql):
            raw_data.append(list(data))
        if raw_data == []:
            print(f'{stock_id}: 資料庫查無區間內資料')
            continue
        elif len(raw_data) == 1:
            print(f'{stock_id}: 區間資料筆數應大於 1 筆')
            continue

        df = pd.DataFrame(raw_data, columns=B_columns)
        # print(df)

        stock_obj = StockRatioDataframe(stock_id, interval_name, interval_str, df)

        # 集結後的ratio，因需求調整為前三個一組，最後一個為一組
        obj_ratios = stock_obj.df_aggregated_ratios
        img_title = stock_obj.img_title
        ratio_pre_y, ratio_post_y = obj_ratios.iloc[:, :-1], obj_ratios.iloc[:, -1:]
        imgname_pre, imgname_post = img_title + 'pre.png', img_title + 'post.png'
        # print(ratio_pre_y, ratio_post_y)
        # print(imgname_pre, imgname_post)

        stock_obj.create_img_file(ratio_pre_y, imgname_pre, y_locator=1, color_init=0)
        stock_obj.create_img_file(ratio_post_y, imgname_post, y_locator=10, color_init=3)

        writer = pd.ExcelWriter(join('excel', img_title + '.xlsx'), engine='xlsxwriter')
        workbook = writer.book
        header_format = workbook.add_format({
            'font_size': 16,
            'bold': True,
            'text_wrap': True,
            'fg_color': '#D7E4BC',
            'border': 1,
            'align': 'center',
        })

        df_excel = stock_obj.df.transpose()
        df_excel.to_excel(writer, index=True, header=False,
                          startrow=1, startcol=0,
                          sheet_name='sheet1',)

        worksheet = writer.sheets['sheet1']
        worksheet.merge_range('A1:J1', img_title, header_format)
        worksheet.insert_image('A10', join(os.getcwd(), 'img', imgname_post))
        worksheet.insert_image('A49', join(os.getcwd(), 'img', imgname_pre))
        writer.save()
        print(f'已匯入報表 {img_title}.xlsx 於 Excel 資料夾!')

    sqlConn.close_cursor()
    sqlConn.close_conn()
    print('程式完成!')


if __name__ == '__main__':
    main()
