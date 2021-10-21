#!/usr/bin/env python
# coding: utf-8

import pyodbc


class SqlConnection:
    def __init__(self, access_path):
        self.access_path = access_path

        try:
            con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + \
                self.access_path + ';'
            self.conn = pyodbc.connect(con_string, autocommit=False)
            print('Connected To Database')
            self.cursor = self.conn.cursor()
        except pyodbc.Error as e:
            print('Error in Connection', e)
            raise

    def select_from_table_generator(self, sql):
        self.cursor.execute(sql)
        for row in self.cursor.fetchall():
            yield row

    def get_all_columns_from_table(self, table_name):
        return self.cursor.columns(table=table_name)

    def save_to_table(self, table_name, columns, save_data):
        sql_columns = self.form_sql_columns(columns)
        place_holders = self.form_sql_place_holder(columns)
        sql = f'INSERT INTO {table_name} {sql_columns} VALUES {place_holders}'
        self.cursor.execute(sql, save_data)
        self.conn.commit()
        print(f'Inserted into {table_name}: {save_data}')

    def update_table(self, table_name, columns, conditions, save_data):
        sql_columns_ph = self.form_sql_columns_ph(columns)
        sql = f'UPDATE {table_name} SET {sql_columns_ph} WHERE {conditions}'
        self.cursor.execute(sql, save_data)
        self.conn.commit()
        print(f'UPDATE {table_name} {conditions}: {save_data}')

    def delete_rows(self, table_name, conditions):
        sql = f'DELETE FROM {table_name} WHERE {conditions}'
        self.cursor.execute(sql)
        self.conn.commit()
        print(f'DELETE {table_name} WHERE {conditions}')

    def delete_duplicate_rows(self):
        sql_a = """
                SELECT stock_id, date_time, count(*)
                FROM B_股權分散表
                GROUP BY stock_id, date_time
                HAVING count(*) > 1
                """
        self.cursor.execute(sql_a)
        for row in self.cursor.fetchall():     
            sql_b = f"""
                    SELECT id FROM B_股權分散表 
                    WHERE stock_id ='{row[0]}' AND date_time='{row[1]}'
                    ORDER BY id ASC
                    """
            count = row[2]
            self.cursor.execute(sql_b)
            for row in self.cursor.fetchall():
                if count == 1:
                    break
                conditions = f'id = {row[0]}'
                self.delete_rows('B_股權分散表', conditions)
                count -= 1

    def form_sql_columns(self, values):
        sql_columns = ', '.join(values)
        return f'({sql_columns})'

    def form_sql_place_holder(self, values):
        ph = ', '.join('?' * len(values))
        return f'({ph})'

    def form_sql_columns_ph(self, values):
        columns_ph = [v + ' = ?' for v in values]
        sql_columns_ph = ', '.join(columns_ph)
        return f'{sql_columns_ph}'

    def close_cursor(self):
        self.cursor.close()

    def close_conn(self):
        self.conn.close()

