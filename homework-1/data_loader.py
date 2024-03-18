from datetime import datetime
import psycopg2
import csv
import os


class DataLoader:
    def __init__(self, conn_params, folder_name='north_data', customers_file='customers_data.csv',
                 employees_file='employees_data.csv', orders_file='orders_data.csv'):

        self.conn_params = conn_params
        self.current_dir = os.path.dirname(__file__)
        self.csv_customers_path = os.path.join(self.current_dir, folder_name, customers_file)
        self.csv_employees_path = os.path.join(self.current_dir, folder_name, employees_file)
        self.csv_orders_path = os.path.join(self.current_dir, folder_name, orders_file)

    def load_customers(self):
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cursor:
                with open(self.csv_customers_path, newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    next(reader)  # Пропуск заголовка
                    for row in reader:
                        cursor.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                       (row[0], row[1], row[2]))

    def load_employees(self):
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cursor:
                with open(self.csv_employees_path, newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    next(reader)  # Пропуск заголовка
                    for row in reader:
                        birth_date = datetime.strptime(row[4], '%Y-%m-%d')
                        cursor.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                       (int(row[0]), row[1], row[2], row[3], birth_date, row[5]))

    def load_orders(self):
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cursor:
                with open(self.csv_orders_path, newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    next(reader)  # Пропуск заголовка
                    for row in reader:
                        order_date = datetime.strptime(row[3], '%Y-%m-%d')
                        cursor.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                       (int(row[0]), row[1], int(row[2]), order_date, row[4]))
