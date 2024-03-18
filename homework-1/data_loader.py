from datetime import datetime
import psycopg2
import csv
import os


class DataLoader:
    def __init__(self, conn_params, folder_name='north_data', customers_file='customers_data.csv',
                 employees_file='employees_data.csv', orders_file='orders_data.csv'):
        """
        Конструктор для файлов CSV и получения пути к файлам
        :param conn_params: параметры подключения к БД
        :param folder_name: название папки, где находятся файлы (по умолчанию название - 'north_data')
        :param customers_file: название файла customers формата csv (по умолчанию название - 'customers_data.csv')
        :param employees_file: название файла employees формата csv (по умолчанию название - 'employees_data.csv')
        :param orders_file: название файла orders формата csv (по умолчанию название - 'orders_data.csv')
        """
        self.conn_params = conn_params
        self.current_dir = os.path.dirname(__file__)
        self.csv_customers_path = os.path.join(self.current_dir, folder_name, customers_file)
        self.csv_employees_path = os.path.join(self.current_dir, folder_name, employees_file)
        self.csv_orders_path = os.path.join(self.current_dir, folder_name, orders_file)

    def load_customers(self):
        """
        Метод для внесение данных в таблицу customers с подключением к БД
        :return: нет
        """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cursor:
                with open(self.csv_customers_path, newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    next(reader)  # Пропуск заголовка
                    for row in reader:
                        cursor.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                       (row[0], row[1], row[2]))

    def load_employees(self):
        """
        Метод для внесение данных в таблицу employees с подключением к БД
        :return: нет
        """
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
        """
        Метод для внесение данных в таблицу orders с подключением к БД
        :return: нет
        """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cursor:
                with open(self.csv_orders_path, newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    next(reader)  # Пропуск заголовка
                    for row in reader:
                        order_date = datetime.strptime(row[3], '%Y-%m-%d')
                        cursor.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                       (int(row[0]), row[1], int(row[2]), order_date, row[4]))
