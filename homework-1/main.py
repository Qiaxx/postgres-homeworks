"""Скрипт для заполнения данными таблиц в БД Postgres."""
from datetime import datetime

import psycopg2
import csv
import os

# Получение текущей директории
current_dir = os.path.dirname(__file__)
# Путь к файлам CSV
csv_customers_path = os.path.join(current_dir, 'north_data', 'customers_data.csv')
csv_employees_path = os.path.join(current_dir, 'north_data', 'employees_data.csv')
csv_orders_path = os.path.join(current_dir, 'north_data', 'orders_data.csv')

conn_params = {
    "host": "localhost",
    "database": "north",
    "user": "postgres",
    "password": "8283315"
}
try:
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            with open(csv_customers_path, newline='') as csvfile:
                reader = csv.reader(csvfile)

                # Пропуск заголовка
                next(reader)

                for row in reader:
                    customer_id = row[0]
                    company_name = row[1]
                    contact_name = row[2]

                    cursor.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                   (customer_id, company_name, contact_name))

            with open(csv_employees_path, newline='') as csvfile:
                reader = csv.reader(csvfile)

                # Пропуск заголовка
                next(reader)

                for row in reader:
                    employee_id = int(row[0])
                    first_name = row[1]
                    last_name = row[2]
                    title = row[3]
                    birth_date_str = row[4]
                    notes = row[5]

                    # Преобраppзование строки с датой рождения в объект datetime
                    birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d')

                    cursor.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                   (employee_id, first_name, last_name, title, birth_date, notes))

            with open(csv_orders_path, newline='') as csvfile:
                reader = csv.reader(csvfile)

                # Пропуск заголовка
                next(reader)

                for row in reader:
                    order_id = int(row[0])
                    customer_id = row[1]
                    employee_id = int(row[2])
                    order_date_str = row[3]
                    ship_city = row[4]

                    # Преобраppзование строки с датой в объект datetime
                    order_date = datetime.strptime(order_date_str, '%Y-%m-%d')

                    cursor.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                   (order_id, customer_id, employee_id, order_date, ship_city))
finally:
    conn.close()
