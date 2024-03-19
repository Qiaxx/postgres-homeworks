"""Скрипт для заполнения данными таблиц в БД Postgres."""
from data_loader import DataLoader
import os

password_bd = os.getenv('PASSWORD_BD')

if __name__ == "__main__":
    conn_params = {
        "host": "localhost",
        "database": "north",
        "user": "postgres",
        "password": password_bd
    }

    data_loader = DataLoader(conn_params)
    data_loader.load_customers()
    data_loader.load_employees()
    data_loader.load_orders()

