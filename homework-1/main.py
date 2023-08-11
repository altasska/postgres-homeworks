"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os

PASS_POSTGRES = os.environ.get("PASS_POSTGRES")

PATH_TO_EMPLOYEES = os.path.join(os.path.dirname(__file__), 'north_data', 'employees_data.csv')
ABSOLUTE_PATH_TO_EMPLOYEES = os.path.abspath(PATH_TO_EMPLOYEES)

PATH_TO_CUSTOMERS = os.path.join(os.path.dirname(__file__), 'north_data', 'customers_data.csv')
ABSOLUTE_PATH_TO_CUSTOMERS = os.path.abspath(PATH_TO_CUSTOMERS)

PATH_TO_ORDERS = os.path.join(os.path.dirname(__file__), 'north_data', 'orders_data.csv')
ABSOLUTE_PATH_TO_ORDERS = os.path.abspath(PATH_TO_ORDERS)

conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password=PASS_POSTGRES)

try:
    with conn:
        with conn.cursor() as cur:
            # загрузка данных в таблицу employees
            with open(ABSOLUTE_PATH_TO_EMPLOYEES, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO employees (first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s)",
                        (row[1], row[2], row[3], row[4], row[5])
                    )

            # загрузка данных в таблицу customers
            with open(ABSOLUTE_PATH_TO_CUSTOMERS, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                        (row[0], row[1], row[2])
                    )

            # загрузка данных в таблицу orders
            with open(ABSOLUTE_PATH_TO_ORDERS, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO orders (customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s)",
                        (row[1], row[2], row[3], row[4])
                    )
except Exception as e:
    print(e)
finally:
    conn.close()
