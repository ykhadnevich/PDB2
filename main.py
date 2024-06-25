import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import uuid
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Load environment variables
load_dotenv()

# Connection settings
HOST = os.getenv('host')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')


def create_connection():
    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None


def execute_sql_script(file_path):
    with open(file_path, 'r') as file:
        script = file.read()

    connection = create_connection()
    if connection is None:
        print("Failed to create database connection")
        return

    cursor = connection.cursor()
    try:
        for result in cursor.execute(script, multi=True):
            if result.with_rows:
                print(f"Rows produced by statement '{result.statement}': {result.fetchall()}")
            else:
                print(f"Number of rows affected by statement '{result.statement}': {result.rowcount}")

        connection.commit()
        print(f"SQL script {file_path} executed successfully")
    except Error as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def insert_data():
    connection = create_connection()
    if connection is None:
        print("Failed to create database connection")
        return

    cursor = connection.cursor()
    fake = Faker()

    # Insert 1,000,000 rows into opt_clients
    print("Inserting into opt_clients...")
    client_insert_query = """
        INSERT INTO opt_clients (id, name, surname, email, phone, address, status) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    clients_data = [
        (str(uuid.uuid4()), fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(), fake.address(),
         random.choice(['active', 'inactive']))
        for _ in range(1000)
    ]
    cursor.executemany(client_insert_query, clients_data)
    connection.commit()
    print("Inserted into opt_clients.")

    # Insert 1,000 rows into opt_products
    print("Inserting into opt_products...")
    product_insert_query = """
        INSERT INTO opt_products (product_name, product_category, description) 
        VALUES (%s, %s, %s)
    """
    categories = ['Category1', 'Category2', 'Category3', 'Category4', 'Category5']
    products_data = [
        (fake.word(), random.choice(categories), fake.text())
        for _ in range(1000)
    ]
    cursor.executemany(product_insert_query, products_data)
    connection.commit()
    print("Inserted into opt_products.")

    # Insert 10,000,000 rows into opt_orders
    print("Inserting into opt_orders...")
    order_insert_query = """
        INSERT INTO opt_orders (order_date, client_id, product_id) 
        VALUES (%s, %s, %s)
    """
    order_date_start = datetime.now() - timedelta(days=365 * 5)
    orders_data = [
        (order_date_start + timedelta(days=random.randint(0, 365 * 5)), random.choice(clients_data)[0],
         random.randint(1, 1000))
        for _ in range(1000)
    ]
    # Use chunks to avoid memory issues
    chunk_size = 10000
    for i in range(0, len(orders_data), chunk_size):
        cursor.executemany(order_insert_query, orders_data[i:i + chunk_size])
        connection.commit()
        print(f"Inserted {i + chunk_size} rows into opt_orders...")

    print("Inserted into opt_orders.")

    # Close the cursor and connection
    cursor.close()
    connection.close()


if __name__ == "__main__":
    # Execute the table creation script
    execute_sql_script('script_01_create_tables.sql')

    # Insert data
    insert_data()

    # Execute the optimization demo script
    execute_sql_script('optimization_demo.sql')
