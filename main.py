import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import uuid
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import time

# Load environment variables
load_dotenv()

# Connection settings
HOST = os.getenv('host')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')


def create_connection(database=None):
    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=database
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None


def execute_sql_script(file_path, database=None):
    with open(file_path, 'r') as file:
        script = file.read()

    connection = create_connection(database=database)
    if connection is None:
        print("Failed to create database connection")
        return

    cursor = connection.cursor()
    try:
        cursor.execute(script, multi=True)  # Execute the script

        for result in cursor.stored_results():
            result.fetchall()

        #for result in cursor.execute(script, multi=True):
           # if result.with_rows:
              #  print(f"Rows produced by statement '{result.statement}': {result.fetchall()}")
            #else:
               # print(f"Number of rows affected by statement '{result.statement}': {result.rowcount}")

        connection.commit()
        print(f"SQL script {file_path} executed successfully")
    except Error as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def insert_data():
    connection = create_connection(database=DATABASE)
    if connection is None:
        print("Failed to create database connection")
        return

    cursor = connection.cursor()
    fake = Faker()

    # Insert 10,000 rows into opt_clients
    print("Inserting into opt_clients...")
    client_insert_query = """
        INSERT INTO opt_clients (id, name, surname, email, phone, address, status) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    clients_data = [
        (str(uuid.uuid4()), fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(), fake.address(),
         random.choice(['active', 'inactive']))
        for _ in range(10000)
    ]
    cursor.executemany(client_insert_query, clients_data)
    connection.commit()
    print("Inserted into opt_clients.")

    # Insert 10,000 rows into opt_products
    print("Inserting into opt_products...")
    product_insert_query = """
        INSERT INTO opt_products (product_name, product_category, description) 
        VALUES (%s, %s, %s)
    """
    categories = ['Category1', 'Category2', 'Category3', 'Category4', 'Category5']
    products_data = [
        (fake.word(), random.choice(categories), fake.text())
        for _ in range(10000)
    ]
    cursor.executemany(product_insert_query, products_data)
    connection.commit()
    print("Inserted into opt_products.")

    # Insert 10,000 rows into opt_orders
    print("Inserting into opt_orders...")
    order_insert_query = """
        INSERT INTO opt_orders (order_date, client_id, product_id) 
        VALUES (%s, %s, %s)
    """
    order_date_start = datetime.now() - timedelta(days=365 * 5)
    orders_data = [
        (order_date_start + timedelta(days=random.randint(0, 365 * 5)), random.choice(clients_data)[0],
         random.randint(1, 10000))
        for _ in range(10000)
    ]
    cursor.executemany(order_insert_query, orders_data)
    connection.commit()
    print("Inserted into opt_orders.")

    # Close the cursor and connection
    cursor.close()
    connection.close()


def verify_data():
    connection = create_connection(database=DATABASE)
    if connection is None:
        print("Failed to create database connection")
        return

    cursor = connection.cursor()

    try:
        # Verify number of rows in opt_clients
        cursor.execute("SELECT COUNT(*) FROM opt_clients")
        print(f"Total rows in opt_clients: {cursor.fetchone()[0]}")

        # Verify number of rows in opt_products
        cursor.execute("SELECT COUNT(*) FROM opt_products")
        print(f"Total rows in opt_products: {cursor.fetchone()[0]}")

        # Verify number of rows in opt_orders
        cursor.execute("SELECT COUNT(*) FROM opt_orders")
        print(f"Total rows in opt_orders: {cursor.fetchone()[0]}")

        # Describe opt_clients table
        cursor.execute("DESCRIBE opt_clients")
        print("Schema of opt_clients:")
        for row in cursor.fetchall():
            print(row)

        # Describe opt_products table
        cursor.execute("DESCRIBE opt_products")
        print("Schema of opt_products:")
        for row in cursor.fetchall():
            print(row)

        # Describe opt_orders table
        cursor.execute("DESCRIBE opt_orders")
        print("Schema of opt_orders:")
        for row in cursor.fetchall():
            print(row)

    except Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        connection.close()


def time_query(query, database):
    connection = create_connection(database=database)
    if connection is None:
        print("Failed to create database connection")
        return None

    cursor = connection.cursor()

    start_time = time.time()
    cursor.execute(query)
    result = cursor.fetchall()
    end_time = time.time()

    cursor.close()
    connection.close()

    execution_time = end_time - start_time
    return execution_time, result



if __name__ == "__main__":
    # Execute the table creation script
    execute_sql_script('script_01_create_tables.sql')

    # Insert data
    insert_data()

    # Define your queries
    non_optimized_query = """
    SELECT c.name, c.surname, p.product_name, o.order_date
    FROM opt_orders o
    JOIN opt_clients c ON o.client_id = c.id
    JOIN opt_products p ON o.product_id = p.product_id
    """

    optimized_query = """
    WITH CTE_Clients AS (
        SELECT id, name, surname
        FROM opt_clients
    ), CTE_Products AS (
        SELECT product_id, product_name
        FROM opt_products
    )
    SELECT c.name, c.surname, p.product_name, o.order_date
    FROM opt_orders o
    JOIN CTE_Clients c ON o.client_id = c.id
    JOIN CTE_Products p ON o.product_id = p.product_id
    """

    # Measure execution time for non-optimized query
    non_optimized_time, _ = time_query(non_optimized_query, database=DATABASE)
    print(f"Non-optimized query execution time: {non_optimized_time:.4f} seconds")

    # Measure execution time for optimized query
    optimized_time, _ = time_query(optimized_query, database=DATABASE)
    print(f"Optimized query execution time: {optimized_time:.4f} seconds")

    # Execute the optimization demo script
    execute_sql_script('optimization_demo.sql', database=DATABASE)

    # Verify inserted data and table schemas
    verify_data()
