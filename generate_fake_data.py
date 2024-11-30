import psycopg2
from psycopg2 import sql
from faker import Faker

# Create an instance of the Faker library
fake = Faker()

# Function to create tables in the database
def create_tables(cursor):
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),  -- Increased length
            last_name VARCHAR(100),   -- Increased length
            email VARCHAR(255),       -- Increased length
            country VARCHAR(100)      -- Increased length
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(255), -- Increased length
            category VARCHAR(100),     -- Increased length
            price NUMERIC(10, 2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id SERIAL PRIMARY KEY,
            customer_id INT REFERENCES customers(customer_id),
            product_id INT REFERENCES products(product_id),
            order_date DATE,
            quantity INT,
            total_price NUMERIC(10, 2)
        );
        """
    ]
    for query in create_table_queries:
        cursor.execute(query)

# Function to insert fake data into the customers and products tables
def insert_customers_and_products(cursor):
    # Lists to store generated customer and product IDs
    customer_ids = []
    product_ids = []

    # Insert fake data into the customers table
    for _ in range(100):
        cursor.execute(
            """
            INSERT INTO customers (first_name, last_name, email, country)
            VALUES (%s, %s, %s, %s) RETURNING customer_id;
            """,
            (fake.first_name(), fake.last_name(), fake.email(), fake.country())
        )
        # Store the generated customer_id
        customer_id = cursor.fetchone()[0]
        customer_ids.append(customer_id)

    # Insert fake data into the products table
    for _ in range(50):
        cursor.execute(
            """
            INSERT INTO products (product_name, category, price)
            VALUES (%s, %s, %s) RETURNING product_id;
            """,
            (fake.word().capitalize(), fake.word().capitalize(), round(fake.random_number(digits=2) + fake.random.random(), 2))
        )
        # Store the generated product_id
        product_id = cursor.fetchone()[0]
        product_ids.append(product_id)

    return customer_ids, product_ids

# Function to insert fake data into the orders table
def insert_orders(cursor, customer_ids, product_ids):
    # Insert fake data into the orders table
    for _ in range(200):
        # Select a random customer_id and product_id from the generated lists
        customer_id = fake.random_element(customer_ids)
        product_id = fake.random_element(product_ids)
        quantity = fake.random_int(min=1, max=10)
        
        # Fetch the price of the selected product to calculate total_price
        cursor.execute(
            """
            SELECT price FROM products WHERE product_id = %s;
            """,
            (product_id,)
        )
        price = cursor.fetchone()[0]
        total_price = round(price * quantity, 2)
        
        cursor.execute(
            """
            INSERT INTO orders (customer_id, product_id, order_date, quantity, total_price)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (
                customer_id,
                product_id,
                fake.date_between(start_date='-1y', end_date='today'),
                quantity,
                total_price
            )
        )

# Function to connect to the database and create tables and insert data
def main():
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            dbname="mydatabase",  # replace with your database name
            user="admin",         # replace with your username
            password="adminpassword", # replace with your password
            host="127.0.0.1",     # replace with your host
            port=5432             # replace with your port
        )

        # Create a cursor object
        cursor = conn.cursor()

        # Create tables
        create_tables(cursor)
        print("Tables created successfully!")

        # Insert customers and products
        customer_ids, product_ids = insert_customers_and_products(cursor)
        print("Customers and Products inserted successfully!")

        # Insert orders with valid customer_id and product_id references
        insert_orders(cursor, customer_ids, product_ids)
        print("Orders inserted successfully!")

        # Commit changes
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        # Close the connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Run the main function
if __name__ == "__main__":
    main()
