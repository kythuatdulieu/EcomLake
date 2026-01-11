import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# Database connection details
DB_USER = "root"
DB_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "admin")
DB_HOST = os.getenv("MYSQL_HOST", "mysql")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))
DB_NAME = "olist"

# Data directory
DATA_DIR = "./data/raw"

def create_database():
    import pymysql
    print(f"Connecting to MySQL at {DB_HOST} as {DB_USER} to create database '{DB_NAME}'...")
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        conn.commit()
        conn.close()
        print(f"Database '{DB_NAME}' created or exists.")
    except Exception as e:
        print(f"Error creating database: {e}")

def get_engine():
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def seed_database():
    create_database()
    print(f"Connecting to database at {DB_HOST}:{DB_PORT}...")
    try:
        engine = get_engine()
        # Test connection
        with engine.connect() as conn:
            pass
        print("Connection successful.")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return

    # Map CSV filenames to table names
    files_to_tables = {
        "olist_customers_dataset.csv": "customers",
        "olist_sellers_dataset.csv": "sellers",
        "olist_products_dataset.csv": "products",
        "olist_orders_dataset.csv": "orders",
        "olist_order_items_dataset.csv": "order_items",
        "olist_order_payments_dataset.csv": "payments",
        "olist_order_reviews_dataset.csv": "order_reviews",
        "olist_geolocation_dataset.csv": "geolocation",
        "product_category_name_translation.csv": "product_category_name_translation"
    }

    for filename, table_name in files_to_tables.items():
        file_path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found. Skipping.")
            continue
            
        print(f"Loading {filename} into table '{table_name}'...")
        try:
            df = pd.read_csv(file_path)
            # Write to SQL
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print(f"Successfully loaded {len(df)} rows into '{table_name}'.")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")

if __name__ == "__main__":
    seed_database()
