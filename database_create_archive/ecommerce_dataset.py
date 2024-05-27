import os
import psycopg2
import pandas as pd
from psycopg2 import sql
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Details to connect to PostgreSQL
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

# Variable names
schema_name = "ecommerce_schema"
table_name = "nov_19"

# Create table if not exists function
def create_table(cursor, schema_name, table_name):
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            event_time TIMESTAMP NOT NULL,
            event_type VARCHAR,
            product_id VARCHAR,
            category_id VARCHAR,
            category_code VARCHAR,
            brand VARCHAR,
            price INT,
            user_id VARCHAR,
            user_session VARCHAR
        )
    """).format(sql.Identifier(schema_name), sql.Identifier(table_name))
    cursor.execute(create_table_query)
    print("Table created successfully!")

# Path to CSV files
base_path = '/Users/elenacellitti/Downloads/ecommerce_data/'
file_names = ['ecommerce_nov_19.csv', 'ecommerce_oct_19.csv']
csv_files = [base_path + file_name for file_name in file_names]

# Read and append CSV files into one DataFrame
df_list = [pd.read_csv(csv_file) for csv_file in csv_files]
combined_df = pd.concat(df_list, ignore_index=True)

# Connect to PostgreSQL
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
conn.autocommit = False  # Ensure autocommit is off
cursor = conn.cursor()

# Create the table if it doesn't exist
create_table(cursor, schema_name, table_name)

# Insert data into the SQL table
insert_query = sql.SQL("""
    INSERT INTO {}.{} (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""").format(sql.Identifier(schema_name), sql.Identifier(table_name))

try:
    for index, row in combined_df.iterrows():
        cursor.execute(insert_query, tuple(row))
    conn.commit()
    print("Data inserted successfully!")
except Exception as e:
    conn.rollback()
    print(f"An error occurred: {e}")
finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()
