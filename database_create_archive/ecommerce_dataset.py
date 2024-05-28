import os
import psycopg2
import pandas as pd
from psycopg2 import sql
from dotenv import load_dotenv

# Get the csv files
base_path = '/Users/elenacellitti/Downloads/archive/'
file_names = ['2019-Oct.csv', '2019-Nov.csv', '2019-Dec.csv']
csv_files = [base_path + file_name for file_name in file_names]

# Selected columns
selected_columns = ['event_time','event_type','product_id','price','user_id','user_session']

# List of all the csvs into a DataFrame with only selected columns
df_list = [pd.read_csv(csv_file, usecols=selected_columns) for csv_file in csv_files]

# Concatenate all to make one dataframe
combined_df = pd.concat(df_list, ignore_index=True)

# Check dataframe
print(combined_df.head)

# Connect to postegres to store dataset

# Load .env variables
load_dotenv()

# Details to connect
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

# Connect to PostgreSQL
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cursor = conn.cursor()

# Variable names
schema_name = "ecommerce_schema"
table_name = "oct_dec_19"

# Create table if not exists function
def create_table(cursor, schema_name, table_name):
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            event_time TIMESTAMP NOT NULL,
            event_type VARCHAR,
            product_id VARCHAR,
            price INT,
            user_id VARCHAR,
            user_session VARCHAR
        )
    """).format(sql.Identifier(schema_name), sql.Identifier(table_name))
    cursor.execute(create_table_query)
    print("Table created successfully!")

# Create the table if it doesn't exist
create_table(cursor, schema_name, table_name)

# Insert data into the SQL table
insert_query = sql.SQL("""
    INSERT INTO {}.{} (event_time, event_type, product_id,price, user_id, user_session)
    VALUES (%s, %s, %s, %s, %s, %s)
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
