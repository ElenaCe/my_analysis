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
table_name = "oct_nov_19"

# Path where the CSV file will be saved
output_directory = "/Users/elenacellitti/Documents/"
output_csv = os.path.join(output_directory, f"{table_name}.csv")

# Establishing the connection
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

# Query to fetch data from the specified table in the specified schema
query = sql.SQL("SELECT * FROM {}.{}").format(
    sql.Identifier(schema_name),
    sql.Identifier(table_name)
)

# Convert the query to a string
query_str = query.as_string(conn)

# Using pandas to execute the query and store the result in a DataFrame
df = pd.read_sql_query(query_str, conn)

# Exporting the DataFrame to a CSV file
df.to_csv(output_csv, index=False)

# Closing the connection
conn.close()

print(f"Data from {schema_name}.{table_name} has been exported to {output_csv}")
