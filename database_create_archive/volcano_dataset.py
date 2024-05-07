import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# load .env variables
load_dotenv()

# details to connect to PostgreSQL
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

# Create the schema needed
# Schema and table information
schema_name = "volcano_schema"

# create schema
def create_schema(cursor, schema_name):
    create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS volcano_schema").format(sql.Identifier(schema_name))
    cursor.execute(create_schema_query)

def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()

    # Create schema
    create_schema(cursor, schema_name)

# Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
