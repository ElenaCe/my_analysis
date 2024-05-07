import os
import psycopg2
import csv
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

# Variable names
schema_name = "volcano_schema"
table_name = "volcano_regional_data"

# create schema
def create_schema(cursor, schema_name):
    create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
    cursor.execute(create_schema_query)

# create table 
def create_table(cursor, schema_name, table_name):
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            volcano_name VARCHAR,
            volcano_image_url VARCHAR,
            volcano_type VARCHAR,
            country VARCHAR,
            region VARCHAR,
            subregion VARCHAR,
            epoch_period VARCHAR,
            last_eruption VARCHAR,
            summit_m INT,
            elevation_ft INT,
            latitude VARCHAR,
            longitude VARCHAR,
            population_within_5km INT,
            population_within_10km INT,
            population_within_30km INT,
            population_within_100km INT
        )
    """).format(sql.Identifier(schema_name), sql.Identifier(table_name))
    cursor.execute(create_table_query)
    print("Table created successfully!")

# Function to insert data from CSV into the table
def insert_data(cursor, schema_name, table_name, csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute(sql.SQL("""
                INSERT INTO {}.{} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(schema_name), sql.Identifier(table_name)), (
                row['volcano_name'],
                row['volcano_image_url'],
                row['volcano_type'],
                row['country'],
                row['region'],
                row['subregion'],
                row['epoch_period'],
                row['last_eruption'],
                row['summit_m'],
                row['elevation_ft'],
                row['latitude'],
                row['longitude'],
                row['population_within_5km'],
                row['population_within_10km'],
                row['population_within_30km'],
                row['population_within_100km']
            ))
    print("Data inserted successfully!")

def main():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()

        # create schema
        create_schema(cursor, schema_name)
        
        # create table
        create_table(cursor, schema_name, table_name)

        # data from csv
        csv_file = "/Users/elenacellitti/Documents/code/my_analysis/datasets/volcanos_of_the_world.csv"
        insert_data(cursor, schema_name, table_name, csv_file)

        # commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        
    # for error handling
    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    main()
