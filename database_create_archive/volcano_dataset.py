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

# Create the schema needed
# Schema and table information
schema_name = "volcano_schema"

# create schema
def create_schema(cursor, schema_name):
    create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
    cursor.execute(create_schema_query)

# create table 
table_name = "volcano_regional_data"


# data imported from https://www.kaggle.com/datasets/ramjasmaurya/volcanoes-on-earth-in-2021
def create_table(cursor):
    cursor.execute("""
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
    """.format(schema_name, table_name))

# Function to insert data from CSV into the table
def insert_data(cursor, csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute("""
                INSERT INTO {}.{} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """.format(schema_name, table_name), (
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

def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()

    # # Create schema
    # create_schema(cursor, schema_name)

    # Create table
    create_table(cursor)

    # Insert data from CSV
    csv_file = "/Users/elenacellitti/Documents/code/my_analysis/datasets/volcanos_of_the_world.csv"
    insert_data(cursor, csv_file)

# Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
