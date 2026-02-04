"""
This script fetches weather data from an API and inserts it into a PostgreSQL database.
It handles database connection, table creation, and record insertion.
"""
import psycopg2
from api_request import mock_fetch_data, fetch_data

def connect_to_db():
    """
    Establishes a connection to the PostgreSQL database using hardcoded credentials.
    Returns a connection object.
    """
    print("Connecting to the PostgreSQL database...")

    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="db",
            user="db_user",
            password="db_password"
        ) 
        return conn

    except psycopg2.Error as e:
        print(f"DB connection failed: {e}")
        raise

def create_table(conn):
    """
    Creates the 'dev' schema and 'raw_weather_data' table if they do not already exist.
    The table stores raw weather information including city, temperature, and timestamps.
    """
    print("Creating weather_data table if not exists...")

    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS dev;
    CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
        id SERIAL PRIMARY KEY,
        city TEXT,
        temperature FLOAT,
        weather_description TEXT,
        wind_speed FLOAT,
        time TIMESTAMP,
        inserted_at TIMESTAMP DEFAULT NOW(),
        utc_offset TEXT
    );
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            print("Table created or already exists.")

    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise


def insert_records(conn, data):
    """
    Extracts current weather and location data from the API response and inserts it into the database.
    """
    print("Inserting records into the weather_data table...")

    try:
        # Extract nested data from the API response JSON
        weather = data['current']
        location = data['location']
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.raw_weather_data (
                city, 
                temperature, 
                weather_description, 
                wind_speed, 
                time,
                inserted_at, 
                utc_offset
                )
            VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                """,
                (
                    location['name'],
                    weather['temperature'],
                    weather['weather_descriptions'][0],
                    weather['wind_speed'],
                    location['localtime'],
                    location['utc_offset']
                ))
        conn.commit()
        print("Record inserted successfully.")
        
    except psycopg2.Error as e:
        print(f"Failed to insert record: {e}")
        raise

def main():
    """
    Main execution flow:
    1. Fetch data from the weather API.
    2. Connect to the database.
    3. Ensure the target table exists.
    4. Insert the fetched data into the database.
    """
    try:
        data = fetch_data()      # Step 1: Fetch data
        conn = connect_to_db()   # Step 2: Connect to DB
        create_table(conn)       # Step 3: Setup table
        insert_records(conn, data) # Step 4: Ingest data

    except Exception as e:
        print(f"An error occurred during execution: {e}")

    finally:
        if 'conn' in locals():
            conn.close()
            print("DB Connection closed.")

main()