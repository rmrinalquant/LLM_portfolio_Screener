from dotenv import load_dotenv
import os
import psycopg2

'''
Authenticate connection with database
Parameters
----------
    path: path to .env file

Returns
-------
    conn: connection object

Future enhancements
----------
    Add logging
    
'''
def get_connection(path=None):
    load_dotenv(path)

    try:
        conn = psycopg2.connect(
            host= os.getenv("DB_HOST"),
            database= os.getenv("DB_NAME"),
            user= os.getenv("DB_USER"),
            password= os.getenv("DB_PASSWORD"),
            port= os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None