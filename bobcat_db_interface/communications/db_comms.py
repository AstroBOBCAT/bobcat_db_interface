import psycopg2  #used to connect to the database in python
from bobcat_db_interface.keys import db_info

def db_connect():
    
    print("in db_connect")

    # This is defining where the file that holds the database connection information lives.
    # This is here to make it easy to change, however, this code may change soon to be housed
    # somewhere where the changes would only need to happen once for changing the database
    # information for all ingestion utility functions.
    #db_file = "ingest_utils/ingest_trial_db_info.txt"

    # Connect to the database in python.
    conn = psycopg2.connect(database = db_info['dbname'], user = db_info['user'], password = db_info['pass'], host = db_info['host'], port = db_info['port'] )

    print("I ran the psycopg2 conn command")
    # Create a cursor instance within the database that allows you to enter SQL commands through python.
    cur = conn.cursor()

    print("at the end of db_connect")

    return cur, conn
