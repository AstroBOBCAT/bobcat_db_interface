import psycopg  #used to connect to the database in python
from bobcat_db_interface.keys import db_info

def db_connect():

    """
    The below shouldn't be necessary if I did it right...
    # Connect to the database in python.
    conn = psycopg.connect(
        dbname = os.environ["POSTGRES_DB"],
        user = os.environ["POSTGRES_USER"],
        password = os.environ["POSTGRES_PASSWORD"],
        host = os.environ["POSTGRES_HOST"],
        port = os.environ["POSTGRES_PORT"],
    )
    """
    conn = psycopg.connect(
        dbname = db_info['dbname'],
        user = db_info['user'],
        password = db_info['pass'],
        host = db_info['host'],
        port = db_info['port']
    )

    # Create a cursor instance within the database that allows you to enter SQL commands through python.
    cur = conn.cursor()

    return cur, conn
