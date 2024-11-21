import psycopg2  #used to connect to the database in python


def db_connect(db_info_file):

    # This is defining where the file that holds the database connection information lives.
    # This is here to make it easy to change, however, this code may change soon to be housed
    # somewhere where the changes would only need to happen once for changing the database
    # information for all ingestion utility functions.
    #db_file = "ingest_utils/ingest_trial_db_info.txt"

    # Read the database name, user, password, host, and port from a text file that is selectively given out.
    db_info_file = open(db_info_file)
    db_info = db_info_file.read().split("\n")[0:5] #read only the first 5 lines and separate based on newline character
    db_info_file.close() #always make sure to close the file   

    # Connect to the database in python.
    conn = psycopg2.connect(database = db_info[0], user = db_info[1], password = db_info[2], host = db_info[3],
                            port = db_info[4] )
    
    # Create a cursor instance within the database that allows you to enter SQL commands through python.
    cur = conn.cursor()

    return cur, conn