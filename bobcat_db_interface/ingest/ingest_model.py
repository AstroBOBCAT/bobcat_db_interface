## ingest_model.py contains code for the function ingest_model(). This function is what will
## connect to a database and ingest a new model entry into the database. There is no manipulation of 
## the model data within this function. This function takes an instance of the model class.
## So all manipulation should be done prior to sending the model object to this function. 
## Currently the function doesn't have a failsafe in terms of if the ingestion of the model doesn't go right.
## This is something that needs to be added into the code because SQL will throw an error and stop if you
## try to ingest something either with the same primary key already in the database or into a table that
## doesn't yet exist.



######
# Import the need libraries and modules for the function to work.
import psycopg2 #what's needed to connect to a postgres database within python
######

# This is defining where the file that holds the database connection information lives.
# This is here to make it easy to change, however, this code may change soon to be housed
# somewhere where the changes would only need to happen once for changing the database
# information for all ingestion utility functions.
db_file = "ingest_utils/ingest_trial_db_info.txt"


################
def ingest_model(model):

    ''' Ingests a single model class instance into a predefined database.

    Inputs:
        model class instance
        OR
        array containing the model class parameters
    Outputs:
        NONE - currently, will fix to show whether or not it successfully ingests the model
    '''

    # Read the database name, user, password, host, and port from a text file that is selectively given out.
    db_info_file = open(db_file)
    db_info = db_info_file.read().split("\n")[0:5] #read only the first 5 lines and separate based on newline character
    db_info_file.close() #always make sure to close the file

    # Connect to the database in python.
    conn = psycopg2.connect(database = db_info[0], user = db_info[1], password = db_info[2], host = db_info[3],
                            port = db_info[4] )
    
    # Create a cursor instance within the database that allows you to enter SQL commands through python.
    cur = conn.cursor()

    # Ingest the model into the database.
    cur.execute("INSERT INTO model(paper_link, source_name, eccentricity, m1, m2, m_tot, m_chirp, mu, q,\
    evidence, evidence_type, evidence_type_waveband, inclination, semimajor_axis, separation, period_epoch, \
    orb_frequency, orb_period, summary, caveats) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
    %s, %s, %s, %s, %s, %s, %s);", model)
    conn.commit() #make sure to actually commit the SQL command to the database

    # Always make sure to close the connection to the database 
    # (much like you should always close a file when done with it).
    conn.close()
###############