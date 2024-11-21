## This ingestion script is to be used for ingesting sources from a google spreadsheet. This was choosen as the 
## offline verison on BOBcat that was created and used to collected candidates before BOBcat started and while
## BOBcat was in the beginning stages is in a google spreadsheet. However, this ingestion script could be used for
## any google spreadsheet that is setup in the correct way and that the key for is known. 
## There should be another ingestion script that deals with pulling data for other databases so candidates from
## large surveys, such as CRTs, are easy to ingest into BOBcat without having to create an entry into a google 
## spreadsheet for every single candidate in the surveys.

#########
# Import the libraries, modules, and functions needed for the ingest function to work properly.
#import sys #this allows this script to be run from the bash command line with the key as an argument 
import pandas as pd #pandas dataframe that the csv file information gets read into for easy manipulation in python
import numpy as np #numpy
 
# Import the utilities made for BOBcat itself and the specific ingestion utilities made for this process.
from gw_utils import calc as gw_calc








################
def ingest_candidate(candidate, db_info_path):

    ''' Ingests a single candidate into a predefined database.

    Inputs:
        array containing the candidate parameters
    Outputs:
        NONE
    '''

    cur, conn = db_connect(db_info_path)

    # Ingest the model into the database.
    cur.execute("INSERT INTO candidate(\
        name, \
        ra_deg, \
        dec_deg,\
        redshift, \
        obs_type_done) \
        VALUES (%s,%s,%s,%s,%s);", candidate)
    conn.commit() #make sure to actually commit the SQL command to the database

    # Always make sure to close the connection to the database 
    # (much like you should always close a file when done with it).
    conn.close()
###############



################
def ingest_binary_model(binary_model, db_info_path):

    ''' Ingests a single binary model into a predefined database.

    Inputs:
        array containing the binary model class parameters
    Outputs:
        NONE - currently, will fix to show whether or not it successfully ingests the model
    '''

    cur, conn = db_connect(db_info_path)

    # Ingest the model into the database.
    cur.execute("INSERT INTO binary model(\
        paper,\
        candidate_name,\
        eccentricity,\
        m1,\
        m2,\
        mtot,\
        mc,\
        mu,\
        q,\
        evid1_type,\
        evid1_note,\
        evid1_wavelength,\
        evid2_type,\
        evid2_note,\
        evid2_wavelength,\
        evid3_type,\
        evid3_note,\
        evid3_wavelength,\
        evid4_type,\
        evid4_note,\
        evid4_wavelength,\
        inclination,\
        semimajor_axis,\
        seperation,\
        period_epoch,\
        orb_freq,\
        orb_period,\
        summary,\
        caveats,\
        ext_proj) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", binary_model)
    conn.commit() #make sure to actually commit the SQL command to the database

    # Always make sure to close the connection to the database 
    # (much like you should always close a file when done with it).
    conn.close()
###############







###############
def ingest(key, db_info_path):

    '''Ingestion of sources and models into database starting from a specific google spreadsheet
    setup.
    
    Inputs:
        key(string) = this is the key that is associated with a google spreadsheet
    Outputs:
        statements telling you whether a source/model has been ingested into the database
    '''

    # Create the url to the google spreadsheet that contains the source information and a possible link to 
    # a model parameter extraction google spreadsheet from the key string value given for using the function.
    url = create_url(key)

    # Pull the relativant information about the source from the google spreadsheet, this includes the paper link,
    # the name of the source in NED, and a link to another google spreadsheet that contains the model parameter information.
    # This information gets put into a pandas dataframe for easy manipulation in python.
    ingestion_data = pd.read_csv(url, usecols = ["Paper Link", "NED Name", "Model Parameter Details"])


    # Go through all the different sources from the spreadsheet.
    for i in range((len(ingestion_data))):
        # Set the ned_name variable as the information from the NED Name column.
        ned_name = ingestion_data.iloc[i,1]
        # If there is a ned_name given (note that it is possible some candidates don't have this so think about
        # what would need to be done to account for that), get the j2000 ra and dec of the source in degrees, as well
        # as the redshift. Should probably put in / use the NED name resolver function in BOBcat utils at this 
        # point as well, will come back and figure out exactly where in the script it should be added.
        if ned_name:
            # Set the ra_deg and dec_deg variables to the j2000 ra and dec positions given in NED for the source.
            ra_deg, dec_deg = (coord_converter(ned_name))
            # Set redshift variable to the redshift given in NED for the source.
            redshift = NED_z(ra_deg, dec_deg)
            # Create the source array needed to use the ingest_source function. 
            # This should truly be whether a creation of an instance of the source class is put. Still currently
            # working and debugging the class code after moving it from ipython notebooks to regular script 
            # python. Will come back and fix that as soon as the source class is better situated.
            candidate = [ned_name, ra_deg, dec_deg, redshift]
            # Now try to ingest the source. There is a try/except block here because you cannot ingest the same
            # source more than once. The primary key for the source table is the source name, so if you try to ingest
            # a source with a name that is already housed in the database SQL with throw an error and fully stop
            # the ingestion process. However, there is the possibility that a source would have multiple papers, and
            # therefore multiple models, so there could be multiple entries for a source in the spreadsheet. This
            # accounts for the SQL error thrown when that happens.
            try:
                ingest_candidate(candidate, db_info_path)
                print("candidate ingested")
            except:
                print("candidate not ingested")


        for i in range((len(ingestion_data))):
        # Check to see if there is an associated model parameter extraction spreadsheet for each source.
        if isinstance(ingestion_data.iloc[i,2],str): #this is checking the column for anything and converting it to strings (it could be a NaN if nothing was in the column)
            # Pull just the key of the google spreadsheet out of the link that is listed in the source spreadsheet.
            binary_model_key = ingestion_data.iloc[i,2].split("/")[-2]
            # Create the full url to the model parameter extraction spreadsheet
            binary_model_url = create_url(binary_model_key)
            # Pull the relativant information about the model from the google spreadsheet.
            # This information gets put into a pandas dataframe for easy manipulation in python.
            binary_model_info = pd.read_csv(binary_model_url, \
                usecols = ['Name', 'Value','Error', 'Error type', 'Units'])
            # Get rid of any actual NaN values because SQL does not like or except that value when trying to
            # ingest model information.
            binary_model_info.replace(np.nan, "", regex=True)
            # Create the model array needed to use the ingest_model function. 
            # This should truly be whether a creation of an instance of the model class is put. Still currently
            # working and debugging the class code after moving it from ipython notebooks to regular script 
            # python. Will come back and fix that as soon as the model class is better situated.
            binary_model = binary_model_info.iloc[:,1].to_numpy()
            # Now try to ingest the source. There is a try/except block here for the exact same reasoning as for the
            # try/except block used above for ingesting sources.
            try:
                ingest_binary_model(binary_model, db_info_path)
                print("binary model ingested")
            except:
                print("binary model not ingested")
        # If there isn't actually a link to a model parameter extraction spreadsheet associated with the source
        # entry then just skip over to the next one and check if it has an entry.
        else:
            pass


