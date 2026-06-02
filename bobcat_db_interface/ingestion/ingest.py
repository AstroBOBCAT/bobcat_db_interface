#########

## This ingestion script is to be used for ingesting sources from a
## google spreadsheet. This was choosen as the offline verison on
## BOBcat that was created and used to collected candidates before
## BOBcat started and while BOBcat was in the beginning stages is in a
## google spreadsheet. However, this ingestion script could be used
## for any google spreadsheet that is setup in the correct way and
## that the key for is known.

## There should be another ingestion script that deals with pulling
## data for other databases so candidates from large surveys, such as
## CRTs, are easy to ingest into BOBcat without having to create an
## entry into a google spreadsheet for every single candidate in the
## surveys.

#########

from math import nan
from types import NoneType
import pandas as pd #pandas dataframe that the csv file information gets read into for easy manipulation in python
import numpy as np #numpy
import psycopg2
from logging import warning
from logging import info
import time
from astropy.coordinates import SkyCoord
from astropy.time import Time
import traceback
import urllib.error

from astroquery.ipac.ned import Ned
Ned.clear_cache()
# Import the utilities made for BOBcat itself and the specific ingestion utilities made for this process.
from gw_utils import astrodb as astrodb
from gw_utils import calc

from bobcat_db_interface.communications import db_comms
from bobcat_db_interface.keys import db_info

## This is used to create the full url needed for having the information in the expected google 
## spreadsheet that is outputted into a csv file. All the function needs is the google spreadsheet key that can
## be found in the google spreadsheet link. Note that the link in the browser when you open a google
## spreadsheet will contain the key needed but it is not the correct url needed. Hence this function
## was created to make sure the url was the correct format needed.

##############
# SARAH! This key needs to stay key because it's referenced by both
# the master and parameter extraction functions.
##############
def create_url(key):

    '''.

    Create url for a google spreadsheet such that it is in csv format
    given the spreadsheet key.
    
    Inputs:
        key - this is a long string of random letters and numbers found in all the links to the 
        google spreadsheet in question
        
    Outputs:
        url string

    '''

    # First we need to check that the key given is actually a string.
    # If it isn't a string we could change it into a string but it is
    # probably better to raise an error to make sure the user is aware
    # of what is wanted for the function.
    if not isinstance(key, str):
        raise TypeError("key must be a string")

    # Concatenate the key with the needed strings. The last portion is
    # what turns the google spreadsheet into csv format which makes it
    # very easy to read into a pandas dataframe in other functions.
    url = "https://docs.google.com/spreadsheet/ccc?key=" + key + "&output=csv"
    return url

def ads_to_scix(link):
    """
    Convert ADS links to SciX links.
    Leaves other links unchanged.
    Inputs:
        link - string of the link to convert
    Outputs:
        new_link - string of the converted link
    """
    if "ui.adsabs" in str(link):
        key = str(link).split("/")[-2]
        new_link = "https://scixplorer.org/abs/" + key
        return new_link
    else:
        return str(link), str(key)

#############

def check_binary_model(model, url):
    """
    Check if everything is the right dtype, and if so, check if it fits in the ranges of values that are expected for the binary model parameters.
    Inputs:
        model - pandas dataframe of the model
        url - string of the url to the google spreadsheet
        
    Outputs:
        summary_list_warnings - list of strings of warnings that are raised if the model is not in the expected ranges
        weirdness - A boolean that is True if any part of the model is not in the expected ranges
    """
    
    str_i = [0,1,-1,-2,-3] # The indices in the table where we expect strings
    float_i = [*range(2,9),*range(21,27)] # The indices in the table where we expect floats
    summary_list_warnings = []

    # Making sure the string data is in the right format
    for i in str_i:
        if type(model.iloc[i]) != str:
            try:
                model.iloc[i] = str(model.iloc[i])
            except:
                raise TypeError(f"{model.iloc[i]} must be a string. {type(model.iloc[i])} given. NED Name: {model.iloc[1]}, url: {url}")
    if len(model.iloc[-2]) > 750: # TODO: Determine final max length for notes
        raise ValueError(f"Caveats must be less than 750 characters. {len(model.iloc[-2])} characters given. NED Name: {model.iloc[1]}, url: {url}")
    if len(model.iloc[-3]) > 750: # TODO: Determine final max length for notes
        raise ValueError(f"Summary must be less than 750 characters. {len(model.iloc[-3])} characters given. NED Name: {model.iloc[1]}, url: {url}")
            
    # Check if the values are in the right range and are all floats
    for i in float_i:
        if type(model.iloc[i]) != float:
            try:
                model.iloc[i] = float(model.iloc[i])
            except:
                raise TypeError(f"{model.iloc[i]} must be a float. {type(model.iloc[i])} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[2] < 0 or model.iloc[2] > 1:
        raise ValueError(f"Eccentricity must be between 0 and 1. {model.iloc[2]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[3] < 4 or model.iloc[3] > 12:
        raise ValueError(f"m1 must be between 4 and 12. {model.iloc[3]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[4] < 4 or model.iloc[4] > 12:
        raise ValueError(f"m2 must be between 4 and 12. {model.iloc[4]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[5] < 4 or model.iloc[5] > 12:
        raise ValueError(f"mtot must be between 4 and 12. {model.iloc[5]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[6] < 4 or model.iloc[6] > 12:
        raise ValueError(f"mc must be between 4 and 12. {model.iloc[6]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[7] < 4 or model.iloc[7] > 12:
        raise ValueError(f"mu must be between 4 and 12. {model.iloc[7]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[8] <= 0 or model.iloc[8] > 1:
        raise ValueError(f"q must be between 0 and 1. {model.iloc[8]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[21] < 0 or model.iloc[21] > 90:
        raise ValueError(f"Inclination must be between 0 and 90. {model.iloc[21]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[22] < 0 or model.iloc[22] > 1000:
        raise ValueError(f"Semimajor axis must be between 0 and 1000. {model.iloc[22]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[23] < 0 or model.iloc[23] > 1000:
        raise ValueError(f"Seperation must be between 0 and 1000. {model.iloc[23]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[24] < 40000 or model.iloc[24] > Time.now().mjd:
        raise ValueError(f"Period epoch must be between 40000 and current date {Time.now().mjd} MJD. {model.iloc[24]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[25] < 0:
        raise ValueError(f"Orb freq must be greater than 0. {model.iloc[25]} given. NED Name: {model.iloc[1]}, url: {url}")
    if model.iloc[26] < 0:
        raise ValueError(f"Orb period must be greater than 0. {model.iloc[26]} given. NED Name: {model.iloc[1]}, url: {url}")
    
    # Check that the values make sense. Won't throw an error for these; could be intentional weirdness.
    # Checks that a value is given for each param, and that an expected value can be calculated, and then checks whether they are within 0.1% of each other.
    weirdness = False # A flag to add this model to odd_params. False by default.
    if not pd.isnull(model.iloc[5]) and not pd.isnull(calc.Mtot_calc(model.iloc[3], model.iloc[4])) and abs(model.iloc[5] - calc.Mtot_calc(model.iloc[3], model.iloc[4])) > 0.001 * calc.Mtot_calc(model.iloc[3], model.iloc[4]):
        warning(f"Mtot does not match M1 + M2. \nMtot: {model.iloc[5]} \nM1 + M2: {calc.Mtot_calc(model.iloc[3], model.iloc[4])} \nNED Name: {model.iloc[1]}, url: {url}")
        summary_list_warnings.append(f"\nBinary model for {model.iloc[1]} had Mtot != M1 + M2. url: {url}")
        weirdness = True
    if not pd.isnull(model.iloc[6]) and not pd.isnull(calc.Mc_calc(model.iloc[3], model.iloc[4])) and abs(model.iloc[6] - calc.Mc_calc(model.iloc[3], model.iloc[4])) > 0.001 * calc.Mc_calc(model.iloc[3], model.iloc[4]):
        warning(f"Mc value does not match given M1, M2. \nGiven Mc: {model.iloc[5]} \nCalculated Mc: {calc.Mc_calc(model.iloc[3], model.iloc[4])} \nNED Name: {model.iloc[1]}, url: {url}")
        summary_list_warnings.append(f"\nBinary model for {model.iloc[1]} had Mc inconsistent with M1 and M2. url: {url}")
        weirdness = True
    if not pd.isnull(model.iloc[7]) and not pd.isnull(calc.mu_calc(model.iloc[3], model.iloc[4])) and abs(model.iloc[7] - calc.mu_calc(model.iloc[3], model.iloc[4])) > 0.001 * calc.mu_calc(model.iloc[3], model.iloc[4]):
        warning(f"mu value does not match given M1, M2. \nGiven mu: {model.iloc[5]} \nCalculated mu: {calc.Mc_calc(model.iloc[3], model.iloc[4])} \nNED Name: {model.iloc[1]}, url: {url}")
        summary_list_warnings.append(f"\nBinary model for {model.iloc[1]} had mu inconsistent with M1 and M2. url: {url}")
        weirdness = True
    if not pd.isnull(model.iloc[8]) and not pd.isnull(calc.q_calc(model.iloc[3], model.iloc[4])) and abs(model.iloc[8] - calc.q_calc(model.iloc[3], model.iloc[4])) > 0.001 * calc.q_calc(model.iloc[3], model.iloc[4]):
        warning(f"q value does not match given M1, M2. \nGiven q: {model.iloc[8]} \nCalculated q: {calc.q_calc(model.iloc[3], model.iloc[4])} \nNED Name: {model.iloc[1]}, url: {url}")
        summary_list_warnings.append(f"\nBinary model for {model.iloc[1]} had q inconsistent with M1 and M2. url: {url}")
        weirdness = True
    if model.iloc[2] == 0 and abs(model.iloc[22] - model.iloc[23]) > 0.001 * model.iloc[23]:
        warning(f"Orbit doesn't make sense for {model.iloc[1]}. e = 0 but a != sep.\nGiven a: {model.iloc[22]}\n Given sep: {model.iloc[23]} \nurl: {url}")
        summary_list_warnings.append(f"Conflicting orbital parameters for {model.iloc[1]}: circular orbit, but semi-major axis != separation. url: {url}")
        weirdness = True
    if not pd.isnull(model.iloc[25]) and abs(model.iloc[25] - calc.freq_calc(f_orb = model.iloc[26])[1]) > 0.001 * calc.freq_calc(f_orb = model.iloc[26])[1]:
        warning(f"Orbital period inconsistent with orbital frequency for {model.iloc[1]}, url: {url}\nGiven frequency: {model.iloc[25]}\nCalculated frequency: {calc.freq_calc(f_orb = model.iloc[26])[1]}")
        summary_list_warnings.append(f"Conflicting orbital parameters for {model.iloc[1]}: orbital period does not match orbital frequency. url: {url}")
        weirdness = True
    return summary_list_warnings, weirdness

def value_filling(model, summary_list_value_filling, summary_list_warnings, url):
    '''
    Fills in missing values in the model array.
    Inputs:
        - array containing the candidate parameters
        - list to append success messages to
        - list to append warning messages to
        - url of the model, for easy access
    Outputs:
        - value filled model
        - a boolen which is used to make a final cound of value filled models in the summary
    '''
    fills = False

    # Mass values
    try:
        masses = calc.mass_val_calc(m1 = float(model.iloc[3]), m2 = float(model.iloc[4]), Mtot = float(model.iloc[5]), q = float(model.iloc[8]), Mc = float(model.iloc[6]), mu = float(model.iloc[7]))
    except Exception as e:
        #raise e
        masses = False
        warning(f"Unable to calculate mass values for {model.iloc[1]}, url: {url}")
        summary_list_warnings.append(f"Unable to calculate mass values for {model.iloc[1]}, url: {url}")
    # Frequency parameters
    try:
        freq_params = calc.freq_calc(T = float(model.iloc[26]), f_orb = float(model.iloc[25]))
        info(f"Calculated freq params for {model.iloc[1]}: {freq_params}")
    except Exception as e:
        #raise e
        freq_params = False
        warning(f"Unable to calculate frequency values for {model.iloc[1]}  , url: {url}")
        summary_list_warnings.append(f"Unable to calculate frequency and period for {model.iloc[1]}, url: {url}")
    
    filled_model = model
    mass_indices = [3, 4, 5, 6, 7, 8]
    freq_indices = [25, 26]
    if masses:
        summary_list_value_filling.append(f"Mass values filled for {model.iloc[1]}:")
        for i in mass_indices:
            if pd.isnull(filled_model.iloc[i]):
                filled_model.iloc[i] = masses[i-3]
                summary_list_value_filling.append(f"Index {i} assigned value {filled_model.iloc[i]}") 
                fills = True
                # TODO: Make it so this says explicitly the name of the parameter being filled rather than just the index.
    if freq_params:
        summary_list_value_filling.append(f"Frequency values filled for {model.iloc[1]}:")
        for i in freq_indices:
            if pd.isnull(filled_model.iloc[i]):
                filled_model.iloc[i] = freq_params[i-25]
                summary_list_value_filling.append(f"Index {i} assigned value {filled_model.iloc[i]}")
                fills = True

    return filled_model, fills

################
def ingest_candidate(candidate):

    ''' Ingests a single candidate into a predefined database.

    Inputs:
        array containing the candidate parameters
    Outputs:
        NONE
    '''
    info("in ingest_candidate")

    cur, conn = db_comms.db_connect()

    info("connected to the database")

    # Ingest the model into the database.
#    cur.execute("INSERT INTO candidate(\
#        name, \
#        ra_deg, \
#        dec_deg,\
#        redshift, \
#        obs_type_done) \
#        VALUES (%s,%s,%s,%s,%s);", candidate)
    cur.execute(
            """
            INSERT INTO candidate (
                name, ra_deg, dec_deg, redshift, obs_type_done
            ) VALUES (%s, %s, %s, %s, %s);
            """,
            candidate
        )
    conn.commit() #make sure to actually commit the SQL command to the database
    # Always make sure to close the connection to the database 
    # (much like you should always close a file when done with it).
    conn.close()
###############



################
def ingest_binary_model(binary_model):

    ''' Ingests a single binary model into a predefined database.

    Inputs:
        array containing the binary model class parameters
    Outputs:
        NONE - currently, will fix to show whether or not it successfully ingests the model
    '''
    info("in ingest_binary_model")

    cur, conn = db_comms.db_connect()

    # Ingest the model into the database.
    cur.execute(
    """
    INSERT INTO binary_model (
        paper,
        candidate_name,
        eccentricity,
        m1,
        m2,
        mtot,
        mc,
        mu,
        q,
        evid1_type,
        evid1_note,
        evid1_wavelength,
        evid2_type,
        evid2_note,
        evid2_wavelength,
        evid3_type,
        evid3_note,
        evid3_wavelength,
        evid4_type,
        evid4_note,
        evid4_wavelength,
        inclination,
        semimajor_axis,
        seperation,
        period_epoch,
        orb_freq,
        orb_period,
        summary,
        caveats,
        ext_proj
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, binary_model)

    conn.commit() #make sure to actually commit the SQL command to the database
    # Always make sure to close the connection to the database 
    # (much like you should always close a file when done with it).
    conn.close()
###############

###############
def ingest():

    '''.

    Ingestion of sources and models into database starting from a
    specific google spreadsheet setup.
    
    Inputs:
        N/A
    Outputs:
        statements telling you whether a source/model has been ingested into the database

    '''

    start_time = time.time()

    # Create the url to the google spreadsheet that contains the
    # source information and a possible link to a model parameter
    # extraction google spreadsheet from the key string value given
    # for using the function.
    url = create_url(db_info['googlekey'])
    # Pull the relativant information about the source from the google
    # spreadsheet, this includes the paper link, the name of the
    # source in NED, and a link to another google spreadsheet that
    # contains the model parameter information.  This information gets
    # put into a pandas dataframe for easy manipulation in python.

    summary_list_warnings = ["\n==========WARNINGS=========="]
    summary_list_value_filling = ["\n==========VALUE FILLING=========="]


    ingestion_data = pd.read_csv(url, usecols = ["Paper Link", "Candidate Name",  "NED Name", "Model Parameter Details"])

    failed_redshift = 0
    value_fills = 0
    name_changes = 0
    odd_params = 0
    
    ned_names = []
    candidate_names = []
    candidates = []
    models = []

    # Go through all the different sources from the spreadsheet.
    print("Getting NED Names...")
    for i in range((len(ingestion_data))):
        # Set the ned_name variable as the information from the NED Name column.
        ned_name = ingestion_data.iloc[i,2]
        #print(ned_name)
        # If there is a ned_name given (note that it is possible some
        # candidates don't have this so think about what would need to
        # be done to account for that), get the j2000 ra and dec of
        # the source in degrees, as well as the redshift. Should
        # probably put in / use the NED name resolver function in
        # BOBcat utils at this point as well, will come back and
        # figure out exactly where in the script it should be added.
#        if ned_name:
            # Set the ra_deg and dec_deg variables to the j2000 ra and
            # dec positions given in NED for the source.

        ned_names.append(ned_name)
        candidate_names.append(ingestion_data.iloc[i,1])        
    ned_names = list(set(ned_names))
    candidate_names = list(set(candidate_names))

    print("Retrieving binary models...")

    # Convert ADS links to Scix links
    for i in range(len(ingestion_data)):
        ingestion_data.iloc[i, 0], bibcodes = ads_to_scix(ingestion_data.iloc[i, 0])
        # TODO: Store bibcodes in database

    keys = []
    for i in range(len(ingestion_data)):
        if isinstance(ingestion_data.iloc[i,3],str): #this is checking the column for anything and converting it to strings (it could be a NaN if nothing was in the column)
            # Pull just the key of the google spreadsheet out of the link that is listed in the source spreadsheet.
            binary_model_key = ingestion_data.iloc[i,3].split("/")[5]
            #print("DEBUG: Primary model key "+binary_model_key)
            # Create the full url to the model parameter extraction spreadsheet
            binary_model_url = create_url(binary_model_key)
            if binary_model_key not in keys:
                keys.append(binary_model_key)
            else:
                raise ValueError(f"Binary model url {binary_model_url} already exists in the list of urls (duplicate sheet)!")
            #print("DEBUG: "+binary_model_url)
            # Pull the relativant information about the model from the google spreadsheet.
            # This information gets put into a pandas dataframe for easy manipulation in python.
            try:
                binary_model_info = pd.read_csv(binary_model_url, \
                    usecols = ['Name', 'Value'])
            except Exception as e:
                raise SystemError(f"Unable to connect to spreadsheet {binary_model_url}\nSpecific error message: {e}")
                # Get rid of any actual NaN values because SQL does not like or except that value when trying to
                # ingest model information.
                #binary_model_info.replace(np.nan, "", regex=True)

                # Create the model array needed to use the ingest_model function. 
                # This should truly be whether a creation of an instance of the model class is put. Still currently
                # working and debugging the class code after moving it from ipython notebooks to regular script 
                # python. Will come back and fix that as soon as the model class is better situated.
        else:
            warning(f"\n!!! No parameter url in the data entry list for {ingestion_data.iloc[i,2]} !!!")

        binary_model = binary_model_info.iloc[:30,1].astype(object)
        if binary_model.iloc[1] != ingestion_data.iloc[i,2]:
            warning(f"""
            ================================================================================================================================================================
            WARNING! Candidate NED name and model name do not match! Setting model name {binary_model.iloc[1]} to candidate NED name {ingestion_data.iloc[i,2]}.
            ================================================================================================================================================================
            """)
            old_name = binary_model.iloc[1]
            binary_model.iloc[1] = ingestion_data.iloc[i,2]
            summary_list_warnings.append("Set model name "+str(old_name)+" to candidate NED name "+str(ingestion_data.iloc[i,2])+".")
            name_changes += 1
    
        binary_model, fills = value_filling(binary_model, summary_list_value_filling, summary_list_warnings, binary_model_url)
        if fills:
            value_fills += 1

        value_warnings, weirdness = check_binary_model(binary_model, binary_model_url)
        summary_list_warnings.extend(value_warnings)
        if weirdness:
            odd_params += 1
        models.append(binary_model)
    models = pd.DataFrame(models)
    print(models)
    print("Getting other candidate parameters... This may take a while.")
    ned_retrieval_times = []
    for i in range(len(ned_names)):
        #candidate_name = ingestion_data.iloc[i,1]
        ned_start_time = time.time()
        try:
            ra, dec = (astrodb.coord_finder(ned_names[i]))
        except Exception as err:
            raise RuntimeError(f"Could not find coordinates for candidate {ned_names[i]}.")
        end_time = time.time()
        ned_retrieval_times.append(end_time-start_time)
        # Convert sky coords to degrees.
        coords = SkyCoord(ra,dec)
        ra_deg = float(coords.ra.degree)
        dec_deg = float(coords.dec.degree)
        
        info("Coordinates found for object {}: {}, {}".format(ned_names[i], ra_deg, dec_deg))
        # Set redshift variable to the redshift given in NED for the source.
        try:
            redshift, name_change = astrodb.redshift(ned_names[i])
            if name_change:
                info(f"Updated NED name for object {ned_names[i]} to {name_change} due to more complete NED entry.")
                # Change name in models to name_change wherever it occurs
                models.loc[models[1] == ned_names[i], 1] = name_change
                ned_names[i] = name_change
                candidate_names[i] = name_change
            info("Redshift found for object " + ned_names[i])
        except Exception as err:
            warning(f"Redshift not found for object {ned_names[i]}. Message from redshift query: {err}")
            summary_list_warnings.append("Redshift not found for object "+str(ned_names[i])+".")
            redshift = None
            failed_redshift += 1
        ned_end_time = time.time()
        ned_retrieval_times.append(ned_end_time-ned_start_time)
        obs_type_done = [] # TODO: What is the point of this?

        #candidate = [candidate_name, ra_deg, dec_deg, redshift, obs_type_done]
        candidates.append([ned_names[i], ra_deg, dec_deg, redshift, obs_type_done])

    for source in candidates:
        try:
            ingest_candidate(source)
            info("Candidate ingested: "+str(source[0]))
        except:
            raise SystemError("candidate not ingested: "+str(source[0]))
    for _, model in models.iterrows():
        try:
            ingest_binary_model(model)
        except:
            raise SystemError("Binary model not ingested: "+str(model.iloc[1]))
        info("Binary model ingested: "+str(model.iloc[1]))

    # Summary of ingestion.

    for message in summary_list_value_filling:
        print(message)
    for message in summary_list_warnings:
        print(message)
    
    end_time = time.time()
    runtime = end_time - start_time
    print(f"""
    ==========SUMMARY==========
    Successfully ingested {len(candidates)} candidates.
    Successfully ingested {len(models)} binary models.
    Failed to find redshift for {failed_redshift} sources.
    Had to change {name_changes} names.
    Successfully filled some values for {value_fills} models ({len(models) - value_fills} failed).
    Found {odd_params} models with inconsistent/suspicious parameters.
    This all took {runtime} seconds.
    Average NED data retrieval time: {np.mean(ned_retrieval_times)} seconds. Max of {np.max(ned_retrieval_times)} seconds.
    """)






if __name__ == "__main__":
    # add parsing from command line
    # ingest.py -key asdfjasgdh -path /Users/sbs/.bobcat/db_info.txt
    #

    
    # parse user inputs and read user info from file

    # Run ingest function.

    print("DEBUG: Started the program. About to run ingest().")
    ingest()
    #ingest("1WU4c_FCEOMEmd1m_680qtqNR7rFIzNztT2GxZpX4dvk","/Users/sbs/.bobcat/db_info.txt")
