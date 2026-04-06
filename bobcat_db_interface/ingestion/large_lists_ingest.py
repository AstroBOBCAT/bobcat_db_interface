#########
# Import the libraries, modules, and functions needed for the ingest function to work properly.
import pandas as pd #pandas dataframe that the csv file information gets read into for easy manipulation in python
import numpy as np #numpy
 
# Import the utilities made for BOBcat itself and the specific ingestion utilities made for this process.
from gw_utils import calc as gw_calc
from gw_utils import ned as ned

from bobcat_db_interface.communications import db_comms
from bobcat_db_interface.keys import db_info
from bobcat_db_interface.ingestion import ingest


""".

Large lists that need to be here (initial if you're working on adding
it, X when done and tested):

 - https://ui.adsabs.harvard.edu/abs/2009ApJ...705L..76W/abstract Wang et al double peaked OIII

 - 

[ADD MORE AS YOU IDENTIFY FIND THEM]

"""


# THIS IS THE VALUE / INDEX MAP FOR BINARY MODEL ELEMENTS
# I don't like doing it this way, but it's a quick fix and will
# make propagating any future changes to the database slightly
# easier so we have the mapping out front...
binary_model_imap = {
    "paper":0,
    "candidate_name":1,
    "eccentricity":2,
    "m1":3,
    "m2":4,
    "mtot":5,
    "mc":6,
    "mu":7,
    "q":8,
    "evid1_type":10,
    "evid1_note":11,
    "evid1_wavelength":12,
    "evid2_type":13,
    "evid2_note":14,
    "evid2_wavelength":15,
    "evid3_type":16,
    "evid3_note":17,
    "evid3_wavelength":18,
    "evid4_type":19,
    "evid4_note":20,
    "evid4_wavelength":21,
    "inclination":22,
    "semimajor_axis":23,
    "seperation":24,
    "period_epoch":25,
    "orb_freq":26,
    "orb_period":27,
    "summary":28,
    "caveats":29,
    "ext_pro":30,
}


############
"""

The bib codes for each subfunction use the psrrefs standard:

    Single Author Paper - first three letters of last name and the year.
        e.g. Lyne, A.G. 1984 would be lyn84 
    Two Authors - first letter of each name plus the year
        e.g. Ables, J. G. and Manchester, R. N. 1976 would be am76 
    Three Authors - first letter of each name plus the year
        e.g. Freire, P.C., Kramer, M. and Lyne, A.G. 2001 would be fkl01 
    Four Authors - first letter of each name plus the year
        e.g. Lorimer, Yates, Lyne and Gould 1995 would be lylg95 
    Five or More Authors - first letter the first three names, a +, then the year
        e.g. Kramer, Bell, Manchester, Lyne, Camilo, et al 2003 would be kbm+03 

If there are two different papers which have matching names, they can
be separated by appending an a/b/c designation to the end of the year.

"""
    

def ingest_wchPLUS09():
    """.

    Wang et al., ApJ Letters, 705, 76 (2009)
    
    Abstract:

    Double-peaked [O iii] profiles in active galactic nuclei (AGNs)
    may provide evidence for the existence of dual AGNs, but a good
    diagnostic for selecting them is currently lacking. Starting from
    ∼7000 active galaxies in Sloan Digital Sky Survey DR7, we assemble
    a sample of 87 type 2 AGNs with double-peaked [O iii] profiles.
    The nuclear obscuration in the type 2 AGNs allows us to determine
    redshifts of host galaxies through stellar absorption lines. We
    typically find that one peak is redshifted and another is
    blueshifted relative to the host galaxy. We find a strong
    correlation between the ratios of the shifts and the double peak
    fluxes. The correlation can be naturally explained by the
    Keplerian relation predicted by models of co-rotating dual
    AGNs. The current sample statistically favors that most of the [O
    iii] double-peaked sources are dual AGNs and disfavors other
    explanations, such as rotating disk and outflows. These dual AGNs
    have a separation distance at ∼1 kpc scale, showing an
    intermediate phase of merging systems. The appearance of dual AGNs
    is about ∼10−2 , impacting on the current observational deficit of
    binary supermassive black holes with a probability of ∼ 10−4
    (Boroson & Lauer).

    """


    # Here insert the common information for all targets.
    binary_model = {"https://ui.adsabs.harvard.edu/abs/2009ApJ...705L..76W/abstract", # paper
                    "", # candidate_name
                    "", # eccentricity
                    "", # m1
                    "", # m2
                    "", # mtot
                    "", # mc
                    "", # mu
                    "", # q
                    "Offset/double emission lines", # evid1_type
                    "Double-peaked broad lines", # evid1_note
                    "optical", # evid1_wavelength
                    "", # evid2_type
                    "", # evid2_note
                    "", # evid2_wavelength
                    "", # evid3_type
                    "", # evid3_note
                    "", # evid3_wavelength
                    "", # evid4_type
                    "", # evid4_note
                    "", # evid4_wavelength
                    "", # inclination
                    "", # semimajor_axis
                    "", # separation
                    "", # period_epoch
                    "", # orb_freq
                    "", # orb_period
                    "87 "type 2 AGN" selected from 7000 SDSS galaxies as having double-peaked [OIII] emission lines. For full sample on average, one peak is redshifted and one is blue-shifted; this is interpreted as dual AGN. Some sources noted to have tidal tails but this comment was not source-specific.", # summary
                    "Separations are estimated to be typically 1kpc from arguments about Kepler's laws and the relative velocities.", # caveats
                    "The publication notes that the mass ratio could be modelled as the ratio of line luminosities (equation 4), under a broad range of assumptions, e.g. if an equal Eddington rate is assumed for the SMBHBs, if they're far apart. They did not actually calculate and report this for the sources so it is not reflected in this BOBcat entry."} # ext_proj

    # Now read the paper's data file.

    # Now iterate per source, populating the data file. It's assumed we already know the NED name.

    # Now try to ingest the source.

    return





###############
def ingest_all():

    '''.

    Main ingestion script for papers representing large lists of sources.

    Each one is unique and handled by its own explicit script and data file(s).
    
    Inputs:
        N/A
    Outputs:
        statements telling you whether a source/model has been ingested into the database



    The problem is here I need somewhere to store the "model parameter
    details" for each large list that's secure. Or perhaps we can simply
    have an offline version for each model parameter detail for each large
    list. We don't necessarily need to use google for it. I think that
    will be the easiest way.

    When I'm done today I should undo the changes to db_info and to __init__.

    #### CAN DELETE BELOW ONCE I HAVE SOMETHING WORKING.
    ## Create the url to the Large Lists google master sheet tab (controlled by gid).
    #url = create_url(db_info['googlekey']+"&gid="+db_info['largelistgid'])
    #
    ## Pull the relativant information about the source from the google
    ## spreadsheet, this includes the paper link, the name of the
    ## source in NED, and a link to another google spreadsheet that
    ## contains the model parameter information.  This information gets
    ## put into a pandas dataframe for easy manipulation in python.
    #try:
    #    ingestion_data = pd.read_csv(url, usecols = ["Paper Link", "Brief Description",  "Paper data file", "Model Parameter Details"])
    #except Exception as err:
    #    print("Error: Could not read large list URL ",url)
    #    print(f"Reported error: {err}")
    #    exit()
'''
    return





if __name__ == "__main__":
    # add parsing from command line
    # ingest.py -key asdfjasgdh -path /Users/sbs/.bobcat/db_info.txt
    #

    
    # parse user inputs and read user info from file

    # Run ingest function.


    ingest()
    #ingest("1WU4c_FCEOMEmd1m_680qtqNR7rFIzNztT2GxZpX4dvk","/Users/sbs/.bobcat/db_info.txt")
