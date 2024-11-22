import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..")) # this seems to be the only one that actually works




import numpy as np
from BOBcat_utils import NED_name
from BOBcat_utils import NED_name_resolver
from BOBcat_utils import coord_converter
from BOBcat_utils import NED_z
from BOBcat_utils import coord_finder
from ingest_utils import ingest_source



#np.loadtxt("SDSS_dualAGN_survey.txt")
#data = np.genfromtxt("SDSS_dualAGN_survey.txt", dtype='unicode', usecols=(1))
#data = np.genfromtxt("objsearch_div.tbl", dtype=None)

#print(data)

#print(NED_name_resolver("SDSS " + data[3]))

#for i in range((len(data))):
#    name = data[i]
#    ned_name = NED_name_resolver(name)
#    ra, dec = (coord_finder(name))
#    ra_deg, dec_deg = (coord_converter(ra, dec))
#    redshift = NED_z(ra_deg, dec_deg)
#    source = [ned_name, ra_deg, dec_deg, redshift]
#    print(source)
    #try:
    #    ingest_source(source)
    #    print("source ingested")
    #except:
    #    print("source not ingested")


filename = "objsearch_div.csv"

file = open(filename)


with file as my_file:
    data_array = my_file.readlines()#.strip("\n")
file.close()

print(data_array[0].split(","))
print(len(data_array))

#data = [ []*(87) ]

data = [[0 for x in range(4)] for y in range(87)] 

print(len(data))


for i in range((len(data_array))):
    b = data_array[i].split(",")
    data[i][0] = b[2].split(">")[1].split("<")[0]
    #data[i][1].append(b[3])
    data[i][1] = b[3]
    data[i][2] = b[4]
    data[i][3] = b[5].strip("\n")

print(b)
print(b[2].split(">")[1].split("<")[0])


#data = np.genfromtxt("objsearch_div.csv", delimiter=",", dtype=None)
#print(data[0])

print(data[0])
