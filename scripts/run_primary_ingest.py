import argparse
from bobcat_db_interface.ingestion import ingest

if __name__ == "__main__":

     print("Testing testing all around town")

     # Set up command line parsing
     parser = argparse.ArgumentParser(
          prog="run_primary_ingest.py",
          description="This is the wrapper code that will be run to ingest the primary BOBcat data. It is intended to be run once for the first large ingest, and then again as needed (approximately once per month) for new candidates thereafter.",
     )
     parser.add_argument(
          "-o",
          "--official",
          help="This creates an official run of the ingestion. It's basically a safeguard so we don't run an unintentional ingest. To make the official ingest run you must supply this option and the case-sensitive passcode yes-i-really-want-to-do-this",
          required=False,
          nargs=1,
     )
     parser.add_argument(
          "-k",
          "--key",
          help="For testing, you can optionally input a manual key that differs from the one in your db_info file.",
          required=False,
          nargs=1,
     )
     myinputs = parser.parse_args() 


     
     if (myinputs.key):
          print("You input the optional key ",str(myinputs.key))
          print("Will run an actual ingestion with this fake input key.")
          # THIS ISNT WORKING YET.
          


     print("You put in the password "+myinputs.official[0])
     # Here's an example where we read one value. Here I didn't include
     # an "if" statement because this value was required.
     if (myinputs.official[0] == "yes-i-really-want-to-do-this"):
          print("Correct passcode! Will proceed with ingest.\n")
          ingest.ingest()
     else:
          print("*********\nERROR: WRONG PASSCODE, cannot complete your request for an ingest.\n********\n");
          exit()
