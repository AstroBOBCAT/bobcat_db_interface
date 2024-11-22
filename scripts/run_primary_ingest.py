from bobcat_db_interface.ingestion import ingest

if __name__ == "__main__":
     # add parsing from command line
     # ingest.py -key asdfjasgdh -path /Users/sbs/.bobcat/db_info.txt
     # parse user inputs and read user info from file
     # Run ingest function.
     
     # Set up command line parsing
     parser = argparse.ArgumentParser(
          prog="template.py",
          description="A description of your code goes here and comes out when people run the code with -h as an input.",
     )
     parser.add_argument(
          "-b",
          "--basic",
          help="Here's a basic input. I've set it to be required for the program to run. It needs one value to be provided.",
          required=True,
          nargs=1
     )
     parser.add_argument(
          "-o",
          "--option",
          help="This is an example of an option that requests an arbitrarily large list of values (for instance, might be used to get a list of files). I set this option to not be required for the program to run.",
          required=False,
          nargs="+",
     )
     parser.add_argument(
          "-v",
          "--verbose",
          help="Here's an example of a true/false input parameter that doesn't take in any actual values. In this case, I'm setting it to turn on lots more printing. That is, it is used to turn on DEBUG-level (general) logging.",
          required=False,
          action='store_true'
     )

     # Set a variable to house the input arguments. You can see below
     # how you call them and use the provided values.
     allmyinputs = parser.parse_args() 


     # Here's an example where we read one value. Here I didn't include
     # an "if" statement because this value was required.
     important_number = allmyinputs.basic

