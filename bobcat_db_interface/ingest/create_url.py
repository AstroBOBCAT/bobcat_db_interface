## This script is used to create the full url needed for having the information in the expected google 
## spreadsheet that is outputted into a csv file. All the function needs is the google spreadsheet key that can
## be found in the google spreadsheet link. Note that the link in the browser when you open a google
## spreadsheet will contain the key needed but it is not the correct url needed. Hence this function
## was created to make sure the url was the correct format needed.

##############
def create_url(key):

    '''Create url for a google spreadsheet such that it is in csv format given the spreadsheet key.
    
    Inputs:
        key - this is a long string of random letters and numbers found in all the links to the 
        google spreadsheet in question
        
    Outputs:
        url string
    '''

    # First we need to check that the key given is actually a string.
    # If it isn't a string we could change it into a string but it is probably better to raise an error to
    # make sure the user is aware of what is wanted for the function.
    if not isinstance(key, str):
        raise TypeError("key must be a string")

    # Concatenate the key with the needed strings. The last portion is what turns the google spreadsheet into
    # csv format which makes it very easy to read into a pandas dataframe in other functions.
    url = "https://docs.google.com/spreadsheet/ccc?key=" + key + "&output=csv"
    return url
#############