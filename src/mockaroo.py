# Imports
import requests
import pandas as pd
import numpy as np
from io import BytesIO
from constants import API_KEY

def generate_mockaroo_data(schema_id, rows, api_key):
 
    """
    This function generates data using Mockaroo.
    It accepts a schema id, the number of rows and an API key. 
    It generates data by chunks having no more than 1000 rows 
    (to get around the restrictions for a free account), 
    concatenates results and returns a Pandas dataframe.
    """ 
  
    # a dataframe for collecting output data
    df_mockaroo_data = pd.DataFrame()

    # split number of rows into chunks, no more than 1000 rows each
    chunks = [1000]*(rows//1000) + [rows%1000]

    # generate data by chunks
    for chunk in chunks:

        # Put together an URL for the API call
        url = "https://api.mockaroo.com/api/" + schema_id + "?count=" + str(chunk) + '&key=' + API_KEY
        
        # Call API
        r = requests.get(url, allow_redirects=True)  # Request data
        print(url, r) # Track progress
        
        # Convert bytes data into a Pandas dataframe
        df = pd.read_csv(BytesIO(r.content)) 

        # append to the output dataframe
        df_mockaroo_data = pd.concat([df_mockaroo_data, df])

    
    return df_mockaroo_data.reset_index(drop=True)



if __name__ == "__main__":

    # example
    df = generate_mockaroo_data("4ad98600", 2025, API_KEY)
    
