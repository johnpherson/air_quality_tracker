import requests
from dotenv import load_dotenv
import os
import json
from datetime import datetime
import pandas as pd
import glob

# Load the dot_env env
load_dotenv()

# Assign Env variables
api_key = os.getenv("API_KEY") 
api_base_url = os.getenv("API_BASE_URL") 

# Create base url
base_url = f"https://api.openaq.org/v3/locations"

# Create the headers required
headers = {
    "X-API-Key": api_key
}

# Create a test request to make sure it works
response = requests.get(base_url, headers=headers)

# See if the auth works
if response.status_code == 200:
    print("Auth worked")
else:
    print(f"Error: {response.status_code}")

def get_location_aq(location_id):
    current_datetime = datetime.now()
    base_url = f"https://api.openaq.org/v3/locations/{location_id}"
    response = requests.get(base_url, headers=headers)
    if response.status_code == 200:
        json_data = response.json()
        #print(json.dumps(json_data, indent=4))    
        with open(f"location_{location_id}_{current_datetime.strftime('%Y%m%d%H%M%S')}.json", "w") as outfile:
            json.dump(json_data, outfile, indent=4)
    else:
        print(f"Error: {response.status_code}")



# Get the location data
location_id = 288
json_data = get_location_aq(location_id)

#find the most recent json file
list_of_files = glob.glob('location_*.json')
latest_file = max(list_of_files, key=os.path.getctime)

#Load the most recent JSON file into a dataframe
with open(latest_file, 'r') as f:
    json_data = json.load(f)
    df = pd.json_normalize(json_data)

# Flatten the JSON data
df_cleaned = pd.json_normalize(json_data, sep='_')




# Print the cleaned Dataframe
print(df_cleaned.head())






