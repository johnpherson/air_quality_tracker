import requests
from dotenv import load_dotenv
import os
import json
from datetime import datetime
import pandas as pd
import glob
#from sqlalchemy import create_engine

# Load the dot_env env
load_dotenv()

# Assign Env variables
api_key = os.getenv("API_KEY") 
api_base_url = os.getenv("API_BASE_URL") 
# db_url = os.getenv("DATABASE_URL")  # Add your database URL in the .env file

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
        # Save JSON data to a file
        with open(f"location_{location_id}_{current_datetime.strftime('%Y%m%d%H%M%S')}.json", "w") as outfile:
            json.dump(json_data, outfile, indent=4)
    else:
        print(f"Error: {response.status_code}")

# Get the location data
location_id = 288
json_data = get_location_aq(location_id)

# Find the most recent JSON file
list_of_files = glob.glob('location_*.json')
latest_file = max(list_of_files, key=os.path.getctime)

# Load the most recent JSON file into a DataFrame
with open(latest_file, 'r') as f:
    json_data = json.load(f)
    df_meta = pd.json_normalize(json_data)
    df_results = pd.json_normalize(json_data['results'])

# Flatten the DataFrame
if 'results' in df_meta.columns:
    df_meta = df_meta.drop(columns=['results'])

# Print column names after flattening
print('Column names after flattening (meta):')
print(df_meta.columns)

print('Column names after flattening (results):')
print(df_results.columns)

# Print the first few rows of the DataFrame
print("First few rows of the DataFrame (meta):")
print(df_meta.head())

print("First few rows of the DataFrame (results):")
print(df_results.head())

# Perform cleaning operations (example: drop columns with all NaN values)
df_cleaned = df_results.dropna(axis=1, how='all')

# Print the cleaned DataFrame
print("Cleaned DataFrame:")
print(df_cleaned.head())

# Insert the cleaned DataFrame into a database
#engine = create_engine(db_url)
#df_cleaned.to_sql('air_quality_data', engine, if_exists='replace', index=False)