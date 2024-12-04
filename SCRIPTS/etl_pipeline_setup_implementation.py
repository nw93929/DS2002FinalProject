!rm -rf DS2002FinalProject
!git clone https://github.com/nw93929/DS2002FinalProject.git
%cd DS2002FinalProject

# Need to install necessary libraries
!pip install pandas openpyxl sqlalchemy pymysql google-cloud-storage

import pandas as pd

# Loading necessary data into data frames
outcomes = pd.read_excel('Data/WinningPartyByCounties.xlsx')
demographics2012 = pd.read_excel('Data/demographicsswingstates2012.xlsx')
demographics2016 = pd.read_excel('Data/demographicsswingstates2016.xlsx')
demographics2020 = pd.read_excel('Data/demographicswingstates2020.xlsx')

# Clean outcomes data
outcomes['state'] = outcomes['state'].str.capitalize()
outcomes['county_name'] = outcomes['county_name'].str.capitalize()
columns_to_drop = ['office', 'version', 'county_fips', 'mode']
outcomes_cleaned = outcomes.drop(columns=columns_to_drop)

# Validate the changes to outcomes
# print(outcomes_cleaned.head())

# keep only necessary columns for analysis
columns_to_keep = ['County Name', 'State Name', 'Males 18-24 %', 'Females 65+ %',
                   'White alone %', 'Black or African American alone %',
                   'less than $39,999 %', 'more than $200,000 %',
                   'Regular high school diploma %', "Master's degree %",
                   'In labor force %', 'Not in labor force %']

columns_to_keep_2016 = ['County Name', 'State Name', 'Males 18-24 %', 'Females 65+ %',
                   'White alone %', 'Black or African American alone %',
                   'less than $39,999 %', 'more than $200,000 %',
                   'Regular high school diploma %', "Master's degree %",
                   'In labor force Percentage %', 'Not in labor force Percentage %']

# Clean demographics data
demographics2012_cleaned = demographics2012[columns_to_keep]
demographics2016_cleaned = demographics2016[columns_to_keep_2016].rename(
    columns={
        "In labor force Percentage %": "In labor force %",
        "Not in labor force Percentage %": "Not in labor force %"
    }
)
demographics2020_cleaned = demographics2020[columns_to_keep]

# Display cleaned demographics data
# print(demographics2012_cleaned.head())
# print(demographics2016_cleaned.head())
# print(demographics2020_cleaned.head())

# Add the 'year' column using .loc
demographics2012_cleaned = demographics2012_cleaned.copy()
demographics2016_cleaned = demographics2016_cleaned.copy()
demographics2020_cleaned = demographics2020_cleaned.copy()

demographics2012_cleaned.loc[:, 'year'] = 2012
demographics2016_cleaned.loc[:, 'year'] = 2016
demographics2020_cleaned.loc[:, 'year'] = 2020

# Clean 'county_name' and 'state' columns for compatibility with outcomes
for df in [demographics2012_cleaned, demographics2016_cleaned, demographics2020_cleaned]:
    # Remove "County" suffix and ensure consistent capitalization
    df.loc[:, 'county_name'] = df['County Name'].str.replace(r'\s*County\s*$', '', regex=True).str.strip().str.title()
    df.loc[:, 'state'] = df['State Name'].str.strip().str.title()

# Merge datasets
merged_2012 = pd.merge(
    outcomes_cleaned[outcomes_cleaned['year'] == 2012],
    demographics2012_cleaned,
    on=['county_name', 'state'],
    how='inner'
)

merged_2016 = pd.merge(
    outcomes_cleaned[outcomes_cleaned['year'] == 2016],
    demographics2016_cleaned,
    on=['county_name', 'state'],
    how='inner'
)

merged_2020 = pd.merge(
    outcomes_cleaned[outcomes_cleaned['year'] == 2020],
    demographics2020_cleaned,
    on=['county_name', 'state'],
    how='inner'
)

# Save merged data
merged_2012.to_csv('merged_data_2012.csv', index=False)
merged_2016.to_csv('merged_data_2016.csv', index=False)
merged_2020.to_csv('merged_data_2020.csv', index=False)

# Verify the merged data
# print("Merged 2012 Data:")
# print(merged_2012.head())

# print("\nMerged 2016 Data:")
# print(merged_2016.head())

# print("\nMerged 2020 Data:")
# print(merged_2020.head())

# Combine datasets
merged_data = pd.concat([merged_2012, merged_2016, merged_2020], axis=0)

# Drop 'year_y' and rename 'year_x' to 'year'
if 'year_y' in merged_data.columns:
    merged_data = merged_data.drop(columns=['year_y'])
if 'year_x' in merged_data.columns:
    merged_data = merged_data.rename(columns={'year_x': 'year'})

# Save the cleaned merged data locally
merged_data.to_csv('merged_data.csv', index=False)

# Validating data
# print("Merged data saved as merged_data.csv")
# print(merged_data.head())

from sqlalchemy import create_engine, text

# Database connection details
db_username = 'root'
db_password = 'election-project-data'
public_ip = '35.199.15.59'
db_name = 'outcomes-demographics'

# Create a database engine
engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{public_ip}/{db_name}')

# Define a table creation query
create_table_query = """
CREATE TABLE outcomes_demographics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT,
    state VARCHAR(255),
    state_po VARCHAR(255),
    county_name VARCHAR(255),
    candidate VARCHAR(255),
    party VARCHAR(255),
    candidatevotes INT,
    totalvotes INT,
    state_name VARCHAR(255),
    males_18_24 FLOAT,
    females_65_plus FLOAT,
    white_alone FLOAT,
    black_or_african_american_alone FLOAT,
    less_than_39999 FLOAT,
    more_than_200000 FLOAT,
    regular_high_school_diploma FLOAT,
    masters_degree FLOAT,
    in_labor_force FLOAT,
    not_in_labor_force FLOAT
);
"""

# Execute the query to create the table
try:
    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        print("Table 'outcomes_demographics' created or already exists.")
except Exception as e:
    print(f"Error creating table: {e}")

# Load the CSV data
merged_data = pd.read_csv('merged_data.csv')

# Upload the data to the MySQL table
try:
    merged_data.to_sql('outcomes_demographics', con=engine, if_exists='replace', index=False)
    print("Data uploaded successfully to 'outcomes_demographics'.")
except Exception as e:
    print(f"Error uploading data: {e}")

# Connecting to MySQL database to validate outcomes_demographics table upload
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM outcomes_demographics;"))
        row_count = result.fetchone()[0]
        # column_count = len(result.keys())
        print(f"Number of rows in 'outcomes_demographics': {row_count}")
        # print(f"Number of columns in 'outcomes_demographics': {column_count}")
except Exception as e:
    print(f"Error verifying data: {e}")

# Validating Google Cloud bucket and connection
import os
from google.cloud import storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Data/ds-final-project-443519-ba17067a570e.json'

# Initialize the client
client = storage.Client()

# List buckets
buckets = list(client.list_buckets())
print("Buckets in the project:")
for bucket in buckets:
    print(bucket.name)

# uploading merged_data.csv to google cloud bucket
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

upload_to_gcs('ds-final-project', 'merged_data.csv', 'gs://ds-final-project/')
