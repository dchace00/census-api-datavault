import pandas as pd
import requests
from sqlalchemy import create_engine, types
from datetime import datetime
import logging
import os
import time

# Determine the current working directory
script_dir = os.getcwd()
log_filename = os.path.join(script_dir, 'api-load.log')

# Setup logging
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.propagate = False

# Log the log file path for confirmation
logger.info(f"Log file path: {log_filename}")

# Create the log file if it doesn't exist
if not os.path.exists(log_filename):
    with open(log_filename, 'w') as file:
        pass
    logger.info("Log file created")

# Initial log message to confirm the script is running
logger.info("Starting script...")

# Database IP/connection details
DB_IP = "postgresql+psycopg2://postgres:Blubberboil@localhost/sandbox"

# Define elements of the URL
DATASET = "acs/acs1"
TABLE_NAME = '_'.join(DATASET.split('/'))

# Census group and numeric columns
group_numeric_columns = {
    "B01001": [
    "B01001_028E", "B01001_024E", "B01001_004E", "B01001_017E", "B01001_002E", 
    "B01001_010E", "B01001_049E", "B01001_036E", "B01001_022E", "B01001_045E", 
    "B01001_047E", "B01001_030E", "B01001_044E", "B01001_012E", "B01001_001E", 
    "B01001_043E", "B01001_003E", "B01001_031E", "B01001_009E", "B01001_037E", 
    "B01001_035E", "B01001_025E", "B01001_041E", "B01001_013E", "B01001_038E", 
    "B01001_039E", "B01001_006E", "B01001_007E", "B01001_023E", "B01001_046E", 
    "B01001_005E", "B01001_008E", "B01001_034E", "B01001_026E", "B01001_016E", 
    "B01001_032E", "B01001_018E", "B01001_019E", "B01001_027E", "B01001_033E", 
    "B01001_042E", "B01001_048E", "B01001_029E", "B01001_014E", "B01001_040E", 
    "B01001_011E", "B01001_020E", "B01001_015E", "B01001_021E"
    ],
    "S0101": [ 
    "S0101_C01_029E", "S0101_C03_038E", "S0101_C03_026E", "S0101_C01_025E", "S0101_C04_032E", 
    "S0101_C04_009E", "S0101_C05_033E", "S0101_C05_013E", "S0101_C01_015E", "S0101_C05_037E", 
    "S0101_C06_016E", "S0101_C01_037E", "S0101_C03_034E", "S0101_C04_035E", "S0101_C06_007E", 
    "S0101_C04_008E", "S0101_C03_017E", "S0101_C01_002E", "S0101_C02_022E", "S0101_C05_015E", 
    "S0101_C04_020E", "S0101_C06_033E", "S0101_C04_011E", "S0101_C06_027E", "S0101_C04_010E", 
    "S0101_C02_034E", "S0101_C06_002E", "S0101_C03_005E", "S0101_C04_030E", "S0101_C01_036E", 
    "S0101_C04_005E", "S0101_C03_033E", "S0101_C02_019E", "S0101_C06_015E", "S0101_C03_035E", 
    "S0101_C04_033E", "S0101_C06_034E", "S0101_C04_007E", "S0101_C05_034E", "S0101_C03_013E", 
    "S0101_C05_018E", "S0101_C02_029E", "S0101_C05_010E", "S0101_C04_006E", "S0101_C03_007E", 
    "S0101_C04_036E", "S0101_C06_025E", "S0101_C06_008E", "S0101_C06_017E", "S0101_C02_038E", 
    "S0101_C02_020E", "S0101_C02_004E", "S0101_C01_001E", "S0101_C06_003E", "S0101_C04_026E", 
    "S0101_C05_014E", "S0101_C03_036E", "S0101_C01_012E", "S0101_C03_031E", "S0101_C03_027E", 
    "S0101_C02_030E", "S0101_C06_006E", "S0101_C02_008E", "S0101_C05_007E", "S0101_C05_028E", 
    "S0101_C05_002E", "S0101_C04_029E", "S0101_C01_013E", "S0101_C06_022E", "S0101_C04_031E", 
    "S0101_C01_032E", "S0101_C06_018E", "S0101_C02_018E", "S0101_C04_002E", "S0101_C04_025E", 
    "S0101_C05_027E", "S0101_C01_009E", "S0101_C04_015E", "S0101_C02_024E", "S0101_C06_031E", 
    "S0101_C02_015E", "S0101_C02_021E", "S0101_C04_003E", "S0101_C03_001E", "S0101_C02_017E", 
    "S0101_C01_031E", "S0101_C03_030E", "S0101_C04_021E", "S0101_C04_027E", "S0101_C05_003E", 
    "S0101_C02_009E", "S0101_C01_016E", "S0101_C04_001E", "S0101_C06_036E", "S0101_C02_013E", 
    "S0101_C01_030E", "S0101_C02_035E", "S0101_C02_027E", "S0101_C04_004E", "S0101_C06_035E", 
    "S0101_C06_037E", "S0101_C02_033E", "S0101_C04_038E", "S0101_C06_004E", "S0101_C01_010E", 
    "S0101_C03_019E", "S0101_C03_021E", "S0101_C03_002E", "S0101_C01_033E", "S0101_C06_005E", 
    "S0101_C05_012E", "S0101_C01_034E", "S0101_C03_011E", "S0101_C06_029E", "S0101_C02_025E", 
    "S0101_C03_028E", "S0101_C05_024E", "S0101_C04_013E", "S0101_C01_023E", "S0101_C01_027E", 
    "S0101_C03_037E", "S0101_C06_032E", "S0101_C03_014E", "S0101_C01_008E", "S0101_C01_014E", 
    "S0101_C02_031E", "S0101_C03_004E", "S0101_C06_012E", "S0101_C03_006E", "S0101_C02_023E", 
    "S0101_C05_022E", "S0101_C01_018E", "S0101_C02_010E", "S0101_C01_024E", "S0101_C01_011E", 
    "S0101_C02_006E", "S0101_C05_026E", "S0101_C05_030E", "S0101_C01_007E", "S0101_C06_013E", 
    "S0101_C03_016E", "S0101_C05_036E", "S0101_C05_006E", "S0101_C01_038E", "S0101_C05_031E", 
    "S0101_C03_025E", "S0101_C05_021E", "S0101_C04_018E", "S0101_C02_011E", "S0101_C04_024E", 
    "S0101_C06_020E", "S0101_C02_028E", "S0101_C04_034E", "S0101_C02_007E", "S0101_C05_008E", 
    "S0101_C02_036E", "S0101_C04_037E", "S0101_C01_022E", "S0101_C01_026E", "S0101_C06_011E", 
    "S0101_C03_012E", "S0101_C06_009E", "S0101_C03_018E", "S0101_C02_003E", "S0101_C03_029E", 
    "S0101_C04_017E", "S0101_C01_003E", "S0101_C05_004E", "S0101_C03_022E", "S0101_C05_032E", 
    "S0101_C05_017E", "S0101_C06_026E", "S0101_C05_011E", "S0101_C03_020E", "S0101_C03_024E", 
    "S0101_C05_025E", "S0101_C04_016E", "S0101_C05_009E", "S0101_C06_010E", "S0101_C02_032E", 
    "S0101_C01_017E", "S0101_C04_012E", "S0101_C04_028E", "S0101_C04_022E", "S0101_C01_005E", 
    "S0101_C03_015E", "S0101_C06_028E", "S0101_C01_021E", "S0101_C06_024E", "S0101_C05_023E", 
    "S0101_C05_038E", "S0101_C05_005E", "S0101_C03_032E", "S0101_C05_016E", "S0101_C06_019E", 
    "S0101_C04_023E", "S0101_C06_014E", "S0101_C03_003E", "S0101_C02_026E", "S0101_C06_021E", 
    "S0101_C05_001E", "S0101_C03_010E", "S0101_C01_028E", "S0101_C05_029E", "S0101_C03_008E", 
    "S0101_C01_020E", "S0101_C01_004E", "S0101_C02_016E", "S0101_C02_002E", "S0101_C06_023E", 
    "S0101_C04_019E", "S0101_C02_014E", "S0101_C02_001E", "S0101_C02_012E", "S0101_C04_014E", 
    "S0101_C01_019E", "S0101_C03_009E", "S0101_C03_023E", "S0101_C06_001E", "S0101_C02_005E", 
    "S0101_C02_037E", "S0101_C05_019E", "S0101_C01_035E", "S0101_C05_035E", "S0101_C06_030E", 
    "S0101_C01_006E", "S0101_C06_038E", "S0101_C05_020E" 
    ],
    "DP05": [ 
    "DP05_0028PE", "DP05_0034E", "DP05_0070E", "DP05_0082PE", "DP05_0081E", 
    "DP05_0034PE", "DP05_0030E", "DP05_0062PE", "DP05_0072E", "DP05_0085PE", 
    "DP05_0026E", "DP05_0017E", "DP05_0053PE", "DP05_0059PE", "DP05_0079PE", 
    "DP05_0024E", "DP05_0012E", "DP05_0083PE", "DP05_0041E", "DP05_0084PE", 
    "DP05_0087PE", "DP05_0039E", "DP05_0084E", "DP05_0065E", "DP05_0056E", 
    "DP05_0028E", "DP05_0003PE", "DP05_0057E", "DP05_0046E", "DP05_0003E", 
    "DP05_0076E", "DP05_0048PE", "DP05_0040E", "DP05_0088PE", "DP05_0065PE", 
    "DP05_0076PE", "DP05_0094E", "DP05_0053E", "DP05_0047PE", "DP05_0029E", 
    "DP05_0019PE", "DP05_0029PE", "DP05_0012PE", "DP05_0070PE", "DP05_0018E", 
    "DP05_0060PE", "DP05_0087E", "DP05_0033E", "DP05_0017PE", "DP05_0015PE", 
    "DP05_0054E", "DP05_0050PE", "DP05_0036E", "DP05_0005E", "DP05_0093E", 
    "DP05_0055E", "DP05_0042PE", "DP05_0088E", "DP05_0008E", "DP05_0080E", 
    "DP05_0025PE", "DP05_0010E", "DP05_0066E", "DP05_0037PE", "DP05_0052E", 
    "DP05_0069E", "DP05_0071PE", "DP05_0004PE", "DP05_0049E", "DP05_0033PE", 
    "DP05_0061E", "DP05_0073PE", "DP05_0043E", "DP05_0036PE", "DP05_0038PE", 
    "DP05_0044E", "DP05_0044PE", "DP05_0037E", "DP05_0002PE", "DP05_0064E", 
    "DP05_0009E", "DP05_0013PE", "DP05_0052PE", "DP05_0064PE", "DP05_0046PE", 
    "DP05_0025E", "DP05_0020PE", "DP05_0093PE", "DP05_0006PE", "DP05_0073E", 
    "DP05_0067PE", "DP05_0068PE", "DP05_0021PE", "DP05_0055PE", "DP05_0089E", 
    "DP05_0056PE", "DP05_0026PE", "DP05_0009PE", "DP05_0004E", "DP05_0075E", 
    "DP05_0058E", "DP05_0063E", "DP05_0011E", "DP05_0068E", "DP05_0092PE", 
    "DP05_0011PE", "DP05_0047E", "DP05_0083E", "DP05_0019E", "DP05_0091PE", 
    "DP05_0045E", "DP05_0090PE", "DP05_0042E", "DP05_0074E", "DP05_0080PE", 
    "DP05_0024PE", "DP05_0010PE", "DP05_0008PE", "DP05_0061PE", "DP05_0072PE", 
    "DP05_0060E", "DP05_0086PE", "DP05_0089PE", "DP05_0016E", "DP05_0039PE", 
    "DP05_0077PE", "DP05_0092E", "DP05_0005PE", "DP05_0051PE", "DP05_0014PE", 
    "DP05_0058PE", "DP05_0086E", "DP05_0078E", "DP05_0045PE", "DP05_0066PE", 
    "DP05_0048E", "DP05_0081PE", "DP05_0079E", "DP05_0035PE", "DP05_0040PE", 
    "DP05_0006E", "DP05_0043PE", "DP05_0054PE", "DP05_0014E", "DP05_0021E", 
    "DP05_0022PE", "DP05_0082E", "DP05_0078PE", "DP05_0041PE", "DP05_0007E", 
    "DP05_0085E", "DP05_0069PE", "DP05_0023E", "DP05_0020E", "DP05_0031PE", 
    "DP05_0013E", "DP05_0023PE", "DP05_0057PE", "DP05_0071E", "DP05_0075PE", 
    "DP05_0001PE", "DP05_0049PE", "DP05_0016PE", "DP05_0032PE", "DP05_0031E", 
    "DP05_0038E", "DP05_0022E", "DP05_0027E", "DP05_0032E", "DP05_0027PE", 
    "DP05_0077E", "DP05_0051E", "DP05_0059E", "DP05_0035E", "DP05_0001E", 
    "DP05_0007PE", "DP05_0015E", "DP05_0094PE", "DP05_0018PE", "DP05_0091E", 
    "DP05_0002E", "DP05_0074PE", "DP05_0067E", "DP05_0062E", "DP05_0063PE", 
    "DP05_0050E", "DP05_0030PE", "DP05_0090E" 
    ]
}

# Function to generate query parameter string
def generate_query_param_string(geo_unit):
    return geo_unit.replace(" ", "%20") + ":*"

# Function to fetch data for a batch of columns with retries and delay
def fetch_census_data(group, numeric_columns, year, geo_unit):
    if group.startswith('B') or group.startswith('C'):
        base_url = f"https://api.census.gov/data/{year}/{DATASET}"
    elif group.startswith('S'):
        base_url = f"https://api.census.gov/data/{year}/{DATASET}/subject"
    elif group.startswith('D'):
        base_url = f"https://api.census.gov/data/{year}/{DATASET}/profile"
    else:
        logger.error("Import FAILED, Variable Group Code did not meet specification for base_url assignment")
        return None

    query_params = f"?get=NAME,{','.join(numeric_columns)}&for={generate_query_param_string(geo_unit)}"
    url = base_url + query_params
    retries = 3

    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTPError: {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying in 5 seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(5)
            else:
                logger.error(f"Failed to fetch data after {retries} attempts for batch: {numeric_columns}.")
                return None

# Function to process and store the data in PostgreSQL
def process_and_store_data(data, group, numeric_columns, year, geo_unit, first_batch):
    engine = create_engine(DB_IP)

    # Identify the geo column dynamically
    geo_col = [col for col in data[0] if geo_unit in col.lower()][0]

    # Convert data to DataFrame with dynamic column names
    columns = [col.upper() for col in data[0]]
    columns[columns.index(geo_col.upper())] = 'GEOID'
    df = pd.DataFrame(data[1:], columns=columns)
    
    # Rename NAME to GEONAME
    df.rename(columns={'NAME': 'GEONAME'}, inplace=True)
    
    # Convert numeric columns to appropriate types
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    # Add year, geounit, and timestamp columns
    df['YEAR'] = year
    df['GEOUNIT'] = geo_unit
    df['TIMESTAMP'] = datetime.now()

    # Rearrange columns
    df = df[['YEAR', 'GEOID', 'GEONAME', 'GEOUNIT'] + numeric_columns + ['TIMESTAMP']]

    # Pivot the table
    df_melted = df.melt(id_vars=['YEAR', 'GEOID', 'GEONAME', 'GEOUNIT', 'TIMESTAMP'], 
                        value_vars=numeric_columns, 
                        var_name='VARIABLE', 
                        value_name='VALUE')

    # Stage Level Model
    df_stage = df_melted.copy()  # Make a copy for staging purposes

    # Ensure VALUE column is stored as FLOAT in PostgreSQL
    with engine.connect() as conn:
        df_stage.to_sql(TABLE_NAME, con=conn, schema='stage', if_exists='append', index=False, dtype={'VALUE': types.Float()})
    
    distinct_sets = df_stage[['YEAR', 'GEOUNIT', 'VARIABLE']].drop_duplicates()
    output_message = "WRITE COMPLETE of sets to acs_acs1 YEAR, GEOUNIT, VARIABLE,\n"

    for index, row in distinct_sets.iterrows():
        output_message += f"{row['YEAR']},{row['GEOUNIT']},{row['VARIABLE']}\n"

    logger.info(output_message)
    logger.handlers[0].flush()

def main():
    logger.info(f"Spinning up {TABLE_NAME}")
    first_batch = True
    years = [2021, 2022, 2023]
    geo_units = ["state", "metropolitan statistical area/micropolitan statistical area"]

    for year in years:
        for geo_unit in geo_units:
            for group, numeric_columns in group_numeric_columns.items():
                for i in range(0, len(numeric_columns), 5):
                    batch_columns = numeric_columns[i:i+5]
                    data = fetch_census_data(group, batch_columns, year, geo_unit)
                    if data:
                        process_and_store_data(data, group, batch_columns, year, geo_unit, first_batch)
                        first_batch = False
                    else:
                        logger.error(f"Some columns failed in batch {batch_columns}, trying individually...")
                        for column in batch_columns:
                            single_column_data = fetch_census_data(group, [column], year, geo_unit)
                            if single_column_data:
                                process_and_store_data(single_column_data, group, [column], year, geo_unit, first_batch)
                                first_batch = False
                    time.sleep(1)  # Add a small delay between requests
    logger.info("Operation complete")
    logger.handlers[0].flush()

if __name__ == '__main__':
    main()
