from datetime import datetime
import os
from pprint import pprint

import numpy as np

import utilities

import os
import pandas as pd
import logging

import utilities

METADATA_DIR = 'documentation'
FLIGHT_DATA_DIR = 'flight-info'


def parse_arguments():
    """Parse command line arguments"""
    parser = utilities.parse_arguments()
    try:
        cmd_args = parser.parse_args()
        logging.info("Parsing command line arguments")
        logging.info(cmd_args)
    except SystemExit as e:
        logging.exception("Exception whilst parsing command line arguments", e.code)
        exit(e.code)
    return cmd_args


def check_directories(dir_path: str):
    """Check required subdirectories exist"""
    if not os.path.isdir(os.path.join(dir_path, METADATA_DIR)) or not os.path.isdir(
            os.path.join(dir_path, FLIGHT_DATA_DIR)):
        logging.error(
            f"Directory {dir_path} does not contain required {METADATA_DIR} and {FLIGHT_DATA_DIR} subdirectories.")
        exit(1)


def load_flight_data(dir_path: str, flight_data_dir: str) -> pd.DataFrame:
    """Load and concat flight data parquet files"""
    flight_files = [f for f in os.listdir(os.path.join(dir_path, flight_data_dir)) if f.endswith('.parquet')]
    return pd.concat(
        [pd.read_parquet(os.path.join(dir_path, flight_data_dir, f), engine='pyarrow') for f in flight_files],
        ignore_index=True)

#_________________________________________________________
# Function that convert the 'HHMM' integer or float to datetime.time
def convert_to_time(cell):
    if pd.isnull(cell):
        return np.nan
    else:
        if cell == 2400: cell = 0
        cell = "{0:04d}".format(int(cell))
        time = datetime.time(int(cell[0:2]), int(cell[2:4]))
        return time


"""
clean the flight_data by dropping unnecessary columns and cleaning data types
This should include converting date types to datetime, cleaning categorical data, and dropping unused columns

This should include the following 
1. the Cancelled and Diverted columns should be updated, replacing the numerical values with string categories Cancelled /Not Cancelled and Diverted/Not Diverted
2. creating a datetime column from the year, month, day columns and drop the individual date columns
3. convert columns series DepTime and ArrTime from floats to python time object. Currently the float values within these columns are encoded as follows the first digit or first two digits represent the hour and the last two digits represent the minutes
4. convert columns CRSDepTime and CRSArrTime from integers to python time object. Currently the integer values within these columns are encoded as follows the first digit or first two digits represent the hour and the last two digits represent minutes
"""
def clean_flight_data(flight_data: pd.DataFrame) -> pd.DataFrame:
    # 1. the Cancelled and Diverted columns should be updated, replacing the numerical values with string categories Cancelled /Not Cancelled and Diverted/Not Diverted
    flight_data['Cancelled'] = flight_data['Cancelled'].astype('category').replace({0: 'Not Cancelled', 1: 'Cancelled'})
    flight_data['Diverted'] = flight_data['Diverted'].astype('category').replace({0: 'Not Diverted', 1: 'Diverted'})

# 2. creating a datetime column from the year, month, day columns and drop the individual date columns
    date_dict = {'year': flight_data['Year'], 'month': flight_data['Month'], 'day': flight_data['DayofMonth']}
    flight_data['Date'] = pd.to_datetime(date_dict)
    flight_data.drop(columns=['Year','Month','DayofMonth'], inplace=True)

    flight_data[['DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime']] = flight_data[['DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime']].apply(convert_to_time)

    return flight_data
    


"""
Process and clean the flight data DataFrame
This function takes a flight data DataFrame as input, cleans the data by converting data types,
creating new columns, and dropping unused columns, and returns the cleaned DataFrame
:param flight_data: DataFrame containing raw flight data
:return: DataFrame containing cleaned flight data
"""
def process_flight_data(flight_data: pd.DataFrame) -> pd.DataFrame:
    """Process loaded flight data"""
    date_dict = {'year': flight_data['Year'], 'month': flight_data['Month'], 'day': flight_data['DayofMonth']}

    flight_data['Date'] = pd.to_datetime(date_dict)
    
    flight_data['Cancelled'] = flight_data['Cancelled'].astype('category').replace({0: 'Not Cancelled', 1: 'Cancelled'})
    flight_data['Diverted'] = flight_data['Diverted'].astype('category').replace({0: 'Not Diverted', 1: 'Diverted'})

    return flight_data


def main():
    # set logging level
    logging.getLogger().setLevel(logging.INFO)

    cmd_args = parse_arguments()
    dir_path = cmd_args.dir

    check_directories(dir_path)

    flight_data = load_flight_data(dir_path, FLIGHT_DATA_DIR)
    logging.info(f"Flight data dataframe created with {flight_data.shape[0]} rows")

    print(flight_data.dtypes)
    print(flight_data.columns)
    print(flight_data.sample(20)[['Year', 'Month', 'DayofMonth', 'Cancelled', 'Diverted', 'DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime']])

    flight_data = clean_flight_data(flight_data)
    logging.info("Flight data cleaned")

    print(flight_data.dtypes)
    print(flight_data.columns)
    print(flight_data.sample(20)[['Date', 'Year', 'Month', 'DayofMonth', 'Cancelled', 'Diverted', 'DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime']])


    flight_data = process_flight_data(flight_data)

    print(flight_data.sample(5)[['Date', 'Year', 'Month', 'DayofMonth', 'Cancelled', 'Diverted']])

    print(f'Shape:\n{flight_data.shape}\n')
    print(f'Dtypes:\n{flight_data.dtypes}\n')

    print(flight_data['UniqueCarrier'].unique())
    print(flight_data['UniqueCarrier'].nunique())
    print(flight_data['UniqueCarrier'].value_counts())
    print(flight_data.Origin.unique())

    flight_data.groupby(['UniqueCarrier', 'Origin'])['ArrDelay'].mean().reset_index()

    print(flight_data.sample(5)[['CRSArrTime', 'CRSDepTime', 'ArrTime', 'DepTime']])

    flight_data.to_csv(os.path.join(dir_path, PROCESSED_FLIGHT_DATA_DIR, PROCESSED_FLIGHT_DATA_FILE), index=False)
    flight_data.to_parquet(os.path.join(dir_path, PROCESSED_FLIGHT_DATA_DIR, PROCESSED_FLIGHT_DATA_FILE.replace('.csv', '.parquet')), index=False)  

if __name__ == "__main__":
    main()
