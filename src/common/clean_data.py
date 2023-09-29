import logging
import os
import pyarrow.csv as csv


def skip(row):
    logging.debug(row)
    return 'skip'

# iterate through all csv files in the metadata sub-directory, clean the csv data and rewrite the csv file
# check for lines with missing values and remove them
def clean_metadata(dir_path, metadata_dir):









# iterate through all csv files in the flight data sub-directory, clean the csv data and rewrite the csv file
# check for lines with missing values and remove them
def clean_flightdata(dir_path, flight_dir):
    flightdata_path = os.path.join(dir_path, flight_dir)
    for filename in os.listdir(flightdata_path):
        if filename.endswith('.csv'):
            csv_path = os.path.join(flightdata_path, filename)
            # read and parse csv file drop all rows which are invalid
            parse_options = csv.ParseOptions(delimiter=",", invalid_row_handler=skip)
            table = csv.read_csv(csv_path, parse_options=parse_options)
            logging.debug(table.shape)
            # write clean csv file
            csv.write_csv(table, csv_path)