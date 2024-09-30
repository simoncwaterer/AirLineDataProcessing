import logging
import utilities
import os
import pyarrow.csv as csv


def skip(row):
    logging.debug(row)
    return 'skip'


# Clean the metadata files
# 1. iterate through all csv files in the metadata subdirectory
# 2. clean the csv data by removing all the rows which are invalid and check for lines with missing values
# 3. rewrite the clean csv file
# 4. use the pyarrow csv package to parse and clean the csv data
def clean_metadata(dir_path, metadata_dir):
    metadata_path = os.path.join(dir_path, metadata_dir)
    for filename in os.listdir(metadata_path):
        if filename.endswith('.csv'):
            csv_path = os.path.join(metadata_path, filename)
            # read and parse csv file drop all rows which are invalid
            parse_options = csv.ParseOptions(delimiter=",", invalid_row_handler=skip)
            table = csv.read_csv(csv_path, parse_options=parse_options)
            logging.debug(table.shape)
            #            csv.write_csv(table, os.path.join(metadata_path, filename.replace('.csv', '_clean.csv')))
            csv.write_csv(table, csv_path)


# iterate through all csv files in the flight data subdirectory, clean the csv data and rewrite the csv file
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


def main():
    # capture commands line arguments
    # add a logger to output debug information
    # configure the metadata and flight directory values
    parser = utilities.parse_arguments()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    cmd_args = parser.parse_args()

    # configure directory and subdirectory paths
    dir_path = cmd_args.dir
    metadata_dir = 'documentation'
    flight_data_dir = 'flight-info'

    clean_metadata(dir_path, metadata_dir)
    clean_flightdata(dir_path, flight_data_dir)


if __name__ == "__main__":
    main()
