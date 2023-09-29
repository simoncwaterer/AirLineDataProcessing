import os
import pandas as pd
import logging

import utilities

if __name__ == "__main__":
    # parse the command line arguments to fetch data directory
    parser = utilities.parse_arguments()

    # add a logging object
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    try:
        cmd_args = parser.parse_args()
        logging.info("Parsing command line arguments")
        logger.info(cmd_args)
    except SystemExit as e:
        logging.exception("Exception whilst parsing command line arguments", e.code)
        exit(e.code)

    # configure directory and sub-directory paths
    dir_path = cmd_args.dir
    metadata_dir = 'documentation'
    flight_data_dir = 'flight-info'

    # check that the directory specified in args.dir contains the required subdirectories
    if not os.path.isdir(os.path.join(dir_path, metadata_dir)) or not os.path.isdir(os.path.join(dir_path, flight_data_dir)):
        logging.error(f"Directory {dir_path} does not contain required {metadata_dir} and {flight_data_dir} subdirectories.")
        exit(1)

    # Get all the Parquet file names from the flight data directory
    flight_files = [f for f in os.listdir(os.path.join(dir_path, flight_data_dir)) if f.endswith('.parquet')]
    
    # Concatenate all the Parquet files into a single DataFrame
    flight_data = pd.concat([pd.read_parquet(os.path.join(dir_path, flight_data_dir, f), engine='pyarrow') for f in flight_files] ,ignore_index=True)

    # log that the dataframe has been created
    logging.info(f"Flight data dataframe created with {flight_data.shape[0]} rows")

    # how large is the dataframe?
    flight_data_row_count = flight_data.shape[0]

    print(f'Shape:\n{flight_data.shape}\n')
    print(f'Dtypes:\n{flight_data.dtypes}\n')
    flight_data.head()
    

    flight_date = dict(day=flight_data.DayofMonth, month=flight_data.Month, year=flight_data.Year)

    # combine the Year, Month and DayofMonth columns from flight_data into a full date value
    flight_data['FlightDate'] = pd.to_datetime(flight_date['day'], flight_date['month'], flight_date['year'])
    
    flight_data.sample(5)[['Year','Month','DayofMonth','full_date']]

