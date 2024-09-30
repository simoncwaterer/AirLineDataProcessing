import os
import logging
import pyarrow.parquet as pq
import pyarrow.csv as csv

"""
Converts CSV files to Parquet format.

    Args:
        dir_path (str): The directory path containing the CSV files.
        metadata_subdir (str): The subdirectory containing the metadata CSV file.
        flight_subdata (str): The subdirectory containing the flight data CSV file.
"""
def write_csv_parquet(dir_path: str, metadata_subdir: str, flight_subdir: str):
    # set up logging
    logger = logging.getLogger(__name__)

    # set input and output file locations
    metadata_dir = os.path.join(dir_path, metadata_subdir)
    flight_data_dir = os.path.join(dir_path, flight_subdir)

    # iterate through the meta_data directory to get metadata csv file
    # for each csv file , get the filename without extension
    # and transform it into a parquet file with the same name
    for filename in os.listdir(metadata_dir):
        if filename.endswith(".csv"):
            metadata_csv = os.path.join(metadata_dir, filename)
            metadata_parquet = os.path.splitext(metadata_csv)[0]+".parquet"
            # read csv file using pyarrow and transform it to parquet file with same name
            table = csv.read_csv(metadata_csv)
            # write parquet file
            pq.write_table(table, metadata_parquet)

    # iterate through the flight_data directory to get flight_data csv file
    # for each csv file , get the filename without extension
    # and transform it into a parquet file with the same name
    for filename in os.listdir(flight_data_dir):
        if filename.endswith(".csv"):
            flight_csv = os.path.join(flight_data_dir, filename)
            flight_parquet = os.path.splitext(flight_csv)[0]+".parquet"
            # read csv file using pyarrow and transform it to parquet file with same name
            table = csv.read_csv(flight_csv)
            # write parquet file
            pq.write_table(table, flight_parquet)
            
    logger.info("CSV files transformed to Parquet")
