import os
import logging
import pyarrow.parquet as pq
import pyarrow.csv as csv

"""Converts CSV files to Parquet format.

    Args:
        dir_path (str): The directory path containing the CSV files.
        metadata_subdir (str): The subdirectory containing the metadata CSV file.
        flight_subdata (str): The subdirectory containing the flight data CSV file.
"""
def write_csv_parquet(dir_path: str, metadata_subdir: str, flight_subdir: str):
    # set up logging
    logger = logging.getLogger(__name__)

    # set input and output file paths
    metadata_csv = os.path.join(dir_path, metadata_subdir, "metadata.csv")
    flight_data_csv = os.path.join(dir_path, flight_subdir, "flight_data.csv")

    metadata_parquet = os.path.join(dir_path, metadata_subdir, "metadata.parquet")
    flight_data_parquet = os.path.join(dir_path, flight_subdir, "flight_data.parquet")

    # read csv files
    table = csv.read_csv(metadata_csv)
    flight_table = csv.read_csv(flight_data_csv)

    # write parquet files
    pq.write_table(table, metadata_parquet)
    pq.write_table(flight_table, flight_data_parquet)

    logger.info("CSV files transformed to Parquet")
