import logging
import os
import boto3
import botocore
import manage_s3
import utilities
import clean_data
import transform_parquet
import upload_data_s3


"""
    Parameters:
    - parser: ArgumentParser object to parse command line arguments
    - logger: Logging object to log events
    - cmd_args: Parsed command line arguments
    - dir_path: Path to directory containing data 
    - metadata_dir: Name of subdirectory containing metadata
    - flight_data_dir: Name of subdirectory containing flight data
    
    Functionality:
    - Parses command line arguments
    - Configures logging
    - Validates directory structure
    - Calls functions to clean data
    - Transforms CSV data to Parquet
    - Uploads CSV and Parquet data to S3
"""
from typing import TypeVar

T = TypeVar('T')


def main():
    parser = utilities.parse_arguments()

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    try:
        cmd_args = parser.parse_args()
        logging.info(cmd_args)
    except SystemExit as e:
        logging.exception("Exception whilst parsing command line arguments", e.code)
        exit(e.code)

    

    # configure directory and sub-directory paths
    dir_path = cmd_args.dir
    metadata_dir = 'documentation'
    flight_data_dir = 'flight-info'

    # check that the directory specified in args.dir contains a documentation sub-directory and an flight-info sub-directory
    if not os.path.isdir(os.path.join(dir_path, metadata_dir)) or not os.path.isdir(os.path.join(dir_path, flight_data_dir)):
        logging.error(f"Directory {dir_path} does not contain required {metadata_dir} and {flight_data_dir} subdirectories.")
        exit(1)

    clean_data.clean_metadata(dir_path, metadata_dir)
    clean_data.clean_flightdata(dir_path, flight_data_dir)

    transform_parquet.write_csv_parquet(dir_path, metadata_dir, flight_data_dir)

    # set aws profile name
    aws_profile = 'airline_data'
    # generate bucket name
    s3_bucket_name = utilities.generate_bucket_name(cmd_args.bucket)
    # create s3 client
    s3_client = boto3.Session(profile_name=aws_profile).client('s3')
    # create s3 bucket
    s3_bucket_response = manage_s3.create_bucket(s3_client, s3_bucket_name, cmd_args.region)
    # upload cdv and parquet data to s3 bucket
    upload_data_s3.upload_parquet_s3(dir_path, metadata_dir, flight_data_dir, s3_client, s3_bucket_name)
    upload_data_s3.upload_csv_s3(dir_path, metadata_dir, flight_data_dir, s3_client, s3_bucket_name);
    
if __name__ == "__main__":
    main()