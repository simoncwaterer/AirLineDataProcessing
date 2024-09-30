import logging
import os
from turtle import up
import boto3
import botocore
import pyarrow.parquet as pq
import pyarrow.csv as csv

import manage_s3
import utilities

# iterate through all csv files in the sub-directories dir_path/metadata and dir_path/flight-info
# and upload each file to the S3 bucket using manage_s3.upload_file
def upload_csv_s3(dir_path, metadata_dir, flight_data_dir, s3_client, s3_bucket_name):
    try:
        for subdir in [metadata_dir, flight_data_dir]:
            subdir_path = os.path.join(dir_path, subdir)
            for filename in os.listdir(subdir_path):
                if filename.endswith('.csv'):
                    file_path = os.path.join(subdir_path, filename)
                    key = f"{subdir}/{filename}"
                    try:
                        manage_s3.upload_to_s3(s3_client, s3_bucket_name, file_path, key)
                        logging.info(f"Uploaded {file_path} to {s3_bucket_name}/{key}")
                    except botocore.exceptions.ClientError as e: # type: ignore
                        logging.error(f"Failed to upload {file_path} to S3: {e}")
                        break
    except Exception as e:
        logging.exception(f"failed to open {dir_path}  {e}")

# iterate through all parquet files in the sub-directories dir_path/metadata and dir_path/flight-info
# and upload each file to the s3 bucket
def upload_parquet_s3(dir_path, metadata_dir, flight_data_dir, s3_client, s3_bucket_name):
    for subdir in [metadata_dir, flight_data_dir]: 
        subdir_path = os.path.join(dir_path, subdir)
        for filename in os.listdir(subdir_path):
            if filename.endswith('.parquet'):
                file_path = os.path.join(subdir_path, filename)
                # get the key name for S3 object by replacing .csv with .parquet in the key name
                key = f"{subdir}/{filename}"
                # upload the parquet file to S3
                manage_s3.upload_to_s3(s3_client, s3_bucket_name, file_path, key)
                logging.info(f"Uploaded {file_path} to {s3_bucket_name}/{key}")

"""
    main() function to handle execution of the upload_data_s3 module.
    
    Parameters:
        None
    
    Functionality:
        - Parses command line arguments
        - Configures logging
        - Validates directory structure
        - Sets up AWS client and credentials
        - Creates S3 bucket 
        - Calls function to upload CSV data to S3
        - Calls function to upload Parquet data to S3
        
    Returns:
        None
"""
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

    # set aws profile name
    aws_profile = 'airline_data'
    # generate bucket name
    s3_bucket_name = utilities.generate_bucket_name(cmd_args.bucket)
    # create s3 client
    s3_client = boto3.Session(profile_name=aws_profile).client('s3')
    # create s3 bucket
    s3_bucket_response = manage_s3.create_bucket(s3_client, s3_bucket_name, cmd_args.region)

    upload_csv_s3(dir_path, metadata_dir, flight_data_dir, s3_client, s3_bucket_name)
    upload_parquet_s3(dir_path, metadata_dir, flight_data_dir, s3_client, s3_bucket_name)

if __name__ == "__main__":
    main()
