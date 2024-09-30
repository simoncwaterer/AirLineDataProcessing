import logging
import os
import boto3
import botocore

import manage_s3
import utilities

"""
main()
    
    The main function executes the following:
    
    1. Parses command line arguments using the utilities.parse_arguments() function.
    
    2. Configures logging.
    
    3. Sets the AWS profile name to 'airline_data'.
    
    4. Creates an S3 client using the boto3 library. 
    
    5. Iterates through all S3 buckets and deletes any buckets that match the prefix passed in the command line 
    arguments. This involves:
    
        - Listing all objects in the bucket
        - Deleting all objects 
        - Deleting the empty bucket
    
    6. Logs that all matching buckets have been deleted.
    
    The main functionality is to delete any S3 buckets that match a given prefix, cleaning up buckets created by 
    previous runs
    
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

    # set aws profile name
    aws_profile = 'airline_data'
    # create s3 client
    s3_client = boto3.Session(profile_name=aws_profile).client('s3')

    # iterate through s3 buckets and delete them
    for bucket in s3_client.list_buckets()['Buckets']:
        # check if bucket name has prefix that matches the one passed as parameter
        if bucket['Name'].startswith(cmd_args.prefix):
            logging.info(f"Deleting bucket: {bucket['Name']}")
            s3_objects = s3_client.list_objects_v2(Bucket=bucket['Name'])["Contents"]
            s3_objects = list(map(lambda x: {"Key": x["Key"]}, s3_objects))
            s3_client.delete_objects(Bucket=bucket['Name'], Delete={"Objects": s3_objects})
            s3_client.delete_bucket(Bucket=bucket['Name'])
    logging.info('All buckets deleted!')


if __name__ == "__main__":
    main()
