import logging
import os
import boto3
import botocore

import manage_s3
import utilities

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

# set aws profile name
aws_profile = 'airline_data'
# create s3 client
s3_client = boto3.Session(profile_name=aws_profile).client('s3')

# iterate through s3 buckets and delete them
for bucket in s3_client.list_buckets()['Buckets']:
    logging.info(bucket['Name'])
    s3_objects = s3_client.list_objects_v2(Bucket = bucket['Name'])["Contents"]
    s3_objects = list(map(lambda x: {"Key":x["Key"]}, s3_objects))
    s3_client.delete_objects(Bucket = bucket['Name'], Delete = {"Objects":s3_objects})
    s3_client.delete_bucket(Bucket=bucket['Name'])
logging.info('All buckets deleted!')
