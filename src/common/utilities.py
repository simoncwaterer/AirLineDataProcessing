import argparse
import os
import uuid

"""
Parameters:
- bucket (str): Name of S3 bucket to upload files to. Required.
- region (str): AWS region name. Defaults to value in AWS_DEFAULT_REGION env var or 'us-east-1'.  
- dir (str): Directory path containing files to upload. Required.

Functionality:
- Parses input arguments passed to the script.
- Required arguments are bucket name and directory path to files.
- Optional arguments are AWS region.
- Sets defaults for region if not provided.
- Returns parsed args object containing the argument values.
"""


def parse_arguments():
    parser = argparse.ArgumentParser(description='S3 bucket operations')
    parser.add_argument('-b', '--bucket', help='Name of S3 bucket', required=True)
    parser.add_argument('-r', '--region', help='AWS region name',
                        default=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
    parser.add_argument('-d', '--dir', help='Directory path containing files to upload', required=True)
    return parser


"""
function to create a s3 bucket name. Generate a unique bucket using a generated uuid and a prefix
"""


def generate_bucket_name(bucket_prefix):
    bucket_name = f"{bucket_prefix}-{uuid.uuid4()}"
    return bucket_name
