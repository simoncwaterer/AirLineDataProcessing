import re
import botocore.exceptions
import boto3

"""
    Parameters:
    - s3_client: The boto3 S3 client object  
    - bucket_name: Name of the S3 bucket to create
    - region: AWS region to create the bucket in, default 'us-east-1'
    
    Functionality:  
    - Validates the bucket name and region 
    - Checks if bucket already exists
    - Creates new S3 bucket in the specified region
    - Prints success/error messages
    - Returns the API response 
"""


def create_bucket(s3_client, bucket_name, region='us-east-1'):
    # Check that the bucket name is valid
    if not re.match(r'^[0-9a-z-]+$', bucket_name):
        raise ValueError(f'Invalid bucket name: {bucket_name}')
    # Check that the region is valid
    if region not in ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2', 'ca-central-1', 'eu-west-1', 'eu-west-2',
                      'eu-central-1', 'ap-south-1', 'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1',
                      'ap-northeast-2', 'sa-east-1']:
        raise ValueError(f'Invalid region: {region}')
    try:

        for bucket in s3_client.list_buckets()['Buckets']:
            print(bucket['Name'])

        # check if the region is the default us-east-1 region
        if region == 'us-east-1':
            bucket_response = s3_client.create_bucket(Bucket=bucket_name)
        else:
            # Set location constraint to the region
            bucketConfiguration = {'LocationConstraint': region}
            bucket_response = s3_client.create_bucket(CreateBucketConfiguration=bucketConfiguration, Bucket=bucket_name)

        print(f'Bucket {bucket_name} created successfully in {region} region.')
        return bucket_response
    except botocore.exceptions.ClientError as e:
        print(f"Error creating bucket: {e}")
        raise e


"""
Parameters:
- s3_connection: The boto3 S3 connection object
- bucket_name: Name of the S3 bucket to upload to
- file_name: Path of the file to upload  
- object_name: Name to upload the file as in S3. Defaults to file_name.

Functionality:
- Checks if the specified S3 bucket exists using head_bucket()
- Uploads the given file to the S3 bucket using the provided name
- If bucket does not exist, raises an Exception
- If any other error occurs, it raises the botocore ClientError
"""


def upload_to_s3(s3_client, bucket_name, file_name, object_name=None):
    # check if bucket exists. If it does not throw an Exception
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket {bucket_name} does not exist.")
            raise Exception("Bucket does not exist.")
        else:
            raise e

    # bucket exists , so we can upload to it s3
    object_name = object_name or file_name
    s3_client.upload_file(file_name, bucket_name, object_name)
