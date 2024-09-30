# Airline Data Processing

## Description

This Python application cleans, transforms, and uploads airline flight data to Amazon S3 for analytics. It takes a directory of CSV files as input, cleans any invalid rows, converts the CSV data to efficient Parquet format, generates an S3 bucket, and uploads both the cleaned CSV and Parquet files to S3. The goal is to prepare raw flight data for analysis by removing invalid rows, using a compressed columnar format, and loading into cloud storage. Key steps include argument parsing, input validation, cleaning, transformation, S3 bucket creation, and file uploads. This enables downstream analytics on clean, efficient flight data stored in S3.

The project processes data about airline flights. Specifically to analyse data related to flight delays. The data used is from the following site [dataverse.harvard.edu](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7). The data used consists of the following:

* flight arrival and departure details for all commercial flights within the USA, from October 1987 to April 2008. The flight data is packaged in yearly chunks. 
* locations of US airports
* listing of carrier codes with airline names
* information about planes  

Variable descriptions: Name Description 1 Year 1987-2008 2 Month 1-12 3 DayofMonth 1-31 4 DayOfWeek 1 (Monday) - 7 (Sunday) 5 DepTime actual departure time (local, hhm m) 6 CRSDepTime scheduled departure time (local, hhmm) 7 ArrTime actual arrival time (local, hhmm) 8 CRSArrTime scheduled arrival time (local, hhmm) 9 UniqueCarrier unique carrier code 10 FlightNum flight number 11 TailNum plane tail number 12 ActualElapsedTime in minutes 13 CRSElapsedTime in minutes 14 AirTime in minutes 15 ArrDelay arrival delay, in minutes 16 DepDelay departure delay, in minutes 17 Origin origin IATA airport code 18 Dest des tination IATA airport code 19 Distance in miles 20 TaxiIn taxi in time, in minutes 21 TaxiOut taxi out time in minutes 22 Cancelled was the flight cancelled? 23 CancellationCode reason for cancellation (A = carrier, B = weather, C = NAS, D = security) 24 Diverted 1 = yes, 0 = no 25 CarrierDelay in minutes 26 WeatherDelay in minutes 27 NASDelay in minutes 28 SecurityDelay in minutes 29 LateAircraftDelay in minutes

## Installation

### Configure AWS Credentials

To access AWS services, you'll need to configure your AWS credentials. You can do this a few different ways of course. Below are the key steps to create an IAM user, define a policy and generate access keys.

1. Log into the AWS Management Console and go to the IAM service.
2. Click on "Users" in the left sidebar and then click the "Add user" button.
3. Give the user a name and select "Programmatic access" for the access type. Click through to create the user.
4. Create a policy for the user that grants the required S3 permissions. Some common S3 policies are "AmazonS3FullAccess" or "AmazonS3ReadOnlyAccess". Attach the policy to the user.
5. Click on the "Security credentials" tab and then click the "Create access key" button. 
6. Save the Access key ID and Secret access key somewhere secure.
7. Add the credentials as environment variables or configure them in your code as follows.
  
Set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables:

```
export AWS_ACCESS_KEY_ID=<your access key>
export AWS_SECRET_ACCESS_KEY=<your secret key> 
```

Use the AWS shared credentials file (~ /.aws/credentials ):

```
[default]
aws_access_key_id = <your access key>
aws_secret_access_key = <your secret key>
Use AWS configuration files (~ /.aws/config ):
[default]
aws_access_key_id = <your access key>
aws_secret_access_key = <your secret key>
region = us-east-1
```

To install dependencies run:

```
bash
pip install -r requirements.txt
```

## Usage

split_flight_dash.sh

This Bash script splits a large CSV file into smaller chunks while preserving the header row in each output file. It takes two arguments - the number of lines to split on, and the path to the input CSV file.

clean_upload.py

* Validates the input directory structure
* Calls functions to clean the raw CSV data
* Transforms the cleaned CSV data to Parquet format
* Creates the S3 bucket
* Uploads the cleaned CSV files to the S3 bucket 
* Uploads the Parquet files to the S3 bucket

In summary, it takes raw CSV flight data as input, cleans it, converts it to Parquet, generates an S3 bucket, and uploads both CSV and Parquet to S3. The main goals are cleaning data, converting to Parquet for efficiency, and loading into S3 for storage and further processing.



To use this project, run:

```bash
python main.py
```