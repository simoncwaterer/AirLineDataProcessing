import os
import pytest
from unittest.mock import MagicMock, patch
from src.aws import clean_upload, clean_data, transform_parquet, manage_s3, upload_data_s3, utilities

@patch('src.aws.clean_upload.utilities.parse_arguments')
@patch('src.aws.clean_upload.clean_data.clean_metadata')
@patch('src.aws.clean_upload.clean_data.clean_flightdata')
@patch('src.aws.clean_upload.transform_parquet.write_csv_parquet')
@patch('src.aws.clean_upload.boto3.Session')
@patch('src.aws.clean_upload.manage_s3.create_bucket')
@patch('src.aws.clean_upload.upload_data_s3.upload_parquet_s3')
@patch('src.aws.clean_upload.upload_data_s3.upload_csv_s3')
def test_main(mock_upload_csv_s3, mock_upload_parquet_s3, mock_create_bucket, mock_boto3_session,
              mock_write_csv_parquet, mock_clean_flightdata, mock_clean_metadata, mock_parse_arguments):
    # Set up mock return values
    mock_args = MagicMock()
    mock_args.dir = 'test_dir'
    mock_args.bucket = 'test_bucket'
    mock_args.region = 'test_region'
    mock_parse_arguments.return_value.parse_args.return_value = mock_args
    
    mock_s3_client = MagicMock()
    mock_boto3_session.return_value.client.return_value = mock_s3_client
    
    # Call the main function
    clean_upload.main()
    
    # Assert expected function calls
    mock_parse_arguments.assert_called_once()
    mock_clean_metadata.assert_called_once_with('test_dir', 'documentation')
    mock_clean_flightdata.assert_called_once_with('test_dir', 'flight-info')
    mock_write_csv_parquet.assert_called_once_with('test_dir', 'documentation', 'flight-info')
    mock_boto3_session.assert_called_once_with(profile_name='airline_data')
    mock_create_bucket.assert_called_once_with(mock_s3_client, 'test_bucket', 'test_region')
    mock_upload_parquet_s3.assert_called_once_with('test_dir', 'documentation', 'flight-info', mock_s3_client, 'test_bucket')
    mock_upload_csv_s3.assert_called_once_with('test_dir', 'documentation', 'flight-info', mock_s3_client, 'test_bucket')

@patch('src.aws.clean_upload.utilities.parse_arguments')
def test_main_invalid_directory(mock_parse_arguments):
    # Set up mock return values
    mock_args = MagicMock()
    mock_args.dir = 'invalid_dir'
    mock_parse_arguments.return_value.parse_args.return_value = mock_args
    
    # Call the main function and assert it exits with code 1
    with pytest.raises(SystemExit) as exc_info:
        clean_upload.main()
    assert exc_info.value.code == 1

@patch('src.aws.clean_upload.utilities.parse_arguments')
def test_main_argument_parsing_error(mock_parse_arguments):
    # Set up mock to raise SystemExit exception
    mock_parse_arguments.return_value.parse_args.side_effect = SystemExit(2)
    
    # Call the main function and assert it exits with code 2
    with pytest.raises(SystemExit) as exc_info:
        clean_upload.main()
    assert exc_info.value.code == 2
