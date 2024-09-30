import os
import logging

import utilities
import clean_data
import transform_parquet

def main():
    """Main pipeline for cleaning flight data.
    
    This script handles:
    
    1. Parsing command line arguments for the data directory
    2. Setting up logging
    3. Cleaning the metadata and flight data CSV files
    4. Transforming the cleaned CSV data into Parquet format
    
    """
    
    # 1. Process command line arguments
    parser = utilities.parse_arguments()  
    args = parser.parse_args()
    data_dir = args.dir
    
    # 2. Create logger 
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # 3. Clean CSV files
    metadata_dir = 'documentation' 
    flight_data_dir = 'flight-info'
    clean_data.clean_metadata(data_dir, metadata_dir)
    clean_data.clean_flightdata(data_dir, flight_data_dir)
    
    # 4. Generate Parquet files 
    transform_parquet.write_csv_parquet(data_dir, metadata_dir, flight_data_dir)
    
if __name__ == "__main__":
    main()