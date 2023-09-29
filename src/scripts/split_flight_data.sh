#!/bin/bash

# split_flight_data.sh
#
# Splits a CSV file into multiple files with a specified number of lines.
# Adds the header from the original CSV to the top of each split file.
#
# Arguments:
#   $1: Number of lines to split the CSV into 
#   $2: Path to CSV file to split
#   
# Functionality:
#   Gets the header line from the CSV file
#   Uses split to split the CSV into multiple files with $1 lines each
#   Loops through each split file
#     Concatenates the header and split file into a temporary file
#     Renames the temporary file to replace the split file
#     Deletes the original split file
#   This has the effect of prepending the header to each split file

# Get number of lines to split into from first argument 
split_lines=$1

# Get csv file to split from second argument
csv_file=$2

# Split csv file into multiple files with $split_lines lines each
split -l "$split_lines" "$csv_file" split_csv

# Add header to each split file
for f in split_csv*; do
    head -n 1 "$csv_file" > "$f".tmp
    cat "$f" >> "$f".tmp
    mv "$f".tmp "$f".csv
    rm "$f"
done
