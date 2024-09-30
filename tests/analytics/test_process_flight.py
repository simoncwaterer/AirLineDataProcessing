import pandas as pd
from src.analytics.process_flight import clean_flight_data

def test_clean_flight_data():
    # Test data
    flight_data = pd.DataFrame({
        'Cancelled': [0, 1, 0],
        'Diverted': [0, 0, 1],
        'Year': [2021, 2022, 2023],
        'Month': [1, 2, 3],
        'DayofMonth': [1, 15, 30],
        'DepTime': [1200, 1500, 1800],
        'CRSDepTime': [1100, 1400, 1700],
        'ArrTime': [1400, 1700, 2000],
        'CRSArrTime': [1300, 1600, 1900]
    })

    # Call the function
    cleaned_data = clean_flight_data(flight_data)

    # Assert the expected changes
    assert cleaned_data['Cancelled'].dtype == 'category'
    assert cleaned_data['Diverted'].dtype == 'category'
    assert 'Date' in cleaned_data.columns
    assert 'Year' not in cleaned_data.columns
    assert 'Month' not in cleaned_data.columns
    assert 'DayofMonth' not in cleaned_data.columns

def test_clean_flight_data_empty():
    # Test with an empty DataFrame
    empty_data = pd.DataFrame()
    cleaned_data = clean_flight_data(empty_data)
    assert cleaned_data.empty

def test_clean_flight_data_missing_columns():
    # Test with missing columns
    missing_columns_data = pd.DataFrame({
        'Cancelled': [0, 1, 0],
        'Diverted': [0, 0, 1],
        'Year': [2021, 2022, 2023],
        'Month': [1, 2, 3],
        'DayofMonth': [1, 15, 30]
    })
    cleaned_data = clean_flight_data(missing_columns_data)
    assert 'DepTime' not in cleaned_data.columns
    assert 'CRSDepTime' not in cleaned_data.columns
    assert 'ArrTime' not in cleaned_data.columns
    assert 'CRSArrTime' not in cleaned_data.columns
