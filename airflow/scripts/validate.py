import pandas as pd


def validate_file(files):
    """
    Validate if the given parquet files have the expected columns.

    Expected columns are:
        - 'VendorID'
        - 'tpep_pickup_datetime'
        - 'tpep_dropoff_datetime'
        - 'passenger_count'
        - 'trip_distance'
        - 'RatecodeID'
        - 'store_and_fwd_flag'
        - 'PULocationID'
        - 'DOLocationID'
        - 'payment_type'
        - 'fare_amount'
        - 'extra'
        - 'mta_tax'
        - 'tip_amount'
        - 'tolls_amount'
        - 'improvement_surcharge'
        - 'total_amount'
        - 'congestion_surcharge'
        - 'airport_fee'

    Parameters
    ----------
    files : list of str
        List of paths to the parquet files to validate.

    Returns
    -------
    bool
        True if the parquet files have the expected columns, False otherwise.
    """
    expected_cols = {
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID',
        'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
        'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
        'congestion_surcharge', 'airport_fee'
    }

    for file in files:
        df = pd.read_parquet(file, engine='pyarrow')
        # some of the column name is different, so we need to rename it
        df.rename(columns={'Airport_fee': 'airport_fee'}, inplace=True)

        if set(df.columns) != expected_cols:
            missing_cols = expected_cols - set(df.columns)
            raise ValueError(f"Validation error, there are {missing_cols} column not found in {file}")

    return True