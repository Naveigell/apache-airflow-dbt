import os
from datetime import datetime

import duckdb
from dotenv import load_dotenv

from scripts.send_email import send_email

load_dotenv()

DB_PATH              = os.environ['DATABASE_NAME']
ALERT_THRESHOLD_DROP = 0.8
SUMMARY_PATH         = os.environ['SUMMARY_PATH'] + '/summary.csv'

def email_anomaly(message):
    """
    Send an email alert with a monitoring body message and a custom message.

    Parameters
    ----------
    body_message : str
        Monitoring body message.
    message : str
        Custom message to include in the email.

    Returns
    -------
    None

    """
    body = f"Monitoring got anomaly with message : {message}"

    send_email(
        to=os.environ['MAIL_TO_ADDRESS'],
        subject="Anomaly Alert",
        body=body,
    )

def check_data_freshness():
    """
    Check data freshness in the DuckDB database.

    Raise a ValueError if there is no data in the fct_trips table.
    Raise a ValueError if the data is not updated in the last 24 hours.

    Print a success message if the data is up to date.

    :return: None
    """
    with duckdb.connect(DB_PATH) as con:
        result = con.execute("SELECT MAX(pickup_datetime) AS latest FROM fct_trips").fetchone()

    latest_date = result[0]

    if latest_date is None:
        raise ValueError("No data in fct_trips table")

    lag_days = (datetime.now() - latest_date).days
    if lag_days > 1:
        email_anomaly(f"Data not updated! Latest update {lag_days} days ago at ({latest_date})")
    else:
        print(f"Data updated, latest {latest_date}")


def validate_row_counts():
    """
    Validate row counts in the fct_trips table.

    Check if there are any rows with fare_amount <= 0.

    Raise a ValueError if there are any invalid rows.

    Print a success message with the total row count and valid count.

    :return: None
    """
    with duckdb.connect(DB_PATH) as con:
        df = con.execute("SELECT COUNT(*) AS total_rows, SUM(fare_amount <= 0) AS invalid_fare FROM fct_trips").df()

    total = df.loc[0, 'total_rows']
    invalid = df.loc[0, 'invalid_fare']

    if invalid > 0:
        email_anomaly(f"Founded {invalid} rows with fare_amount <= 0")
    else:
        print(f"Rows count ({total:,} valid count)")


def compare_with_historical():
    """
    Compare row counts with historical data.

    Raise a ValueError if the ratio of the latest day to the average of the previous days is less than the alert threshold.

    Print a success message if the ratio is within the alert threshold.

    :return: None
    """
    with duckdb.connect(DB_PATH) as con:
        query = """
            SELECT
                DATE (pickup_datetime) AS tanggal, COUNT (*) AS trips
            FROM fct_trips
            GROUP BY 1
            ORDER BY 1 DESC
                LIMIT 7 \
            """
        df = con.execute(query).df()

    if len(df) < 2:
        print("Historical data not enough")
        return

    latest = df.iloc[0]["trips"]
    avg_hist = df["trips"][1:].mean()

    ratio = latest / avg_hist if avg_hist > 0 else 1

    if ratio < ALERT_THRESHOLD_DROP:
        email_anomaly(f"Total trip is down: {ratio:.2%} from historical average")
    else:
        print(f"Normal trip ({latest} vs average {avg_hist:.0f})")


def generate_daily_summary():
    """
    Generate a daily summary report.

    This function generates a daily summary report by querying the fct_trips table in the DuckDB database.
    The report contains the total trip count, total revenue, and average fare for the latest day.
    The report is saved as a CSV file at the path specified by the SUMMARY_PATH variable.

    :param None: None
    :return: None
    """
    with duckdb.connect(DB_PATH) as con:
        summary = con.execute("""
                              SELECT
                                  DATE (pickup_datetime) AS tanggal, COUNT (*) AS total_trip, SUM (total_amount) AS total_revenue, AVG (total_amount) AS avg_fare
                              FROM fct_trips
                              GROUP BY 1
                              ORDER BY 1 DESC
                                  LIMIT 1
                              """).df()

    summary.to_csv(SUMMARY_PATH, index=False)

    print(f"Daily report at : {SUMMARY_PATH}")