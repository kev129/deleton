import os
from datetime import datetime as dt
from typing import Union

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
hostname = os.environ["DB_HOST"]
db_name = os.environ["DB_NAME"]
port = os.environ["DB_PORT"]
production_schema = os.environ["PRODUCTION_SCHEMA"]
production_table = os.environ["PRODUCTION_TABLE"]


def fetch_dashboard_data() -> pd.DataFrame:
    """Connect to AWS Aurora database and read the dashboard data as a Pandas Dataframe

    Returns:
        pd.DataFrame: Dataframe from production schema to be used for visualizations
    """
    query = f"""
            SELECT * FROM {production_schema}.{production_table}
            WHERE to_timestamp(time, 'DD-MM-YYYY HH24:MI:SS') > (NOW() - INTERVAL '11 HOUR')
            """
    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    return df


def change_dtypes_to_int(x: object) -> float:
    """Change non-Nonetype values to float type

    Args:
        x (object): Defaulted type when reading table from Aurora

    Returns:
        float: Numerical row entries turned to float type
    """

    if x:
        return float(x)
    return x


def change_zero_values_to_null(x: float) -> Union[float, None]:
    """Change 0 values to None

    Args:
        x (float): Ride data row entries

    Returns:
        Union[float, None]: If ride data is 0, replace with null
        so the data is not skewed

    """

    if x == 0:
        return None
    return x


def swap_days_and_months(x: dt) -> dt:
    """Change date column to the format YYYY-MM-DD

    Args:
        x (dt): Date in format DD--MM-YYYY

    Returns:
        dt: Date in format YYYY-MM-DD
    """

    date_str = str(x).replace("-", "/")
    return dt.strptime(date_str, "%d/%m/%Y %H:%M:%S")


def apply_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Applies all cleaning functions to the dataframe as lambda functions

    Args:
        df (pd.DataFrame): Dataframe to apply cleaning on

    Returns:
        pd.DataFrame: clean dataframe ready for visualizations
    """

    float_columns = [
        "power",
        "time_elapsed",
        "resistance",
        "heart_rate",
        "rotations_pm",
    ]
    string_columns = ["first_name", "last_name", "gender"]

    for column in float_columns:
        df[column] = df[column].apply(lambda x: change_dtypes_to_int(x))

    for column in string_columns:
        df[column] = df[column].astype("string")

    df["power"] = df["power"].apply(lambda x: change_zero_values_to_null(x))
    df["time"] = df["time"].apply(lambda x: swap_days_and_months(x))

    return df


entire_dash_df = fetch_dashboard_data()
df = apply_cleaning(entire_dash_df)
