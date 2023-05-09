import math
import os
from datetime import datetime as dt
from typing import Union

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
hostname = os.environ["DB_HOST"]
db_name = os.environ["DB_NAME"]
port = os.environ["DB_PORT"]
staging_schema = os.environ["STAGING_SCHEMA"]
production_schema = os.environ["PRODUCTION_SCHEMA"]


def read_table_from_schema(table_name: str, schema_name: str) -> pd.DataFrame:
    """Connects to aurora and reads the sql table in a given schema, returns pandas df

    Args:
        table_name (str): the sql table name to be accessed
        schema (str): the schema where the table lies

    Returns:
        pd.DataFrame: DataFrame containing information from table
    """

    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )

    with engine.connect() as conn:
        df = pd.read_sql_table(table_name, conn, schema=schema_name)

    return df


def get_users_rides_data() -> tuple:
    """Creates DataFrames from the 3 tables in aurora

    Returns:
        tuple: 3 Panda df's, one for user data, one for ride data and one for user ride data
    """

    users_df = read_table_from_schema("USERS", staging_schema)

    rides_df = read_table_from_schema("RIDES", staging_schema)

    user_rides_df = read_table_from_schema("USER_RIDES", staging_schema)

    return users_df, rides_df, user_rides_df
