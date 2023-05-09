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


def merge_dataframes(
    users_df: pd.DataFrame, rides_df: pd.DataFrame, junction_df: pd.DataFrame
) -> pd.DataFrame:
    """Takes 3 dataframes and merges them into and returns it

    Args:
        users_df (pd.Dataframe): df with data of user
        rides_df (pd.Dataframe): df with data from individual rides
        junction_df (pd.Dataframe): df with data of user id attached with ride id

    Returns:
        joined df (pd.Dataframe): columns time, user_id, first_name, last_name, gender, age, height,
        weight, ride_id, duration, resistance, heart_rate, rotations_pm
    """

    users_df = users_df.drop_duplicates()
    users_df_with_ride_id = users_df.merge(junction_df)
    joined_df = users_df_with_ride_id.merge(rides_df)

    return joined_df


def convert_dob_to_age(dob: str) -> Union[None, int]:
    """Takes the dob in ms, and converts to age in years

    Args:
        dob (int): ms from 01/01/1970

    Returns:
        age in years (int): age in years
    """

    if not math.isnan(float(dob)):
        dob = int(dob)
        epoch_in_seconds = dob / 1000
        formatted_dob = dt.fromtimestamp(epoch_in_seconds)
        epoch_age = dt.now() - formatted_dob
        age = math.floor(epoch_age.days / 365)
        return age
    return dob


def rename_age_duration(df: pd.DataFrame) -> pd.DataFrame:
    """Renames dob and duration columns to age and time_elapsed respectively

    Args:
        df (pd.Dataframe): dataframe of data joined on ride and user id

    Returns:
        renamed df (pd.Dataframe): df with renamed dob and duration
    """

    df = df.rename(columns={"dob": "age", "duration": "time_elapsed"})
    return df


def change_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Changes column type of joined df to be of type string

    Args:
        df (pd.Dataframe): joined df with all data

    Returns:
        df (pd.Dataframe): df with some columns retyped
    """

    for col in [
        "first_name",
        "last_name",
        "gender",
        "heart_rate",
        "resistance",
        "rotations_pm",
        "power",
    ]:
        df[col] = df[col].astype("string")

    return df


def replace_zeroes_with_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Checks specified columns to for 0 values and replaces with null

    Args:
        df (pd.Dataframe): joined df

    Returns:
        df (pd.Dataframe): df with 0's replaced
    """

    columns_to_change = ["heart_rate", "resistance", "rotations_pm", "power"]
    for column in columns_to_change:
        df[column] = np.where(df[column] == "0", np.nan, df[column])
        df[column] = np.where(df[column] == "0.0", np.nan, df[column])

    return df


def write_df_to_sql_production(df: pd.DataFrame, table_name: str):
    """Save the pandas dataframe to tables in production schema,
        appends to table if the table exists already

    Args:
        df (pd.Dataframe): df that needs to be push to production
        table_name (str): name of the table in the schema
    """

    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )
    with engine.connect() as conn:
        df.to_sql(
            table_name, conn, schema=production_schema, if_exists="replace", index=False
        )

        print("Dataframe transformed")


def handler(event, context):
    users_df, rides_df, junction_df = get_users_rides_data()
    joined_df = merge_dataframes(users_df, rides_df, junction_df)

    joined_df["dob"] = joined_df["dob"].apply(lambda x: convert_dob_to_age(x))
    joined_df = rename_age_duration(joined_df)
    joined_df = change_dtypes(joined_df)
    joined_df = replace_zeroes_with_nulls(joined_df)

    write_df_to_sql_production(joined_df, "EZ_PRODUCTION_TABLE")

    return "Wrote clean data to production schema"
