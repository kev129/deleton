import numpy as np
import pandas as pd
from dash_extract import df


def change_seconds_to_minutes(x: int) -> int:
    """Takes a time value in seconds and changes the value to hours

    Args:
        x (int): The duration of a bike ride in seconds

    Returns:
        int: The duration of a bike ride in minutes
    """

    return round(x / 3600)


def generate_id_gender_duration_df(df: pd.DataFrame) -> pd.DataFrame:
    """Get a dataframe that only includes ride id, gender and time elapsed columns

    Args:
        df (pd.DataFrame): entire dashboard dataframe

    Returns:
        pd.DataFrame: Only the desired columns
    """

    return df[["ride_id", "gender", "time_elapsed"]]


def duration_by_gender_df(df: pd.DataFrame) -> pd.DataFrame:
    """Shows the total duration of bike rides taken by males and females,
    and changes duration from seconds to hours

    Args:
        df (pd.DataFrame): entire dashboard dataframe

    Returns:
        pd.DataFrame: total duration and gender with duration augmented
    """

    total_duration_by_gender = df.groupby("ride_id").max().groupby("gender").sum()
    total_duration_by_gender.time_elapsed = total_duration_by_gender.time_elapsed.apply(
        lambda x: change_seconds_to_minutes(x)
    )
    return total_duration_by_gender


age_bins = [
    "0-18",
    "19-25",
    "26-32",
    "33-39",
    "40-45",
    "46-52",
    "53-59",
    "60-65",
    "65+",
]


def rides_across_age_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Gets a dataframe showing the number of rides being taken by different age brackets

    Args:
        df (pd.DataFrame): entire dashboard dataframe

    Returns:
        pd.DataFrame: Number of rides per age group
    """

    id_age_duration = df[["ride_id", "age", "time_elapsed"]]
    age_ride_id = id_age_duration[["ride_id", "age"]]
    age_rides_with_bins = age_ride_id.groupby(
        pd.cut(age_ride_id.age, bins=[0, 18, 25, 32, 39, 45, 52, 59, 65, np.Inf])
    ).nunique()
    age_rides_with_bins.index = age_bins

    return age_rides_with_bins


def duration_of_rides_across_age_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Gets a dataframe showing the total duration of rides taken by different age groups

    Args:
        df (pd.DataFrame): entire dashboard dataframe

    Returns:
        pd.DataFrame: duration of rides per age bucket
    """

    age_duration = df[["age", "time_elapsed"]]
    duration_by_age = age_duration.groupby(
        pd.cut(age_duration.age, bins=[0, 18, 25, 32, 39, 45, 52, 59, 65, np.Inf])
    ).sum()
    duration_by_age.index = age_bins
    duration_by_age = duration_by_age[["time_elapsed"]]
    duration_by_age.time_elapsed = duration_by_age.time_elapsed.apply(
        lambda x: change_seconds_to_minutes(x)
    )

    return duration_by_age


def time_power_output_df(df: pd.DataFrame) -> pd.DataFrame:
    """Filters for time and power output

    Args:
        df (pd.DataFrame): entire dashboard dataframe

    Returns:
        pd.DataFrame: time and power output
    """

    return df[["time", "power"]]


id_gender_duration_df = generate_id_gender_duration_df(df)
total_duration_by_gender = duration_by_gender_df(id_gender_duration_df)
age_rides_df_with_bins = rides_across_age_groups(df)
duration_by_age = duration_of_rides_across_age_groups(df)
time_power_df = time_power_output_df(df)
