import os
from datetime import datetime

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()

user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
hostname = os.environ["DB_HOST"]
db_name = os.environ["DB_NAME"]
port = os.environ["DB_PORT"]
production_schema = os.environ["PRODUCTION_SCHEMA"]


def read_sql_table(table_name: str) -> pd.DataFrame:
    """Connects to aurora and read the sql table, converts to pandas df

    Args:
        table_name (str): table name as a string to put into the connection command

    Returns:
        dataframe (pd.dataframe): dataframe from aurora table
    """

    engine = sqlalchemy.create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )

    with engine.connect() as conn:
        df = pd.read_sql_table(table_name, conn, schema=production_schema)

    return df


def get_single_row_for_rides(df: pd.DataFrame):
    """Gets unique rides by taking only rows with a duration of 1

    Args:
        df (pd.DataFrame): dateframe of all ride and user table from ez_production_table

    Returns:
        _type_: _description_
    """
    df_unique_rides = df[(df["time_elapsed"] == "1.0")]
    return df_unique_rides


main_ride_df = read_sql_table("EZ_PRODUCTION_TABLE")
unique_ride_df = get_single_row_for_rides(main_ride_df)


def format_rides() -> dict:
    """Returns all the of the ride data into a json format

    Returns:
        formatted_json:number of rides, success, ride data in dict
    """
    formatted_json = format_ride(unique_ride_df)
    return formatted_json


def format_ride(ride_df: pd.DataFrame) -> dict:
    """Helper function that creates the json formatted rides

    Args:
        ride_df (pd.DataFrame): dataframe that contains all ride details

    Returns:
        dict: number of rides, successful creation, rides list
    """
    return {
        "total rides": len(make_rides_list(ride_df)),
        "success": True,
        "rides": make_rides_list(ride_df),
    }


def make_rides_list(ride_d: pd.DataFrame) -> list:
    """Create list of rides with the information wanted

    Args:
        ride_df (pd.DataFrame): dataframe that contains all ride details

    Returns:
        list: rides in dict form
    """
    list_of_rides = []
    ride_df = ride_df.reset_index()
    for index, ride in ride_df.iterrows():
        list_of_rides.append(ride_dict_creator(ride))
    return list_of_rides


def ride_dict_creator(ride: pd.Series) -> dict:
    """Takes one row of dataframe and extracts data from it

    Args:
        ride (pd.Series): One row of the dataframe as a series

    Returns:
        dict: dictionary format for one ride
    """
    return {
        "id": ride["user_id"],
        "ride_id": ride["ride_id"],
        "first_name": ride["first_name"],
        "last_name": ride["last_name"],
        "time": str(ride["time"]),
        "age": ride["age"],
    }


ride_json = format_rides()


def get_ride_by_id(list_id: list) -> dict:
    """Takes a list of ids, iterates through list and returns rides for those id's

    Args:
        list_id (list): list of id's in url

    Returns:
       dict : boolean if all the rides were fetched, and list of rides based on ids
    """
    rides = []
    all_rides = ride_json
    ride_list = all_rides["rides"]

    for ride_id in list_id:
        for ride in ride_list:
            if ride["ride_id"] == int(ride_id):
                rides.append(ride)

    if len(rides) < len(list_id):
        return {"all_rides_available": False, "rides": rides}
    elif len(rides) == len(list_id):
        return {"all_rides_available": True, "rides": rides}
    else:
        return "No ride with that ride ID"


def delete_ride_by_id(list_id: list) -> str:
    """Takes a list of ids, and iterates through list and deletes from ride_json

    Args:
        list_id (list): list with ids for rides to be deleted

    Returns:
        str: string that says rides have been deleted and lists the rides that have been deleted
    """
    all_rides = ride_json
    ride_list = all_rides["rides"]

    for ride_id in list_id:
        for ride in ride_list.copy():
            if ride.get("ride_id") == int(ride_id):
                ride_list.remove(ride)
    return f"Rides with ride ids for {list_id} have been deleted"


def get_riders() -> list:
    """Loops through main df, drops duplicate user_id rows and returns list of riders

    Returns:
        list_of_riders(list): list of all riders as a dict
    """
    df_rides = unique_ride_df
    dropped_df = df_rides.drop_duplicates(subset=["user_id"])
    dropped_df = dropped_df.reset_index()
    list_of_riders = []
    for index, ride in dropped_df.iterrows():
        list_of_riders.append(rider_dict_creator(ride))
    return list_of_riders


def convert_HR_column(x: str) -> int:
    """Takes a value in a pandas column, converts str to int

    Args:
        x (str): heart rate value

    Returns:
        x (int): heart rate as an int
    """
    if isinstance(x, str):
        return int(x)
    return x


def get_avg_heart_rate(id: int) -> int:
    """Gets the average heart rate of user, filters df for 1 user, and return average heart rate

    Args:
        id (int): user id that you need a heart rate for

    Returns:
        int: average heart rate
    """
    df = main_ride_df
    df_user = df.loc[df["user_id"] == int(id)]
    df_hr = df_user.copy()
    df_hr["heart_rate"] = df_hr["heart_rate"].apply(lambda x: convert_HR_column(x))

    avg_hr = (
        df_hr["heart_rate"].astype(float).agg({"heart_rate": "mean"}).round().values[0]
    )

    return avg_hr


def rider_dict_creator(ride: pd.Series) -> dict:
    """Takes a row from the df, and produces a dict of the required information

    Args:
        ride (pd.Series): a row as a series

    Returns:
        dict: dict of rider information
    """
    return {
        "id": ride["user_id"],
        "avg_heart_rate": get_avg_heart_rate(ride["user_id"]),
        "first_name": ride["first_name"],
        "last_name": ride["last_name"],
        "time": str(ride["time"]),
        "age": ride["age"],
        "email": ride["email"],
    }


def get_rider_info(user_ids: list) -> list:
    """Returns list of rider info if one or more riders provided

    Args:
        user_ids (list): list of ids to filter df for

    Returns:
        list: list of rider info in the form of a dict
    """
    list_of_riders = get_riders()
    list_of_user_ids = []
    list_of_users = []

    for rider in list_of_riders:
        list_of_user_ids.append(rider["id"])

    for user_id in user_ids:
        if int(user_id) in list_of_user_ids:
            for rider in list_of_riders:
                if rider["id"] == int(user_id):
                    list_of_users.append(rider)
    if len(list_of_users) > 0:
        return list_of_users
    else:
        return "No user with that id"


def get_all_rides_of_user(user_id: int) -> list:
    """Takes a user id, and gets all the rides for that user

    Args:
        user_id (int): user id that you want to get rides for

    Returns:
        list: list of rides for a specific user
    """
    rides = ride_json
    list_of_user_ids = []
    list_of_rides = []
    for ride in rides["rides"]:
        list_of_user_ids.append(ride["id"])

    if int(user_id) in list_of_user_ids:
        for rider in rides["rides"]:
            if rider["id"] == int(user_id):
                list_of_rides.append(rider)
        return list_of_rides
    else:
        return "User with that id has no rides"


def get_rides_for_day(date: str) -> dict:
    """Take a date as a string and filters all rides for rides on that date, if no date defaults to today

    Args:
        date (str): date you want to get rides for

    Returns:
        dict: number of rides, rides in a list
    """
    if date is None:
        date = datetime.today().strftime("%d/%m/%Y")
    else:
        date = str(date)

    rides = ride_json
    list_of_dates = []
    list_of_rides = []

    for ride in rides["rides"]:
        list_of_dates.append(str(ride["time"]))
    if str(date) in date:
        for rider in rides["rides"]:
            if str(date) in str(rider["time"]):
                list_of_rides.append(rider)
        return {"no. of rides": len(list_of_rides), "rides": list_of_rides}
    else:
        return "No rides on that date"
