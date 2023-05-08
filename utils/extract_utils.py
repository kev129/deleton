import json

import pandas as pd
from confluent_kafka import cimpl


def decode_message(message_object: cimpl.Message) -> dict:
    """Takes kafka message in bytes and decodes it using 'utf-8'

    Args:
        message_object (cimpl.Message): kafka message decoded

    Returns:
        json dict
    """
    return json.loads(message_object.value().decode("utf-8"))


def extract_user_name(user_details):
    """Takes in name from msg,
       extracts first name and last name even if prefix is present

    Args:
        user_data (dict): dict of all user details

    Returns:
        List: first name, last name
    """

    if len(user_details["name"].split(" ")) == 3:
        return [user_details["name"].split(" ")[1], user_details["name"].split(" ")[2]]

    elif len(user_details["name"].split(" ")) == 2:
        return [user_details["name"].split(" ")[0], user_details["name"].split(" ")[1]]


def extract_user_data(message: str, ride_id: int) -> list:
    """Takes a message and extracts the current bike user's data

    Args:
        message (str): kafka message decoded
        ride_id (int): len of ride_df + 1

    Returns:
        List: ride id, and user data
    """

    user_details = eval(message.split("= ")[-1])

    user_name = extract_user_name(user_details)

    new_user_row = [
        user_details["user_id"],
        user_name[0],
        user_name[1],
        user_details["gender"],
        user_details["date_of_birth"],
        user_details["height_cm"],
        user_details["weight_kg"],
        user_details["email_address"],
    ]

    user_ride_table_data = [ride_id, user_details["user_id"]]

    return user_ride_table_data, new_user_row


def process_system_message(msg: str, ride_id: int) -> pd.DataFrame | pd.DataFrame:
    """Takes the new system message and ride_id, extracts the relevant data and writes
    to staging schema

    Args:
        msg (str): Kafka message
        ride_id (int): ride_id based on what is currently in the database
    """
    join_list_data, user_data = extract_user_data(msg, ride_id)
    user_ride_df = pd.DataFrame(
        {"user_id": [join_list_data[1]], "ride_id": [join_list_data[0]]}
    )
    user_df = pd.DataFrame(
        {
            "user_id": user_data[0],
            "first_name": [user_data[1]],
            "last_name": [user_data[2]],
            "gender": [user_data[3]],
            "dob": [str(user_data[4])],
            "height": [user_data[5]],
            "weight": [user_data[6]],
            "email": [user_data[7]],
        }
    )

    return user_ride_df, user_df
