import json
from datetime import datetime

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


def process_system_message(message: str, ride_id: int) -> list:
    """Takes a message and extracts the current bike user's data

    Args:
        message (str): kafka message decoded
        ride_id (int): len of ride_df + 1

    Returns:
        List: ride id, and user data
    """

    user_details = eval(message.split("= ")[-1])

    user_name = extract_user_name(user_details)

    user_data = [
        user_details["user_id"],
        user_name[0],
        user_name[1],
        user_details["gender"],
        user_details["date_of_birth"],
        user_details["height_cm"],
        user_details["weight_kg"],
        user_details["email_address"],
    ]

    user_ride_data = [ride_id, user_details["user_id"]]

    return user_ride_data, user_data


def process_system_data(
    user_ride_data: list, user_data: list
) -> pd.DataFrame | pd.DataFrame:
    """Takes the extracted data from message, and converts into a DataFrame

    Args:
        user_ride_data (list): list of values containing the ride_id and user_id
        user_data (list): list of values containing user's: id, firstname, lastname, gender, DoB, height, weight, email address

    Returns:
        pd.DataFrame | pd.DataFrame: 2 DataFrames of a single containing the information of the args
    """
    user_ride_df = pd.DataFrame(
        {"user_id": [user_ride_data[1]], "ride_id": [user_ride_data[0]]}
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


def process_ride_message(msg: str) -> list:
    """Takes a message and extracts the current ride data

    Args:
        message (str): kafka message decoded

    Returns:
        List: resistance, duration
    """
    current_ride_info = msg.split("[INFO]: ")[1][:-1].split("= ")

    resistance = current_ride_info[-1]
    duration = current_ride_info[1].split(";")[0]

    ride_table_resistance_duration = [resistance, duration]
    return ride_table_resistance_duration


def process_telemetry_message(msg: str) -> list:
    """Takes a message and extracts the current telemetry data

    Args:
        message (str): kafka message decoded

    Returns:
        List: power, heart rate, rotations_pm
    """
    current_telemetry_info = msg.split("[INFO]: ")[1][:-1].split("= ")

    power = current_telemetry_info[-1]
    hrt = current_telemetry_info[1].split(";")[0]
    rotations_pm = current_telemetry_info[2].split(";")[0]

    ride_table_power_hrt_rpm = [power, hrt, rotations_pm]
    return ride_table_power_hrt_rpm


def process_ride_telemetry_data(
    resistance_duration: list, power_hrt_rpm: list, ride_id: int
) -> pd.DataFrame:
    """Takes the two lists containing values for ride resistance, duration, power, heart rate, rpm and the ride_id,
    to create a DataFrame with one row to add to database

    Args:
        resistance_duration (list): List with two values, resistance and duration
        power_hrt_rpm (list): List with three values, power, heart rate and rpm
        ride_id (int): Current ride_id

    Returns:
        pd.DataFrame: DataFrame with one row containing all ride data for the current second of the ride
    """
    ride_row = [
        ride_id,
        resistance_duration[1],
        resistance_duration[0],
        power_hrt_rpm[1],
        power_hrt_rpm[2],
        power_hrt_rpm[0],
    ]
    time_now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    ride_df = pd.DataFrame(
        {
            "ride_id": [ride_row[0]],
            "duration": [ride_row[1]],
            "resistance": [ride_row[2]],
            "heart_rate": [ride_row[3]],
            "rotations_pm": [ride_row[4]],
            "power": [ride_row[5]],
            "time": time_now,
        }
    )
    return ride_df
