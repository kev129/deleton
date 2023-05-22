import math
import os
from datetime import datetime as dt

import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

load_dotenv()


sender = os.environ["SENDER"]


def create_row(
    ride_id: int,
    first_name: str,
    last_name: str,
    gender: str,
    dob: str,
    duration: int,
    HR: int,
    email: str,
    user: str,
    password: str,
    hostname: str,
    port: int,
    db_name: str,
    staging_schema: str,
):
    """Creates a row in the 'CURRENT_RIDE' table with the latest ride information.

    Args:
        ride_id (int) : Current ride id
        first_name (str): First name of rider
        last_name (str): Last name of rider
        gender (str): Gender of rider
        dob (str): Date of birth of rider
        duration (int): Current ride duration
        HR (int): Current heart rate of rider
        user (str): Database username
        email (str): Riders email
        password (str): Database password
        hostname (str): Database hostname
        port (int): Database port
        db_name (str): Database name
        staging_schema (str): Staging schema name
    """
    age = convert_dob_to_age(dob)
    full_name = f"{first_name} {last_name}"

    df = pd.DataFrame(
        {
            "RIDE_ID": [ride_id],
            "NAME": [full_name],
            "GENDER": [gender],
            "AGE": [age],
            "DURATION": [duration],
            "HEART RATE": [HR],
            "EMAIL": [email],
        }
    )

    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )
    with engine.connect() as conn:
        df.to_sql(
            "CURRENT_RIDE",
            conn,
            schema=staging_schema,
            if_exists="replace",
            index=False,
        )


def get_new_df(
    table_name: str,
    user: str,
    password: str,
    hostname: str,
    port: int,
    db_name: str,
    staging_schema: str,
) -> pd.DataFrame:
    """Obtains DataFrame from specified table.

    Args:
        table_name (str): Table name in question
        user (str): Database username
        password (str): Database password
        hostname (str): Database hostname
        port (int): Database port
        db_name (str): Database name
        staging_schema (str): Staging schema name

    Returns:
        pd.DataFrame: Returns pandas dataframe
    """
    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )
    with engine.connect() as conn:
        df = pd.read_sql_table(table_name, conn, schema=staging_schema)
    return df


def convert_dob_to_age(dob: str) -> int:
    """Takes the dob in ms, and converts to age in years

    Args:
        dob (int): ms from 01/01/1970

    Returns:
        int: Age in years
    """

    if not math.isnan(float(dob)):
        dob = int(dob)
        epoch_in_seconds = dob / 1000
        formatted_dob = dt.fromtimestamp(epoch_in_seconds)
        epoch_age = dt.now() - formatted_dob
        age = math.floor(epoch_age.days / 365)
        return age
    return dob


def send_email(BODY_TEXT: str, BODY_HTML: str, RECIPIENT: str, SUBJECT: str):
    """
    Function to send email to recipient.

    Args:
        BODY_TEXT (str): Body text of email
        BODY_HTML (str): HTML version of body text
        RECIPIENT (str): Recipient email
    """
    SENDER = sender
    CHARSET = "UTF-8"

    client = boto3.client("ses")

    try:
        response = client.send_email(
            Destination={
                "ToAddresses": [
                    RECIPIENT,
                ],
            },
            Message={
                "Body": {
                    "Html": {
                        "Charset": CHARSET,
                        "Data": BODY_HTML,
                    },
                    "Text": {
                        "Charset": CHARSET,
                        "Data": BODY_TEXT,
                    },
                },
                "Subject": {
                    "Charset": CHARSET,
                    "Data": SUBJECT,
                },
            },
            Source=SENDER,
        )
    except ClientError as e:
        print(e.response["Error"]["Message"])
    else:
        print("Email sent! Message ID:"),
        print(response["MessageId"])


def get_current_ride_data(
    ride_id: int,
    user: str,
    password: str,
    hostname: str,
    port: int,
    db_name: str,
    schema: str,
    table_name: str,
) -> pd.DataFrame:
    """Obtains dataframe based on current ride id.

    Args:
        ride_id (int): Current ride id
        user (str): Database username
        password (str): Database password
        hostname (str): Database hostname
        port (int): Database port
        db_name (str): Database name
        schema (str): Staging schema name
        table_name (str): Table name in question

    Returns:
        int: Age in years
    """

    query = f"""
            SELECT * FROM "{schema}"."{table_name}"
            WHERE ride_id={ride_id}
            """
    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    return df
