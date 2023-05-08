import os
import uuid
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer, cimpl
from dotenv import load_dotenv
from sqlalchemy import create_engine

import utils.extract_utils as util


def user_ride_length() -> int:
    """Find the number of rides that have occurred

    Returns:
        length (int): length of table from aurora
    """

    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )
    try:
        with engine.connect() as conn:
            user_ride_df = pd.read_sql_table("USER_RIDES", conn, schema=staging_schema)
            length = len(user_ride_df)
    except:
        length = 0
        pass
    finally:
        return length


def write_df_to_sql_staging(df: pd.DataFrame, table_name: str):
    """Save the pandas dataframe to tables in staging schema,
       appends if table exists

    Args:
        df (pd.dataframe): dataframe created from kafka stream
        table_name (_type_): name of table on aurora we want to write to
    """

    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )
    print("Created engine")
    with engine.connect() as conn:
        df.to_sql(
            table_name, conn, schema=staging_schema, if_exists="append", index=False
        )

        print("Dataframe pushed")


def get_messages(consumer: cimpl.Consumer) -> pd.DataFrame:
    """
    Connect to kafka topic and get messages,
    extracts data, and puts into a new df,
    push to table on aurora

     Args:
         consumer (cimpl.Consumer): kafka consumer

     Returns:
         String: when function is stopped
    """
    consumer.subscribe([kafka_topic_name])
    try:
        while True:
            consumer_poll = consumer.poll(0.5)

            if consumer_poll is None:
                print("Waiting for new messages")
                continue
            if consumer_poll.error():
                print("Error!")

            msg_dict = util.decode_message(consumer_poll)

            msg = msg_dict["log"]

            print(msg)

            if "SYSTEM" in msg:
                ride_id = user_ride_length() + 1
                user_ride_df, user_df = util.process_system_message(msg, ride_id)

                write_df_to_sql_staging(user_ride_df, "USER_RIDES")
                write_df_to_sql_staging(user_df, "USERS")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        return "Stopped Streaming From Kafka"


if __name__ == "__main__":
    load_dotenv()

    bootstrap_servers = os.environ.get("KAFKA_SERVER")
    security_protocol = "SASL_SSL"
    sasl_mechanisms = "PLAIN"
    sasl_username = os.environ.get("KAFKA_USERNAME")
    sasl_password = os.environ.get("KAFKA_PASSWORD")
    kafka_topic_name = os.environ.get("KAFKA_TOPIC")

    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    hostname = os.environ["DB_HOST"]
    db_name = os.environ["DB_NAME"]
    port = os.environ["DB_PORT"]
    staging_schema = os.environ["STAGING_SCHEMA"]
