import os
import sys
from types import NoneType

import plotly.express as px

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from utils.dash_app_utils import get_current_ride_data, get_new_df, send_email
from dash import Input, Output, callback, dash_table, dcc, html
from dotenv import load_dotenv

load_dotenv()

user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
hostname = os.environ["DB_HOST"]
db_name = os.environ["DB_NAME"]
port = os.environ["DB_PORT"]
staging_schema = os.environ["STAGING_SCHEMA"]

html_text = """<html>
    <head></head>
    <body>
    <p>Your heart rate has fallen out of the recommended safe range.</p>
    <p>Please take care.</p>
    <p>This is an automated message.</p>
    </body>
    </html>
                """

body_text = (
    "Your heart rate has fallen out of the recommended safe range.\r\n"
    "Please take care.\r\n"
    "This is an automated message."
)

df = pd.DataFrame(
    {
        "RIDE ID": [""],
        "NAME": [""],
        "EMAIL": [""],
        "GENDER": [""],
        "AGE": [""],
        "DURATION": [""],
        "HEART RATE": [""],
    }
)

alert = dbc.Alert(
    "Your heart rate is out of the expected range. Please stay safe!",
    color="danger",
    dismissable=False,
    duration=3000,
)


dash.register_page(__name__, path="/Live_Data")

layout = html.Div(
    [
        html.H4("CURRENT RIDE"),
        dcc.Interval("graph-update", interval=1000, n_intervals=0),
        html.Div(id="the_alert", children=[]),
        dash_table.DataTable(
            id="table",
            data=df.to_dict("records"),
            columns=[
                {"name": ["Your Information", "NAME"], "id": "NAME"},
                {"name": ["Your Information", "AGE"], "id": "AGE"},
                {"name": ["Your Information", "GENDER"], "id": "GENDER"},
                {"name": ["Ride Information", "DURATION"], "id": "DURATION"},
                {"name": ["Ride Information", "HEART RATE"], "id": "HEART RATE"},
            ],
            style_table={"overflowX": "auto"},
            style_cell={"padding": "15px"},
            style_cell_conditional=[
                {"if": {"column_id": c}, "textAlign": "center"} for c in df.columns
            ],
            style_as_list_view=True,
            merge_duplicate_headers=True,
            style_header={"backgroundColor": "rgb(30, 30, 30)", "color": "white"},
            style_data={"backgroundColor": "rgb(50, 50, 50)", "color": "white"},
        ),
        html.Div(children=[dcc.Graph(id="graph-output-4")]),
    ]
)


@callback(
    dash.dependencies.Output("table", "data"),
    [dash.dependencies.Input("graph-update", "n_intervals")],
)
def update_table(n: int) -> pd.DataFrame:
    """Function that updates dash table with new date every second.

    Args:
        n (int): Counter that increases based on pre-defined interval.

    Returns:
        pd.DataFrame: Returns dataframe of new data.
    """
    try:
        df = get_new_df(
            "CURRENT_RIDE", user, password, hostname, port, db_name, staging_schema
        )
        df["GENDER"] = df["GENDER"][0].capitalize()
    except TypeError as te:
        print(te)
        pass
    except IndexError as ie:
        print(ie)
        pass
    except Exception as e:
        if (
            str(type(e)) == "<class 'ValueError'>"
            or '"ez_brokers_staging.CURRENT_RIDE" does not exist' in str(e)
            or str(type(e)) == "<class 'sqlalchemy.exc.NoSuchTableError'>"
        ):
            pass
    else:
        return df.to_dict("records")


@callback([Output("the_alert", "children")], [Input("table", "data")])
def check_heart_rate(data: dict) -> list:
    """Function that takes in data as a dictionary and returns an alert and email notification if the
    heart rate is in a critical place.

    Args:
        data (dict): Dictionary containing the most recent ride data.

    Returns:
        list: Returns list with either an alert or empty string.
    """
    heart_rate = data[0]["HEART RATE"]
    age = data[0]["AGE"]
    max_heart_rate = 207 - ((age) * (0.7))
    email = data[0]["EMAIL"]
    if type(heart_rate) != NoneType and heart_rate != "":
        if int(heart_rate) >= max_heart_rate or (
            int(heart_rate) <= 50 and int(heart_rate) > 0
        ):
            send_email(body_text, html_text, email, "Abnormal Heart Rate")
            return [alert]
    return [""]


@callback(
    Output("graph-output-4", "figure"),
    Input("graph-update", "n_intervals"),
    Input("table", "data"),
    suppress_callback_exceptions=True,
)
def update_graph(n: int, data: dict) -> px.pie:
    """Function that creates and updates a line graph with the heart rate of the current ride.

    Args:
        n (int): Counter that increases based on pre-defined interval.
        data (dict): Dictionary containing the most recent ride data.

    Returns:
        px.pie: Plotly express pie chart
    """
    ride_id = data[0]["RIDE_ID"]
    df = get_current_ride_data(
        ride_id, user, password, hostname, port, db_name, staging_schema, "RIDES"
    )
    figure = px.line(df, x="duration", y="heart_rate")
    return figure
