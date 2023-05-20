import os
from datetime import datetime as dt
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
import pandas as pd
import plotly.express as px
import PyPDF2
from dotenv import load_dotenv
from fpdf import FPDF
from sqlalchemy import create_engine

import utils.report_utils as report_utils

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
            WHERE to_timestamp(time, 'DD-MM-YYYY HH24:MI:SS') > (NOW() - INTERVAL '23 HOUR')
            """
    engine = create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    return df


def create_multipart_message(
    sender: str,
    recipients: list,
    title: str,
    text: str = None,
    html: str = None,
    attachments: list = None,
) -> MIMEMultipart:
    """Creates a MIME multipart message object.
    Uses only the Python `email` standard library.
    Emails, both sender and recipients, can be just the email string or have the format 'The Name <the_email@host.com>'.

        Args:
            sender (str): The sender
            recipients (list): List of recipients. Needs to be a list, even if only one recipient.
            title (str): The title of the email.
            text (str): The text version of the email body (optional).
            html (str): The html version of the email body (optional).
            attachments (list): List of files to attach in the email.

        Returns:
            MIMEMultipart: A `MIMEMultipart` to be used to send the email.
    """
    multipart_content_subtype = "alternative" if text and html else "mixed"
    msg = MIMEMultipart(multipart_content_subtype)
    msg["Subject"] = title
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    if text:
        part = MIMEText(text, "plain")
        msg.attach(part)
    if html:
        part = MIMEText(html, "html")
        msg.attach(part)

    for attachment in attachments or []:
        with open(attachment, "rb") as f:
            part = MIMEApplication(f.read())
            part.add_header("Content-Disposition", "attachment", filename=(attachment))
            msg.attach(part)

    return msg
