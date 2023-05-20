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
