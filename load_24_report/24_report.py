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
name = os.environ["NAME"]
sender_email = os.environ["SENDER_EMAIL"]
recipient_email = os.environ["RECIPIENT_EMAIL"]


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


def send_mail(
    sender: str,
    recipients: list,
    title: str,
    text: str = None,
    html: str = None,
    attachments: list = None,
) -> dict:
    """Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    Args:
        sender (str): The sender
        recipients (list): List of recipients. Needs to be a list, even if only one recipient.
        title (str): The title of the email.
        text (str): The text version of the email body (optional).
        html (str): The html version of the email body (optional).
        attachments (list): List of files to attach in the email.

    Returns:
        dict:
    """

    AWS_REGION = "eu-west-2"
    msg = create_multipart_message(sender, recipients, title, text, html, attachments)
    ses_client = boto3.client("ses")
    return ses_client.send_raw_email(
        Source=sender, Destinations=recipients, RawMessage={"Data": msg.as_string()}
    )


def handler(event, context):
    df_24 = fetch_dashboard_data()

    total_rides = len(df_24["ride_id"].unique())
    avg_heart_rate = (
        df_24["heart_rate"].astype(float).agg({"heart_rate": "mean"}).round().values[0]
    )
    avg_power = df_24["power"].astype(float).agg({"power": "mean"}).round().values[0]
    total_power = df_24["power"].astype(float).agg({"power": "sum"}).round().values[0]

    df_unique_rides = df_24[(df_24["time_elapsed"] == "1.0")]

    df_gender = df_unique_rides.groupby("gender").count().reset_index()

    gender_split_bar_fig = px.bar(
        df_gender,
        x="gender",
        y="user_id",
        labels={"gender": "Gender", "user_id": "Number of rides"},
        text_auto=".2s",
        title="Gender split of rides",
    ).update_traces(marker_color=["#00898a", "#215d6e"])

    gender_split_pie_fig = px.pie(
        df_gender,
        values="user_id",
        names="gender",
        title="Gender split of rides",
        color_discrete_sequence=["#215d6e", "#00898a"],
    ).update_layout(
        title_font_color="#00898a",
        title_x=0.46,
        legend_title="Gender",
        legend_title_font_color="#00898a",
        legend_title_side="top",
        legend_bordercolor="#00898a",
        legend_borderwidth=5,
    )

    df_age = df_unique_rides.copy()

    df_age = df_age.groupby(
        pd.cut(df_age.age, bins=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    ).count()
    df_age = df_age[["user_id"]]
    df_age = df_age.reset_index()
    df_age["age"] = df_age["age"].astype(str)
    df_age["age"] = df_age["age"].str.replace("(", "")
    df_age["age"] = df_age["age"].str.replace("]", "")
    df_age["age"] = df_age["age"].str.replace(",", " -")

    age_split_fig = (
        px.bar(
            df_age,
            x="age",
            y="user_id",
            labels={"age": "Age groups", "user_id": "Number of rides"},
            text_auto=".2s",
            title="Age distribution of riders",
        )
        .update_xaxes(tickmode="linear")
        .update_layout(title_font_color="#00898a", title_x=0.46, width=650, height=500)
        .update_traces(
            marker_color=[
                "#2f5e7e",
                "#215d6e",
                "#08737f",
                "#089f8f",
                "#39b48e",
                "#64c987",
                "#92dc7e",
                "#c4ec74",
                "#fafa6e",
            ]
            * 3
        )
    )

    df_hr_cleaned = df_24.copy()
    df_hr_cleaned["heart_rate"] = df_hr_cleaned["heart_rate"].apply(
        lambda x: report_utils.convert_HR_column(x)
    )
    df_hr = df_hr_cleaned.groupby("user_id").agg({"heart_rate": "mean"}).reset_index()

    my_colors3 = [(x / 10.0, x / 20.0, 0.9) for x in range(len(df_hr))]

    avg_hr_fig = (
        px.bar(
            df_hr,
            x="user_id",
            y="heart_rate",
            labels={"user_id": "User ID", "heart_rate": "Average Heart Rate (bpm)"},
            text_auto=".2s",
            title="Average heart rate of each rider",
        )
        .update_xaxes(tickmode="linear")
        .update_traces(
            textfont_size=12,
            textangle=0,
            textposition="outside",
            cliponaxis=False,
            marker_color=[
                "#2f5e7e",
                "#215d6e",
                "#08737f",
                "#089f8f",
                "#39b48e",
                "#64c987",
                "#92dc7e",
                "#c4ec74",
                "#fafa6e",
            ]
            * 5,
        )
        .update_layout(title_font_color="#00898a", title_x=0.46, width=650, height=500)
    )

    df_power_cleaned = df_24.copy()
    df_power_cleaned["power"] = df_power_cleaned["power"].apply(
        lambda x: report_utils.convert_power_column(x)
    )
    df_power = df_power_cleaned.groupby("user_id").agg({"power": "mean"}).reset_index()

    avg_power_fig = (
        px.bar(
            df_power,
            x="user_id",
            y="power",
            labels={"user_id": "User ID", "power": "Power (W)"},
            text_auto=".2s",
            title="Average power of each rider",
        )
        .update_xaxes(tickmode="linear")
        .update_traces(
            textfont_size=12,
            textangle=0,
            textposition="outside",
            cliponaxis=False,
            marker_color=[
                "#2f5e7e",
                "#215d6e",
                "#08737f",
                "#089f8f",
                "#39b48e",
                "#64c987",
                "#92dc7e",
                "#c4ec74",
                "#fafa6e",
            ]
            * 5,
        )
    )

    gender_split_bar_fig.write_image(
        f"/tmp/{report_utils.get_today_date()}_gender_split_bar.pdf"
    )
    gender_split_pie_fig.write_image(
        f"/tmp/{report_utils.get_today_date()}_gender_split_pie.pdf"
    )
    age_split_fig.write_image(f"/tmp/{report_utils.get_today_date()}_age_split.pdf")
    avg_hr_fig.write_image(f"/tmp/{report_utils.get_today_date()}_avg_heart_rate.pdf")
    avg_power_fig.write_image(f"/tmp/{report_utils.get_today_date()}_avg_power.pdf")

    fpdf = FPDF()

    fpdf.add_page(orientation="L")
    fpdf.set_font("Arial", style="u", size=50)
    fpdf.text(x=10, y=30, txt=f"24 Hour Report - {report_utils.get_today_date()}")
    fpdf.set_font("Arial", size=30)
    fpdf.text(
        x=10, y=50, txt=f"Total number of rides over the last 24 hours: {total_rides} "
    )
    fpdf.text(
        x=10,
        y=60,
        txt=f"Average heart rate over the last 24 hours: {avg_heart_rate} bpm",
    )
    fpdf.text(x=10, y=70, txt=f"Average power over the last 24 hours: {avg_power} W")
    fpdf.text(x=10, y=80, txt=f"Total power produced: {total_power} W")

    fpdf.text(
        x=10, y=100, txt=f"Following graphs are gender split, age distribution and"
    )
    fpdf.text(x=10, y=110, txt=f"the average power and heart rate per user.")
    fpdf.output(f"/tmp/Report for {report_utils.get_today_date()}.pdf", dest="F")

    pdfs = [
        f"/tmp/Report for {report_utils.get_today_date()}.pdf",
        f"/tmp/{report_utils.get_today_date()}_gender_split_bar.pdf",
        f"/tmp/{report_utils.get_today_date()}_gender_split_pie.pdf",
        f"/tmp/{report_utils.get_today_date()}_age_split.pdf",
        f"/tmp/{report_utils.get_today_date()}_avg_heart_rate.pdf",
        f"/tmp/{report_utils.get_today_date()}_avg_power.pdf",
    ]

    for pdf_name in pdfs:
        if pdf_name != f"/tmp/Report for {report_utils.get_today_date()}.pdf":
            pdf_file = PyPDF2.PdfFileReader(pdf_name)
            page0 = pdf_file.getPage(0)
            page0.scaleBy(1.6)
            writer = PyPDF2.PdfFileWriter()
            writer.addPage(page0)
            with open(f"{pdf_name}", "wb+") as f:
                writer.write(f)

    merger = PyPDF2.PdfMerger()

    for pdf in pdfs:
        merger.append(pdf)

    merger.write(f"/tmp/24 Hour Report - {report_utils.get_today_date()}.pdf")
    merger.close()

    sender_ = f"{name} {sender_email}"
    recipients_ = [f"Recipient One {recipient_email}"]
    title_ = f"24 Hour Report: {report_utils.get_today_date()}"
    text_ = (
        "Amazon SES Test (Python)\r\n"
        "This email was sent with Amazon SES using the "
        "AWS SDK for Python (Boto)."
    )
    body_ = f"""<html>
<head></head>
<body>
<h1 style="color:#215d6e">Report of rides from previous day</h1>
<p style="color:#215d6e">Dear CEO, please find attached the report for the previous day.</p>
<p style="color:#215d6e">Kind regards, </p>
<p style="color:#215d6e">{name} </p>
</body>
</html>
            """
    attachments_ = [f"/tmp/24 Hour Report - {report_utils.get_today_date()}.pdf"]

    response_ = send_mail(sender_, recipients_, title_, text_, body_, attachments_)
    print(response_)

    return "Daily report sent to CEO"
