import pandas as pd
import plotly.express as px
from load_dash_app.dash_pipeline.dash_transform import (
    age_rides_df_with_bins,
    duration_by_age,
    id_gender_duration_df,
    time_power_df,
    total_duration_by_gender,
)


def rides_across_genders_pie(df: pd.DataFrame) -> px.pie:
    """Creates a pie chart that shows the number of rides taken by males and females

    Args:
        df (pd.DataFrame): number of rides and gender

    Returns:
        px.pie: proportion of rides taken by each gender
    """

    total_rides_by_gender = df[["ride_id", "gender"]].groupby("gender").nunique()

    return px.pie(
        total_rides_by_gender,
        values="ride_id",
        names=total_rides_by_gender.index,
        title="The Share of Rides Taken Across Genders",
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


def duration_by_gender_bar(df: pd.DataFrame) -> px.bar:
    """Creates a bar chart that shows the total duration of bike rides taken by males and females

    Args:
        df (pd.DataFrame): total duration for each gender

    Returns:
        px.bar: total duration by gender
    """

    return (
        px.bar(
            df,
            x=df.index,
            y="time_elapsed",
            title="Total Duration of Bike Rides by Gender",
            labels={"gender": "Gender", "time_elapsed": "Total Duration (Hours)"},
            text_auto=".2s",
        )
        .update_layout(title_font_color="#00898a", title_x=0.46)
        .update_traces(marker_color=["#215d6e", "#00898a"])
    )


def rides_across_age_groups_histogram(df: pd.DataFrame) -> px.histogram:
    """Creates a histogram showing the number of rides taken by different age groups

    Args:
        df (pd.DataFrame): Number of rides per age group

    Returns:
        px.histogram: Rides per age bucket
    """

    return (
        px.histogram(
            df,
            title="Total Number of Rides Demographs",
            x=df.index,
            y="ride_id",
            labels={"index": "Age (Years)"},
            text_auto=".2s",
        )
        .update_layout(
            yaxis_title="Total Number of Rides",
            title_font_color="#00898a",
            title_x=0.46,
        )
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
        )
    )


def duration_of_rides_by_age_histogram(df: pd.DataFrame) -> px.histogram:
    """Gets a histogram showing the total duration of rides taken by different age groups

    Args:
        df (pd.DataFrame): duration of rides per age bucket

    Returns:
        px.histogram: duration of Bike Rides Demographic
    """

    return (
        px.histogram(
            df,
            title="Duration of Bike Rides Demographic",
            x=df.index,
            y="time_elapsed",
            labels={"index": "Age (Years)"},
            text_auto=".2s",
        )
        .update_layout(
            yaxis_title="Total Duration of Rides Demographic (Hours)",
            title_font_color="#00898a",
            title_x=0.46,
        )
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
        )
    )


def power_output_by_hour_histogram(df: pd.DataFrame) -> px.histogram:
    """Gets a histogram showing the cumulative power output hourly

    Args:
        df (pd.DataFrame): time and power output

    Returns:
        px.histogram: hourly power output
    """

    return px.histogram(
        df,
        title="Hourly Power Output of Bikes",
        x="time",
        y="power",
        nbins=12,
        color_discrete_sequence=["#215d6e"],
        cumulative=True,
        text_auto=".2s",
    ).update_layout(
        yaxis_title="Total Power Output (Watts)",
        xaxis_title="Time",
        title_x=0.45,
        title_font_color="#00898a",
    )


def average_hourly_power_output(df: pd.DataFrame) -> px.histogram:
    """Gets a histogram showing the average hourly power output

    Args:
        df (pd.DataFrame): time and power output

    Returns:
        px.histogram: average hourly power output
    """

    return px.histogram(
        df,
        title="Average Power Outputs Hourly",
        x="time",
        y="power",
        nbins=12,
        histfunc="avg",
        color_discrete_sequence=["#215d6e"],
        text_auto=".2s",
    ).update_layout(
        xaxis_title="Time",
        title_x=0.5,
        yaxis_title="Average Power Output (Watts)",
        title_font_color="#00898a",
    )


fig1 = rides_across_genders_pie(id_gender_duration_df)
fig2 = duration_by_gender_bar(total_duration_by_gender)
fig3 = rides_across_age_groups_histogram(age_rides_df_with_bins)
fig4 = duration_of_rides_by_age_histogram(duration_by_age)
fig5 = power_output_by_hour_histogram(time_power_df)
fig6 = average_hourly_power_output(time_power_df)
