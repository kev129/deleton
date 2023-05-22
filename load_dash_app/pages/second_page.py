import sys

import dash
from dash import Input, Output, callback, dcc, html

sys.path.append("..")
from utils.dash_app_dash_pipeline_dash_transform_utils import (
    fig1,
    fig2,
    fig3,
    fig4,
    fig5,
    fig6,
)

dash.register_page(__name__, path="/12_Hour_Report")

layout = html.Div(
    children=[
        html.Div(
            children=[
                dcc.Dropdown(
                    [
                        "Share of Rides Across Genders",
                        "Duration of Rides Across Genders",
                    ],
                    "Share of Rides Across Genders",
                    id="data-input-1",
                ),
                dcc.Dropdown(
                    [
                        "Cumulative Power Output of Bikes",
                        "Average Power Output of Bikes",
                    ],
                    "Cumulative Power Output of Bikes",
                    id="data-input-3",
                ),
            ]
        ),
        html.Div(
            children=[
                dcc.Graph(
                    id="graph-output-1",
                    style={"display": "inline-block", "width": "50%"},
                ),
                dcc.Graph(
                    id="graph-output-3",
                    style={"display": "inline-block", "width": "50%"},
                ),
            ]
        ),
        html.Div(
            [
                dcc.Dropdown(
                    [
                        "Share of Rides Across Age Groups",
                        "Duration of Rides Across Age Groups",
                    ],
                    "Duration of Rides Across Age Groups",
                    id="data-input-2",
                )
            ]
        ),
        dcc.Graph(id="graph-output-2"),
    ]
)


@callback(
    Output("graph-output-1", "figure"),
    Input("data-input-1", "value"),
    suppress_callback_exceptions=True,
)
def update_graph(value):
    if value == "Share of Rides Across Genders":
        return fig1
    return fig2


@callback(
    Output("graph-output-2", "figure"),
    Input("data-input-2", "value"),
    suppress_callback_exceptions=True,
)
def update_graph(value):
    if value == "Share of Rides Across Age Groups":
        return fig3
    return fig4


@callback(
    Output("graph-output-3", "figure"),
    Input("data-input-3", "value"),
    suppress_callback_exceptions=True,
)
def update_graph(value):
    if value == "Cumulative Power Output of Bikes":
        return fig5
    return fig6
