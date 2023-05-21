import dash
from dash import html

dash.register_page(__name__, path="/")

layout = html.Div(
    children=[
        html.Img(id="logo_img", src="../assets/images/Deloton_Clear.png"),
        html.H1("Welcome To The Deloton Dashboard!", style={"textAlign": "center"}),
        html.Div(
            children="""
        This website contains live data of the current ride and the 12 hour historical data.
    """,
            style={"textAlign": "center"},
        ),
    ]
)
