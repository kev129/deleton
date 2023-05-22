import dash
import dash_bootstrap_components as dbc
from dash import Dash, html

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.SOLAR])

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Home", href="/")),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem("More pages", header=True),
                dbc.DropdownMenuItem("Live Data", href="/Live_Data"),
                dbc.DropdownMenuItem("12 Hour Report", href="/12_Hour_Report"),
            ],
            nav=True,
            in_navbar=True,
            label="More",
        ),
    ],
    brand="Deloton",
    brand_href="/",
    color="dark",
    dark=True,
)

app.layout = html.Div([navbar, dash.page_container])

app.run_server(host="0.0.0.0", debug=True, port=8080)
