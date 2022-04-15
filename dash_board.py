# learning dash 
# so i am using dash here

from news_scraper import GatherNews
from DCF_Input import DCF_DATA
import pandas as pd
import pandas_datareader import data as web
import dash
from dash import Input, Output
from dash import dcc, html, Dash
import datetime


app = Dash(__name__)

app.layout = html.Div(children=[
    html.H1()
])


if __name__ == '__main__':
    None