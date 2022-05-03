# learning dash 
# so i am using dash here

from news_scraper import GatherNews
from pandas_datareader import data as web
from dash import Input, Output
from dash import dcc, html, Dash, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime


app = Dash(__name__, external_stylesheets=[dbc.themes.SLATE])


@app.callback(
    Output(component_id='stock-fig', component_property='figure'),
    [Input(component_id='stock-ticker', component_property='value')]
)
def _stock_chart(ticker):

    ticker = str(ticker).upper()
    start = datetime.datetime(2022, 1, 1)
    end = datetime.datetime.now()

    df_stock = web.DataReader(ticker, 'yahoo', start, end)
    df_stock = df_stock.filter(regex='Adj Close')

    df_sp500 = web.DataReader('VOO', 'yahoo', start, end)
    df_sp500 = df_sp500.filter(regex='Adj Close')

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, shared_yaxes=False)
    fig.add_trace(go.Scatter({'x': df_stock.index, 'y': df_stock['Adj Close'], 'name': ticker}), 1, 1)
    fig.add_trace(go.Scatter({'x': df_sp500.index, 'y': df_sp500['Adj Close'], 'name': 'VOO'}), 2, 1)

    return fig


@app.callback(
    Output(component_id='news-table', component_property='children'),
    Input(component_id='stock-ticker', component_property='value')
)
def news_table(ticker):
    news = GatherNews(ticker=ticker).gather_news()

    return dash_table.DataTable(
        data=news.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in (news.columns)],
        style_table={'height': 300,
                     'overflowX': 'auto',
                     },
        style_cell={'textAlign': 'left',
                    'textOverflow': 'ellipsis',
                    'maxWidth': 0}

    )


@app.callback(
    Output(component_id='analyst-target', component_property='children'),
    Input(component_id='stock-ticker', component_property='value')
)
def analyst_table(ticker):

    ratings = GatherNews(ticker=ticker).analysts_targets()

    return dash_table.DataTable(
        data=ratings.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in (ratings.columns)],
        style_table={'height': 200,
                     'overflowX': 'auto',
                     'width': 900
                     },
        style_cell={'textAlign': 'left',
                    'textOverflow': 'ellipsis',
                    'maxWidth': 0}
    )


# input card
ticker_input = dbc.Card(
    [
        html.P('Enter a stock symbol'),
        dbc.Input(id='stock-ticker',
                  type='text',
                  value='amd',
                  size='sm',
                  debounce=True
                  )
    ]
)

# stock chart chart
stock_chart = dbc.Card([
    dcc.Graph(id='stock-fig', style=dict(display='inline',
                                         height=300))
])

# news card
news = dbc.Card([
    html.Div(id='news-table')
])

# analyst targets card
analyst_targets = dbc.Card([
    html.Div(id='analyst-target')
])

# combine all cards
cards = html.Div(
    [
        # top row
        dbc.Row([
            dbc.Col(ticker_input)
        ]),
        # mid row
        dbc.Row([
            dbc.Col(stock_chart),
            dbc.Col(news)
        ]),
        html.Br(),
        # bottom row
        dbc.Row([
            dbc.Col(analyst_targets)
        ])

    ]
)

app.layout = html.Div([cards])

if __name__ == '__main__':
    app.run_server(debug=False)