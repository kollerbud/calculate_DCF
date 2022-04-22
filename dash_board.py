# learning dash 
# so i am using dash here

from news_scraper import GatherNews
from pandas_datareader import data as web
from dash import Input, Output
from dash import dcc, html, Dash, dash_table
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime


app = Dash(__name__)

@app.callback(
    Output(component_id='output-figure', component_property='figure'),
    [Input(component_id='stock-ticker', component_property='value')]
)
def _stock_chart(ticker):

    ticker = str(ticker).upper()
    start = datetime.datetime(2022,1,1)
    end = datetime.datetime.now()

    df_stock = web.DataReader(ticker, 'yahoo', start, end)
    df_stock = df_stock.filter(regex='Adj Close')

    df_sp500 = web.DataReader('VOO', 'yahoo', start, end)
    df_sp500 = df_sp500.filter(regex='Adj Close')

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, shared_yaxes=False)
    fig.add_trace(go.Scatter({'x':df_stock.index, 'y':df_stock['Adj Close'], 'name': ticker}),1,1)
    fig.add_trace(go.Scatter({'x':df_sp500.index, 'y':df_sp500['Adj Close'], 'name': 'VOO'}),2,1)

    return fig


@app.callback(
    Output(component_id='news_list', component_property='children'),
    Input(component_id='stock-ticker', component_property='value')
)
def news_table(ticker):

    news = GatherNews(ticker=ticker).gather_news()

    return dash_table.DataTable(
        data=news.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in (news.columns)],
        style_table={'height': 300,
                     'overflowX': 'auto'
                     },
        style_cell={'textAlign': 'left',
                    'textOverflow': 'ellipsis',
                    'maxWidth': 0}

    )


app.layout = html.Div([
    html.H1('CANT THINK OF A PUNTY TITLE'),
    html.Div([
        dcc.Input(id='stock-ticker', value='nvda', type='text', debounce=True)
    ], style={'height': 22,
              'display': 'flex',
              'margin-left': 30}),

    # flex container
    html.Div([
        # graph container
        html.Div([
            dcc.Graph(id='output-figure', style={'display': 'inline-block',
                                                 'width': '100%',
                                                 'height': 500,
                                                 'margin-top': 0}),
        ]),
        # table container
        html.Div(
            id='news_list', style={'display': 'inline-block',
                                    'width': '60%',
                                    'height': 500,
                                    'margin-top': 100
                                    }
        )
    ], style={'display':'flex'})

])


if __name__ == '__main__':
    #print(GatherNews('NVDA').gather_news().to_dict('records'))

    app.run_server(debug=False)