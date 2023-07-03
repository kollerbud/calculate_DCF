'''pull overview data of the ticker'''
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
import yfinance as yf
from goog_auth import gcp_credentials


class CompanyOverviewInfo:
    '''
    grab company info and some summarized
    financial metrics from yfinance
    and upload to bq

    for whatever reason json object is impossible to upload

    '''

    def __init__(self, ticker) -> None:
        self.ticker = str(ticker).upper()

    @property
    def info(self) -> pd.DataFrame:
        '''pull company info'''
        _info = yf.Ticker(self.ticker).info

        df_info = pd.DataFrame([{
                'uuid': _info['uuid'],
                'ticker': self.ticker,
                'market_cap': _info['marketCap'],
                'beta': _info['beta'],
                'shares_outstanding': _info['sharesOutstanding'],
                'sector': _info['sector'],
                'industry': _info['industry'],
                'business_summary': _info['longBusinessSummary'],
                'current_date': datetime.now().date().strftime('%Y-%m-%d'),
                'stock_price': _info['currentPrice']
                }])
        df_info['current_date'] = pd.to_datetime(df_info['current_date'])
        df_info.fillna(0)
        float_col = ['market_cap', 'beta', 'shares_outstanding', 'stock_price']
        df_info[float_col] = df_info[float_col].astype(float)
        return df_info

    @property
    def upload_info_to_bq(self) -> str:
        client = bigquery.Client(credentials=gcp_credentials())
        table_id = 'all_data.ticker_info'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('uuid', 'STRING'),
                bigquery.SchemaField('market_cap', 'FLOAT'),
                bigquery.SchemaField('shares_outstanding', 'FLOAT'),
                bigquery.SchemaField('current_date', 'DATE'),
                bigquery.SchemaField('stock_price', 'FLOAT'),
                bigquery.SchemaField('ticker', 'STRING'),
                bigquery.SchemaField('beta', 'FLOAT'),
            ],
        )

        df_info = self.info

        client.load_table_from_dataframe(
            df_info,
            table_id,
            job_config=job_configs
        ).result()

        return f'uploaded {self.ticker} to ticker info'
