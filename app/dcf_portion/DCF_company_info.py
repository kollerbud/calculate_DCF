import yfinance as yf
from dataclasses import dataclass
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
    'app/dcf_portion/hello_google.json')


@dataclass
class CompanyInfo:
    '''
    grab company info and some summarized
    financial metrics from yfinance
    and upload to bq

    for whatever reason json object is impossible to upload

    '''
    ticker: str

    def __post_init__(self):
        self.ticker = str(self.ticker).upper()

    @property
    def info(self) -> pd.DataFrame:
        _info = yf.Ticker(self.ticker).info

        df_info = pd.DataFrame([{
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
        client = bigquery.Client()
        table_id = 'all_data.ticker_info'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('market_cap', 'FLOAT'),
                bigquery.SchemaField('shares_outstanding', 'FLOAT'),
                bigquery.SchemaField('current_date', 'DATE'),
                bigquery.SchemaField('stock_price', 'FLOAT'),
                bigquery.SchemaField('ticker', 'STRING')
            ],
        )
        df = self.info
        client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_configs
        ).result()

        return f'uploaded {self.ticker} to ticker info'


if __name__ == '__main__':
    print(CompanyInfo('snow').upload_info_to_bq)
