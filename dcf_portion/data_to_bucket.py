import functools
import yfinance as yf
import pandas as pd
from google.cloud import storage
from api_keys import G_KEYS
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dcf-model-project-89fcb0a775c4.json'


class DCF_DATA:
    '''Gather data from yfinance(API) to feed into a discount cash flow(DCF) model
    '''
    def __init__(self, ticker: str) -> None:
        self.ticker = str(ticker).upper()
        self.ticker_check

    @property
    def ticker_check(self):
        price_info = yf.Ticker(self.ticker).info['regularMarketPrice']
        if price_info is None:
            raise ValueError('invalid ticker')

    @functools.cached_property
    def _income_statement(self) -> pd.DataFrame:
        '''
        pull income statement from API for a specific company,
        and cacheing for later use
        '''
        fin = yf.Ticker(self.ticker).financials
        fin = fin.T
        fin['ticker'] = self.ticker
        fin.reset_index(inplace=True)
        fin.rename(columns={fin.columns[0]: 'period'}, inplace=True)

        # trim some extra columns off
        keep_cols = ['ticker', 'period', 'Income Before Tax',
                     'Research Development', 'Net Income',
                     'Selling General Administrative',
                     'Gross Profit', 'Interest Expense', 'Operating Income',
                     'Income Tax Expense', 'Total Revenue', 'Cost Of Revenue']
        fin = fin[keep_cols]
        fin = fin.fillna(0)

        return fin

    @functools.cached_property
    def _cash_flow_statement(self) -> pd.DataFrame:
        '''
        pull cash flow statement from API for a specific company,
        and cacheing for later use
        '''
        cash = yf.Ticker(self.ticker).cashflow
        cash = cash.T
        cash['ticker'] = self.ticker
        cash.reset_index(inplace=True)
        cash.rename(columns={cash.columns[0]: 'period'}, inplace=True)

        # trim some extra columns off
        # nothing use in the calculation yet, off for now
        return cash

    @functools.cached_property
    def _balanced_sheet(self) -> pd.DataFrame:
        '''
        pull balance sheet statement from API for a specific company,
        and cacheing for later use
        '''
        balance = yf.Ticker(self.ticker).balance_sheet
        balance = balance.T
        balance['ticker'] = self.ticker
        balance.reset_index(inplace=True)
        balance.rename(columns={balance.columns[0]: 'period'}, inplace=True)
        # trim some extra columns off
        keep_cols = ['ticker', 'period', 'Long Term Debt',
                     'Total Stockholder Equity',
                     'Cash', 'Long Term Investments', 'Short Long Term Debt']
        balance = balance[keep_cols]
        balance = balance.fillna(0)
        return balance

    @functools.cached_property
    def _yf_info(self):
        '''
        grab general info of a company
        '''
        'check if "beta" value is contained inside info'
        ticker_info = yf.Ticker(self.ticker).info
        if ticker_info['beta'] is None:
            ticker_info['beta'] = 0
        df_info = pd.DataFrame(ticker_info)

        return df_info

    @property
    def generate_csv(self):
        self._income_statement.to_csv(f'{self.ticker}_income_statement.csv',
                                      index=None)

        self._balanced_sheet.to_csv(f'{self.ticker}_balance_sheet.csv',
                                    index=None)

        self._cash_flow_statement.to_csv(f'{self.ticker}_cashflow.csv',
                                         index=None)

        return None


def upload_to_bucket(bucket_name, tickers=None):

    # generate csv files
    for ticker in tickers:
        DCF_DATA(ticker=ticker).generate_csv
    csv_files = [filename for filename in os.listdir() if filename.endswith('.csv')]
    # locate bucket
    bucket = storage.Client().bucket(bucket_name=bucket_name)
    # upload csv files:
    for upFile in csv_files:
        blob = bucket.blob(upFile)
        blob.upload_from_filename(upFile)

    return blob.public_url


if __name__ == '__main__':
    bucket = G_KEYS.bucket
    upload_to_bucket(bucket_name=bucket, tickers=['abt', 'amd', 'msft', 'nvda', 'sq'])