import functools
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
    'app/dcf_portion/hello_google.json')


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
        pull income statement from API for a specific company,`
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

        fin.rename({'period': 'time_period',
                    'Income Before Tax': 'income_before_tax',
                    'Research Development': 'research_development',
                    'Net Income': 'net_income',
                    'Selling General Administrative': 'sga',
                    'Gross Profit': 'gross_profit',
                    'Interest Expense': 'interest_expense',
                    'Operating Income': 'operating_income',
                    'Income Tax Expense': 'income_tax_expense',
                    'Total Revenue': 'total_revenue',
                    'Cost Of Revenue': 'cost_of_revenue'
                    },
                   axis=1, inplace=True)
        fin['period'] = pd.to_datetime(fin['period']).dt.date

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
        keep_cols = ['Capital Expenditures', 'Depreciation']
        cash = cash[keep_cols]
        cash = cash.fillna(0)

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
        balance['capex'] = self._cash_flow_statement['Capital Expenditures']
        balance['depreciation'] = self._cash_flow_statement['Depreciation']
        # trim some extra columns off
        if 'Long Term Debt' not in balance.columns:
            balance.loc[:, 'Long Term Debt'] = 0
        if 'Short Long Term Debt' not in balance.columns:
            balance.loc[:, 'Short Long Term Debt'] = 0
        if 'Long Term Investments' not in balance.columns:
            balance.loc[:, 'Long Term Investments'] = 0

        keep_cols = ['ticker', 'period', 'Long Term Debt',
                     'Total Stockholder Equity', 'Cash',
                     'Long Term Investments', 'Short Long Term Debt',
                     'capex', 'Total Liab', 'Total Assets', 'depreciation'
                     ]
        balance = balance[keep_cols]
        balance.rename({'Long Term Debt': 'long_term_debt',
                        'Total Stockholder Equity': 'total_stock_holder',
                        'Cash': 'cash',
                        'Long Term Investments': 'long_term_invest',
                        'Short Long Term Debt': 'short_term_debt',
                        'Total Assets': 'total_asset',
                        'Total Liab': 'total_liab'
                        },
                       axis=1, inplace=True)
        balance = balance.fillna(0)
        balance['period'] = pd.to_datetime(balance['period']).dt.date

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
    def upload_incomeStatement(self) -> str:
        client = bigquery.Client()
        table_id = 'all_data.income_statement'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('ticker', 'STRING'),
                bigquery.SchemaField('time_period', 'DATE'),
                bigquery.SchemaField('research_development', 'FLOAT')
            ]
        )
        df_income = self._income_statement
        client.load_table_from_dataframe(
            df_income,
            table_id,
            job_config=job_configs
        )

        return f'uploaded {self.ticker} income statement'

    @property
    def upload_balanceSheet(self) -> str:
        client = bigquery.Client()
        table_id = 'all_data.balance_sheet'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('ticker', 'STRING'),
                bigquery.SchemaField('period', 'DATE'),
                bigquery.SchemaField('total_stock_holder', 'FLOAT')
            ]
        )
        df_income = self._balanced_sheet
        client.load_table_from_dataframe(
            df_income, table_id,
            job_config=job_configs
        )

        return f'uploaded {self.ticker} balance sheet'


def update_query(capex, date, ticker):
    '''
    use to update data when new columns are
    put in the table
    '''
    query_string = '''
        update all_data.balance_sheet
        set depreciation = ?
        where period = ? and ticker = ?
    '''
    job_configs = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, 'FLOAT', capex),
            bigquery.ScalarQueryParameter(None, 'DATE', date),
            bigquery.ScalarQueryParameter(None, 'STRING', ticker)
        ]
    )
    # run query
    client = bigquery.Client()
    query_job = client.query(
            query=query_string,
            job_config=job_configs
            )
    query_job.result()
    print(f'finished {ticker}')


if __name__ == '__main__':
    pass
