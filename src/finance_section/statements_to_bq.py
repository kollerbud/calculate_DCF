'''using REST API to gather financial informations'''
import functools
import pandas as pd
import requests
from google.cloud import bigquery
from goog_auth import gcp_credentials


'''
shoutout to https://discountingcashflows.com/
'''


class FinancialsToBigquery:
    '''
    query financial statements from API(discountingcashflows.com)
    and store it to Bigquery
    '''

    def __init__(self, ticker: str) -> None:
        self.ticker = str(ticker).upper()

    def _statements(self, statement_type: str):
        # pick statement
        if statement_type == 'income':
            link = f'https://discountingcashflows.com/api/income-statement/{self.ticker}/'

        elif statement_type == 'balance':
            link = f'https://discountingcashflows.com/api/balance-sheet-statement/{self.ticker}/'

        elif statement_type == 'cashflow':
            link = f'https://discountingcashflows.com/api/cash-flow-statement/{self.ticker}/'

        else:
            raise ValueError('choose "income", "balance" or "cashflow"')
        # get response
        agent_header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0'
            }
        response = requests.get(link, headers=agent_header, timeout=10)
        # checck if response is valid
        if response.status_code != 200:
            print('response error')
        # return json
        return response.json()['report']

    @functools.cached_property
    def _income_transform(self) -> pd.DataFrame:
        '''
        transforming data income statement data from API
        '''
        fin = pd.DataFrame(
                self._statements(statement_type='income')
            )
        # trim some extra columns off
        keep_cols = ['symbol', 'date', 'incomeBeforeTax',
                     'researchAndDevelopmentExpenses', 'netIncome',
                     'sellingGeneralAndAdministrativeExpenses',
                     'grossProfit', 'interestExpense', 'operatingIncome',
                     'incomeTaxExpense', 'revenue', 'costOfRevenue'
                     ]
        fin = fin[keep_cols]
        fin = fin.fillna(0)
        fin.rename(
            {'symbol': 'ticker',
             'date': 'time_period',
             'incomeBeforeTax': 'income_before_tax',
             'researchAndDevelopmentExpenses': 'research_development',
             'netIncome': 'net_income',
             'sellingGeneralAndAdministrativeExpenses': 'sga',
             'grossProfit': 'gross_profit',
             'interestExpense': 'interest_expense',
             'operatingIncome': 'operating_income',
             'incomeTaxExpense': 'income_tax_expense',
             'revenue': 'total_revenue',
             'costOfRevenue': 'cost_of_revenue'},
            axis=1, inplace=True)
        fin['time_period'] = pd.to_datetime(fin['time_period']).dt.date

        return fin

    @functools.cached_property
    def _cashflow_transform(self) -> pd.DataFrame:
        '''
        transform cashflow statement from API
        '''
        cash = pd.DataFrame(
            self._statements(statement_type='cashflow')
        )
        keep_cols = ['date', 'symbol',
                     'capitalExpenditure',
                     'depreciationAndAmortization',
                     'changeInWorkingCapital']
        cash = cash[keep_cols]
        cash = cash.fillna(0)

        return cash

    @functools.cached_property
    def _balanced_transform(self) -> pd.DataFrame:
        '''
        transform balanced sheets from API
        '''
        balance = pd.DataFrame(
            self._statements(statement_type='balance')
        )
        balance['capex'] = self._cashflow_transform['capitalExpenditure']
        balance['depreciation'] = (
            self._cashflow_transform['depreciationAndAmortization']
        )
        balance['changeNWC'] = (
            self._cashflow_transform['changeInWorkingCapital']
            )
        # trim some extra columns off
        if 'Long Term Debt' not in balance.columns:
            balance.loc[:, 'Long Term Debt'] = 0
        if 'Short Long Term Debt' not in balance.columns:
            balance.loc[:, 'Short Long Term Debt'] = 0
        if 'Long Term Investments' not in balance.columns:
            balance.loc[:, 'Long Term Investments'] = 0

        keep_cols = ['symbol', 'date', 'longTermDebt',
                     'totalStockholdersEquity', 'cashAndShortTermInvestments',
                     'longTermInvestments', 'shortTermDebt',
                     'capex', 'totalLiabilities', 'totalAssets',
                     'depreciation', 'changeNWC'
                     ]
        balance = balance[keep_cols]
        balance.rename(
            {'symbol': 'ticker',
             'date': 'period',
             'longTermDebt': 'long_term_debt',
             'totalStockholdersEquity': 'total_stock_holder',
             'cashAndShortTermInvestments': 'cash',
             'longTermInvestments': 'long_term_invest',
             'shortTermDebt': 'short_term_debt',
             'totalAssets': 'total_asset',
             'totalLiabilities': 'total_liab',
             'changeNWC': 'change_in_nwc'},
            axis=1, inplace=True)

        balance = balance.fillna(0)
        balance['period'] = pd.to_datetime(balance['period']).dt.date

        return balance

    @property
    def income_to_bigquery(self) -> str:
        'upload income statement to Bigquery table'
        client = bigquery.Client(credentials=gcp_credentials())
        table_id = 'all_data.income_statement'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('ticker', 'STRING'),
                bigquery.SchemaField('time_period', 'DATE'),
                bigquery.SchemaField('research_development', 'FLOAT'),
                bigquery.SchemaField('income_before_tax', 'FLOAT'),
                bigquery.SchemaField('net_income', 'FLOAT'),
                bigquery.SchemaField('sga', 'FLOAT'),
                bigquery.SchemaField('gross_profit', 'FLOAT'),
                bigquery.SchemaField('interest_expense', 'FLOAT'),
                bigquery.SchemaField('operating_income', 'FLOAT'),
                bigquery.SchemaField('income_tax_expense', 'FLOAT'),
                bigquery.SchemaField('total_revenue', 'FLOAT'),
                bigquery.SchemaField('cost_of_revenue', 'FLOAT'),
            ]
        )
        _df = self._income_transform
        client.load_table_from_dataframe(
            _df,
            table_id,
            job_config=job_configs
        )

        return f'uploaded {self.ticker} income statement'

    @property
    def balanced_to_bigquery(self) -> str:
        'upload balanced sheet to Bigquery'
        client = bigquery.Client(credentials=gcp_credentials())
        table_id = 'all_data.balance_sheet'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('ticker', 'STRING'),
                bigquery.SchemaField('period', 'DATE'),
                bigquery.SchemaField('total_stock_holder', 'FLOAT'),
                bigquery.SchemaField('long_term_debt', 'FLOAT'),
                bigquery.SchemaField('cash', 'FLOAT'),
                bigquery.SchemaField('short_term_debt', 'FLOAT'),
                bigquery.SchemaField('long_term_invest', 'FLOAT'),
                bigquery.SchemaField('capex', 'FLOAT'),
                bigquery.SchemaField('total_asset', 'FLOAT'),
                bigquery.SchemaField('total_liab', 'FLOAT'),
                bigquery.SchemaField('depreciation', 'float'),
                bigquery.SchemaField('change_in_nwc', 'float'),
            ]
        )
        _df = self._balanced_transform
        client.load_table_from_dataframe(
            _df, table_id,
            job_config=job_configs
        )

        return f'uploaded {self.ticker} balance sheet'


def update_query(col_value, date, ticker):
    '''
    use to update data when new columns are
    put in the table
    '''
    query_string = '''
        update all_data.balance_sheet
        set cash = ?
        where period = ? and ticker = ?
    '''
    job_configs = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, 'FLOAT', col_value),
            bigquery.ScalarQueryParameter(None, 'DATE', date),
            bigquery.ScalarQueryParameter(None, 'STRING', ticker)
        ]
    )
    # run query
    client = bigquery.Client(gcp_credentials())
    query_job = client.query(
            query=query_string,
            job_config=job_configs
            )
    query_job.result()
    print(f'finished {ticker}')


if __name__ == '__main__':
    print(FinancialsToBigquery(ticker='nvda')._balanced_transform)