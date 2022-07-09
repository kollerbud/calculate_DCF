
import functools
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from dataclasses import dataclass
from api_keys import FMP, G_KEYS
from DCF_company_info import CompanyInfo
import requests

'''
to do:
    yfinance unreliable, will rewrite to pull data from FMP
'''


@dataclass
class DCF_DATA:
    '''Gather data from yfinance(API) to feed into a discount cash flow(DCF) model
    '''
    ticker: str

    def __post_init__(self):
        self.ticker = str(self.ticker).upper()

    def statements(self, statement):
        # pick statement
        if statement == 'income':
            link = 'https://financialmodelingprep.com/api/v3/'\
                   'income-statement/'\
                   f'{self.ticker}?limit=120&apikey={FMP.key}'
        elif statement == 'balance':
            link = 'https://financialmodelingprep.com/api/v3/'\
                   'balance-sheet-statement'\
                   f'/{self.ticker}?limit=120&apikey={FMP.key}'
        elif statement == 'cashflow':
            link = 'https://financialmodelingprep.com/api/v3/'\
                    'cash-flow-statement/'\
                    f'{self.ticker}?limit=120&apikey={FMP.key}'
        else:
            raise ValueError('choose "income", "balance" or "cashflow"')
        # get response
        response = requests.get(link)
        # checck if response is valid
        if response.status_code != 200:
            print('response error')
        # return json
        return response.json()

    @functools.cached_property
    def _income_statement(self) -> pd.DataFrame:
        '''
        pull income statement from API for a specific company,`
        and cacheing for later use
        '''
        json_response = self.statements(statement='income')
        fin = pd.DataFrame(
                json_response
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

        fin.rename({
                    'symbol': 'ticker',
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
                    'costOfRevenue': 'cost_of_revenue'
                    },
                   axis=1, inplace=True)
        fin['time_period'] = pd.to_datetime(fin['time_period']).dt.date

        return fin

    @functools.cached_property
    def _cash_flow_statement(self) -> pd.DataFrame:
        '''
        pull cash flow statement from API for a specific company,
        and cacheing for later use
        '''
        json_response = self.statements(statement='cashflow')
        cash = pd.DataFrame(
            json_response
        )
        keep_cols = ['date', 'symbol',
                     'capitalExpenditure',
                     'depreciationAndAmortization',
                     'changeInWorkingCapital']
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
        json_response = self.statements(statement='balance')
        balance = pd.DataFrame(
            json_response
        )
        balance['capex'] = self._cash_flow_statement['capitalExpenditure']
        balance['depreciation'] = (
            self._cash_flow_statement['depreciationAndAmortization']
        )
        balance['changeNWC'] = (
            self._cash_flow_statement['changeInWorkingCapital']
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
        balance.rename({
                        'symbol': 'ticker',
                        'date': 'period',
                        'longTermDebt': 'long_term_debt',
                        'totalStockholdersEquity': 'total_stock_holder',
                        'cashAndShortTermInvestments': 'cash',
                        'longTermInvestments': 'long_term_invest',
                        'shortTermDebt': 'short_term_debt',
                        'totalAssets': 'total_asset',
                        'totalLiabilities': 'total_liab',
                        'changeNWC': 'change_in_nwc'
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
        df_income = self._balanced_sheet
        client.load_table_from_dataframe(
            df_income, table_id,
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
    client = bigquery.Client()
    query_job = client.query(
            query=query_string,
            job_config=job_configs
            )
    query_job.result()
    print(f'finished {ticker}')


def schema_check(check_table):
    'schema test'
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id=G_KEYS.dataset, project=G_KEYS.project)
    table_ref = dataset_ref.table(table_id=check_table)
    table = client.get_table(table_ref)
    return [
        '{0}'.format(
            schema.name
        ) for schema in table.schema
    ]
    

if __name__ == '__main__':
    None
    
    #ticker_list = ['sq', 'net', 'amd', 'nvda', 'snow', 'axp', 'msft', 'intc', 'gs', 'abt','qcom', 'mdt']
    #ticker_list = ['txn', 'mu', 'on']
    #for t in ticker_list:
    #    t = t.upper()
    #    
    #    for i in DCF_DATA(ticker=t).statements('balance'):
    #        update_query(col_value=i['cashAndShortTermInvestments'],
    #                     date=i['date'],
    #                     ticker=t
    #                     )

    
    #print(DCF_DATA('NVDA').statements('balance'))