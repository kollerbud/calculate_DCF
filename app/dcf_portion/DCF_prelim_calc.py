from statistics import mean
import functools
from dataclasses import dataclass
from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'app/dcf_portion/'\
                                               'hello_google.json'


@dataclass
class DCFDataInput:
    '''Gather data from yfinance(API) to feed into a discount cash flow(DCF) model

        output for everything need for a DCF calculation
    '''
    ticker: str
    client = bigquery.Client()

    def __post_init__(self):
        'check if ticker has info in bigquery'

        # make ticker upper case
        self.ticker = str(self.ticker).upper()
        # query string for bigquery
        query_str = '''
                SELECT ticker
                FROM `all_data.income_statement`
                WHERE ticker = ?
                '''
        # query config
        job_configs = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, 'STRING', self.ticker)
            ]
        )
        # run query
        query_job = self.client.query(query=query_str,
                                      job_config=job_configs)
        # check point there to see if query returns anything
        query_result = [row for row in query_job]
        if len(query_result) == 0:
            # error is ticker is not in bigquery
            raise ValueError(f'no data of {self.ticker}, upload it first')

    @functools.cached_property
    def _income_statement(self):
        query_str = '''
                SELECT *
                FROM all_data.income_statement
                WHERE ticker = ?
                ORDER BY time_period desc
                ;
                '''
        job_configs = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, 'STRING', self.ticker)
            ]
        )
        # run query
        query_job = self.client.query(query=query_str,
                                      job_config=job_configs)
        query_job.result()
        print('ran income statement query')
        query_results = []
        for row in query_job:
            row_dict = {
                        'date': row['time_period'],
                        'ticker': row['ticker'],
                        'income_before_tax': row['income_before_tax'],
                        'research_development': row['research_development'],
                        'net_income': row['net_income'],
                        'sga': row['sga'],
                        'gross_profit': row['gross_profit'],
                        'interest_expense': row['interest_expense'],
                        'operating_income': row['operating_income'],
                        'income_tax_expense': row['income_tax_expense'],
                        'total_revenue': row['total_revenue'],
                        'cost_of_revenue': row['cost_of_revenue']
                        }
            query_results.append(row_dict)

        return query_results

    @functools.cached_property
    def _balance_sheet(self):
        query_str = '''
                SELECT *
                FROM all_data.balance_sheet
                WHERE ticker = ?
                ORDER BY period desc
                ;
                '''
        job_configs = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, 'STRING', self.ticker)
            ]
        )
        # run query
        query_job = self.client.query(query=query_str,
                                      job_config=job_configs)
        query_job.result()
        print('ran balance sheet query')
        query_results = []
        for row in query_job:
            row_dict = {
                        'date': row['period'],
                        'ticker': row['ticker'],
                        'long_term_debt': row['long_term_debt'],
                        'total_stock_holder': row['total_stock_holder'],
                        'cash': row['cash'],
                        'long_term_invest': row['long_term_invest'],
                        'short_term_debt': row['short_term_debt'],
                        'capex': row['capex'],
                        'net_working_cap': (
                            row['total_asset'] - row['total_liab']
                            ),
                        'depreciation': row['depreciation']
                        }
            query_results.append(row_dict)

        return query_results

    @functools.cached_property
    def ticker_info(self):
        query_str = '''
            SELECT *
            FROM all_data.ticker_info
            WHERE ticker = ?
            '''
        job_configs = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, 'STRING', self.ticker)
            ]
        )
        # run query
        query_job = self.client.query(query=query_str,
                                      job_config=job_configs)
        query_job.result()
        dict_info = {}
        for row in query_job:
            dict_info['market_cap'] = row['market_cap']
            dict_info['beta'] = row['beta']
            dict_info['shares_outstanding'] = row['shares_outstanding']
            dict_info['sector'] = row['sector']
            dict_info['industry'] = row['industry']
            dict_info['business_summary'] = row['business_summary']
            dict_info['current_data'] = row['current_date']
            dict_info['stock_price'] = row['stock_price']

        return dict_info

    @property
    def yoy_grwoth_(self) -> float:
        '''
        calculate the average year over year revenue growth for last 4 years
        of a company, return a percentage number
        '''
        income_resp = [rev['total_revenue'] for
                       rev in self._income_statement]
        mean_growth = mean([(x-y)/y for x, y in
                            zip(income_resp[:-1], income_resp[1:])])

        return mean_growth

    @property
    def operating_margin(self) -> float:
        '''
        caculate the average year over year operating margin of a
        company for last 4 years return a percentage number
        '''
        _revs = [rev['total_revenue'] for rev in self._income_statement]
        _op_inc = [income['operating_income'] for
                   income in self._income_statement]
        margin = [x/y for y, x in zip(_revs, _op_inc)]

        return margin

    @property
    def tax_rate(self) -> float:
        '''
        calculate the average tax rate
        '''
        ebit = self.ebit
        tax = self.taxes
        return [x/y for x, y in zip(tax, ebit)]

    @property
    def revenues(self) -> list:
        '''
        return a list all (4) revenue numbers of a company,
        these numbers should not be none
        '''
        return [rev['total_revenue'] for rev in self._income_statement]

    @property
    def ebit(self) -> list:
        '''
        return ebit of a company for the last 4 years
        these numbers should not be none
        '''
        return [ebit['operating_income'] for ebit in self._income_statement]

    @property
    def taxes(self) -> list:
        '''
        return all taxes paid of a company for 4 years
        should be a list
        '''
        return [tax['income_tax_expense'] for tax in self._income_statement]

    @property
    def depreciation(self) -> list:
        '''
        return list of all depreciation

        '''
        return [depre['depreciation'] for depre in self._balance_sheet]

    @property
    def capex(self) -> list:
        '''
        return capex
        '''
        return [capx['capex'] for capx in self._balance_sheet]

    @property
    def nwc(self) -> list:
        'return net working capital'
        return [nwc['net_working_cap'] for nwc in self._balance_sheet]

    @property
    def cash_minus_debt(self) -> float:
        '''
        latest cash minus debt
        use for equity value calculation
        '''
        return (self._balance_sheet[0]['cash'] -
                self._balance_sheet[0]['long_term_debt']
                )

    def input_fileds(self):
        # compile all sections to one method
        return {
            'revenues': self.revenues,
            'ebit': self.ebit,
            'taxes': self.taxes,
            'depreciation': self.depreciation,
            'capex': self.capex,
            'netWorkCap': self.nwc,
            'cashMinusDebt': self.cash_minus_debt,
            'yoyGrowth': self.yoy_grwoth_,
            'operMargin': self.operating_margin,
            'tax_rate': self.tax_rate
        }


if __name__ == '__main__':
    print(DCFDataInput('sq').input_fileds())
