from statistics import mean
import functools
from dataclasses import dataclass
from google.cloud import bigquery
import yfinance as yf
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'app/dcf_portion/hello_google.json'


@dataclass
class DcfPrelim:
    '''Gather data from yfinance(API) to feed into a discount cash flow(DCF) model
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
        query_results = []
        for row in query_job:
            row_dict = {
                        'date': row['period'],
                        'ticker': row['ticker'],
                        'long_term_debt': row['long_term_debt'],
                        'total_stock_holder': row['total_stock_holder'],
                        'cash': row['cash'],
                        'long_term_invest': row['long_term_invest'],
                        'short_term_debt': row['short_term_debt']
                        }
            query_results.append(row_dict)

        return query_results

    @property
    def _yoy_grwoth_(self) -> float:
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
    def _operating_margin(self) -> float:
        '''
        caculate the average year over year operating margin of a
        company for last 4 years return a percentage number
        '''
        _revs = [rev['total_revenue'] for rev in self._income_statement]
        _op_inc = [income['operating_income'] for
                   income in self._income_statement]
        margin = [x/y for y, x in zip(_revs, _op_inc)]

        return mean(margin)

    @property
    def revenues(self):
        '''
        return last 2 years of revenue numbers of a company,
        these numbers should not be none
        '''
        thisRev = self._income_statement[0]['total_revenue']
        lastRev = self._income_statement[1]['total_revenue']

        return thisRev, lastRev

    @property
    def bookValueOfDebt(self):
        '''
        return last 2 years book value of debt for a company
        book_value_of_debt = long term debt

        '''
        thisBVOD = self._balance_sheet[0]['long_term_debt']
        lastBVOD = self._balance_sheet[1]['long_term_debt']

        return thisBVOD, lastBVOD

    @property
    def ebit(self):
        '''
        return ebit of a company for the last 2 years
        these numbers should not be none
        '''
        thisEbit = self._income_statement[0]['operating_income']
        lastEbit = self._income_statement[1]['operating_income']

        return thisEbit, lastEbit

    @property
    def bookValueOfEquity(self):
        '''
        return book value of equity, something on the balanced sheet
        '''
        thisBVOE = self._balance_sheet[0]['total_stock_holder']
        lastBVOE = self._balance_sheet[1]['total_stock_holder']

        return thisBVOE, lastBVOE

    @property
    def cash(self):
        '''
        return amount of cash on hand, typically including long term
        investments
        '''
        thisCash = (self._balance_sheet[0]['cash'] +
                    self._balance_sheet[0]['long_term_invest'])

        lastCash = (self._balance_sheet[1]['cash'] +
                    self._balance_sheet[1]['long_term_invest'])

        return thisCash, lastCash

    @property
    def tax_rate(self):
        '''
        return amount of income tax
        '''
        effTax = (self._income_statement[0]['income_tax_expense'] /
                  self._income_statement[0]['income_before_tax'])
        # if the tax rate is negative,
        # set it close to 0 to avoid divide by zero
        if effTax < 0:
            effTax = 0.00001
        return effTax

    @property
    def market_cap(self):
        return yf.Ticker(self.ticker).info['marketCap']

    @property
    def interests_expense(self):
        return self._income_statement[0]['interest_expense']

    @property
    def wacc(self):
        '''
        # WACC-weighted cost of capital
        # WACC = (E/V*Re) + (D/V*Rd*(1-Tc))
        '''
        beta = yf.Ticker(self.ticker).info['beta']

        Re = 0.0159 + beta * (0.10-0.0159)
        Rd = (self.interests_expense/self.bookValueOfDebt[0])*(1-0.21)
        wacc = ((self.market_cap/(self.market_cap+self.bookValueOfDebt[0])*Re) +
                (self.bookValueOfDebt[0]/(self.market_cap+self.bookValueOfDebt[0])*Rd*(1-self.tax_rate)))

        return wacc

    def input_fileds(self):
        # compile all sections to one method

        # revenues
        thisRev, lastRev = (self.revenues[0],
                            self.revenues[1])
        # EBIT or operating income
        thisEbit, lastEbit = (self.ebit[0], self.ebit[1])
        # book value of equity
        thisBVOE, lastBVOE = (self.bookValueOfEquity[0],
                              self.bookValueOfEquity[1])
        # book value of debt, most likely long term debt
        thisBVOD, lastBVOD = (self.bookValueOfDebt[0],
                              self.bookValueOfDebt[1])
        # Cash
        thisCash, lastCash = (self.cash[0], self.cash[1])
        # number of shares outstanding
        shares = yf.Ticker(self.ticker).info['sharesOutstanding']

        # effective tax rate
        effTax = self.tax_rate
        # sales to capital ratio
        shareholder_equity = self._balance_sheet[0]['total_stock_holder']
        sales_to_cap = thisRev/(thisBVOD + shareholder_equity - thisCash)

        return {'Revenues': [thisRev, lastRev],
                'EBIT': [thisEbit, lastEbit],
                'BVOE': [thisBVOE, lastBVOE],
                'BVOD': [thisBVOD, lastBVOD],
                'Cash': [thisCash, lastCash],
                'Shares': shares,
                'EffectiveTax': effTax,
                'sales_to_cap': sales_to_cap,
                'wacc': self.wacc,
                'growth_rate': self._yoy_grwoth_,
                'oper_margin': self._operating_margin
                }

if __name__ == '__main__':
    x = DcfPrelim('intc').input_fileds()
    print(x)