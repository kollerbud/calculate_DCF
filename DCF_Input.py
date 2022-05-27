from statistics import mean
import functools
import yfinance as yf
import requests_cache
import pandas as pd


class DCF_DATA:
    '''Gather data from yfinance(API) to feed into a discount cash flow(DCF) model
    '''

    _header = ('Mozilla/5.0 (Windows NT 10.0;  Win64; x64; rv:91.0)' +
               'Gecko/20100101 Firefox/91.0')
    _req_session = requests_cache.CachedSession('yfinance.cache')
    _req_session.headers['User-agent'] = _header

    def __init__(self, ticker: str) -> None:
        self.ticker = str(ticker).upper()
        self.session = DCF_DATA._req_session

    @functools.cached_property
    def _income_statement(self) -> pd.Dataframe:
        '''
        pull income statement from API for a specific company, 
        and cacheing for later use
        '''
        fin = yf.Ticker(self.ticker, session=self.session).financials
        fin = fin.T
        return fin

    @functools.cached_property
    def _cash_flow_statement(self) -> pd.DataFrame:
        '''
        pull cash flow statement from API for a specific company, 
        and cacheing for later use
        '''
        cash = yf.Ticker(self.ticker, session=self.session).cashflow
        cash = cash.T
        return cash

    @functools.cached_property
    def _balanced_sheet(self) -> pd.DataFrame:
        '''
        pull balance sheet statement from API for a specific company, 
        and cacheing for later use
        '''
        balance = yf.Ticker(self.ticker, session=self.session).balance_sheet
        balance = balance.T
        return balance

    @functools.cached_property
    def _yf_info(self):
        '''
        grab general info of a company
        '''
        'check if "beta" value is contained inside info'
        if yf.Ticker(self.ticker, session=self.session).info['beta'] is None:
            raise ValueError('beta value missing')
        
        return yf.Ticker(self.ticker, session=self.session).info

    @property
    def _yoy_grwoth_(self) -> float:
        '''
        calculate the average year over year revenue growth for last 4 years
        of a company, return a percentage number
        '''
        income_resp = self._income_statement['Total Revenue']
        mean_growth = mean([(x-y)/y for x, y in
                            zip(income_resp[:-1], income_resp[1:])])

        return mean_growth

    @property
    def _operating_margin(self) -> float:
        '''
        caculate the average year over year operating margin of a company for last 4 years
        return a percentage number
        '''
        op_income = self._income_statement.loc[:, ['Total Revenue',
                                                   'Operating Income']]
        op_income['oper_margin'] = (op_income['Operating Income'] /
                                    op_income['Total Revenue'])

        return mean(op_income['oper_margin'].values)

    @property
    def revenues(self):
        '''
        return last 2 years of revenue numbers of a company,
        these numbers should not be none
        '''
        thisRev = self._income_statement['Total Revenue'][0]
        lastRev = self._income_statement['Total Revenue'][1]
        'check if value is None'
        if (thisRev is None) or (lastRev is None):
            raise ValueError('no revenue data')

        return thisRev, lastRev

    @property
    def bookValueOfDebt(self):
        '''
        return last 2 years book value of debt for a company
        book_value_of_debt = long term debt
        return 0 if no debt
        '''
        try:
            thisBVOD = self._balanced_sheet['Long Term Debt'][0]
            lastBVOD = self._balanced_sheet['Long Term Debt'][1]

        except KeyError:

            print('no long term debt found')
            thisBVOD, lastBVOD = 0, 0

        return thisBVOD, lastBVOD

    @property
    def ebit(self):
        '''
        return ebit of a company for the last 2 years
        these numbers should not be none
        '''
        thisEbit = self._income_statement['Operating Income'][0]
        lastEbit = self._income_statement['Operating Income'][1]
        # check if ebit number is None
        if (thisEbit is None) or (lastEbit is None):
            raise ValueError('no ebit data')

        return thisEbit, lastEbit

    @property
    def bookValueOfEquity(self):
        '''
        return book value of equity, something on the balanced sheet
        '''
        thisBVOE = self._balanced_sheet['Total Stockholder Equity'][0]
        lastBVOE = self._balanced_sheet['Total Stockholder Equity'][1]

        return thisBVOE, lastBVOE

    @property
    def cash(self):
        '''
        return amount of cash on hand, typically including long term 
        investments
        '''
        thisCash = (self._balanced_sheet['Cash'][0] +
                    self._balanced_sheet['Long Term Investments'][0])

        lastCash = (self._balanced_sheet['Cash'][1] +
                    self._balanced_sheet['Long Term Investments'][1])

        return thisCash, lastCash

    @property
    def tax_rate(self):
        '''
        return amount of income tax
        '''
        effTax = (self._income_statement['Income Tax Expense'][0] /
                  self._income_statement['Income Before Tax'][0])
        # if the tax rate is negative,
        # set it close to 0 to avoid mathmatic break point
        if effTax < 0:
            effTax = 0.00001
        return effTax

    @property
    def market_cap(self):
        return self._yf_info['marketCap']

    @property
    def interests_expense(self):
        interest_ = self._income_statement['Interest Expense'][0]
        if not interest_:
            interest_ = 0
        return interest_

    @property
    def wacc(self):
        '''
        # WACC-weighted cost of capital
        # WACC = (E/V*Re) + (D/V*Rd*(1-Tc))
        '''
        beta = self._yf_info['beta']

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
        shares = self._yf_info['sharesOutstanding']

        # effective tax rate
        effTax = self.tax_rate
        # sales to capital ratio
        shareholder_equity = self._balanced_sheet['Total Stockholder Equity'][0]
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
    print(DCF_DATA('nvda').bookValueOfDebt)