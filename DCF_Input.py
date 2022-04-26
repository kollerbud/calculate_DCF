from statistics import mean
import functools
import yfinance as yf
import requests_cache


# replace with yfinance to remove api constraint

class DCF_DATA:
    # Build Data pipeline to feed into calculation
    _header = 'Mozilla/5.0 (Windows NT 10.0;  Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'
    _req_session = requests_cache.CachedSession('yfinance.cache')
    _req_session.headers['User-agent'] = _header

    def __init__(self, ticker: str) -> None:
        self.ticker = str(ticker).upper()
        self.session = DCF_DATA._req_session

    @functools.cached_property
    def _income_statement(self):
        fin = yf.Ticker(self.ticker, session=self.session).financials
        fin = fin.T
        return fin

    @functools.cached_property
    def _cash_flow_statement(self):
        cash = yf.Ticker(self.ticker, session=self.session).cashflow
        cash = cash.T
        return cash

    @functools.cached_property
    def _balanced_sheet(self):
        balance = yf.Ticker(self.ticker, session=self.session).balance_sheet
        balance = balance.T
        return balance
    
    @functools.cached_property
    def _yf_info(self):
        return yf.Ticker(self.ticker, session=self.session).info

    @property
    def _yoy_grwoth_(self):

        income_resp = self._income_statement['Total Revenue']
        mean_growth = mean([(x-y)/y for x,y in zip(income_resp[:-1], income_resp[1:])])

        return mean_growth

    @property
    def _operating_margin(self):
        op_income = self._income_statement.loc[:, ['Total Revenue',
                                                   'Operating Income']]
        op_income['oper_margin'] = (op_income['Operating Income'] /
                                    op_income['Total Revenue'])
        return mean(op_income['oper_margin'].values)

    @functools.cached_property
    def input_fileds(self):
        # revenues
        try:
            thisRev, lastRev = (self._income_statement['Total Revenue'][0],
                                self._income_statement['Total Revenue'][1])
            # EBIT or operating income
            thisEbit, lastEbit = (self._income_statement['Operating Income'][0],
                                  self._income_statement['Operating Income'][1])
            # book value of equity
            thisBVOE, lastBVOE = (self._balanced_sheet['Total Stockholder Equity'][0],
                                  self._balanced_sheet['Total Stockholder Equity'][0])
            # book value of debt, most likely long term debt
            thisBVOD, lastBVOD = (self._balanced_sheet['Long Term Debt'][0],
                                  self._balanced_sheet['Long Term Debt'][1])
            # Cash
            thisCash, lastCash = ((self._balanced_sheet['Cash'][0] +
                                   self._balanced_sheet['Long Term Investments'][0]),
                                  (self._balanced_sheet['Cash'][1] +
                                  self._balanced_sheet['Long Term Investments'][1]))

        # number of shares outstanding
            shares = self._yf_info['sharesOutstanding']
            # effective tax rate
            effTax = (self._income_statement['Income Tax Expense'][0] /
                      self._income_statement['Income Before Tax'][0])

        except KeyError:
            print('server unresponsive, re-run')
        # if net_income is 0 then tax is 0
        if effTax < 0:
            effTax = 0.00001

        # sales to capital ratio
        net_income = self._income_statement['Net Income'][0]
        shareholder_equity = self._balanced_sheet['Total Stockholder Equity'][0]
        sales_to_cap = net_income/(thisBVOD + shareholder_equity - thisCash)

        # market capital
        # WACC-weighted cost of capital
        # WACC = (E/V*Re) + (D/V*Rd*(1-Tc))
        market_cap = self._yf_info['marketCap']
        interest_ = self._income_statement['Interest Expense'][0]
        # cost of equity    
        Re = 0.0159 + self._yf_info['beta'] * (0.10-0.0159)
        Rd = (interest_/thisBVOD)*(1-0.21)
        wacc = ((market_cap/(market_cap+thisBVOD)*Re) +
                (thisBVOD/(market_cap+thisBVOD)*Rd*(1-effTax)))

        return {'Revenues': [thisRev, lastRev],
                'EBIT': [thisEbit, lastEbit],
                'BVOE': [thisBVOE, lastBVOE],
                'BVOD': [thisBVOD, lastBVOD],
                'Cash': [thisCash, lastCash],
                'Shares': shares,
                'EffectiveTax': effTax,
                'sales_to_cap': sales_to_cap,
                'wacc': wacc,
                'growth_rate': self._yoy_grwoth_,
                'oper_margin': self._operating_margin
                }



if __name__ == '__main__':
    print(DCF_DATA('nvda').input_fileds)