from statistics import mean
from api_keys import FMP
import requests
import functools
import yfinance as yf


class DATA_DCF:
    # Build Data pipeline to feed into calculation

    def __init__(self, ticker: str, years_back: int) -> None:
        self.ticker = str(ticker).upper()
        # could've less than # of years situation (new IPO)
        self.years_back = years_back
        self.req_header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'}

    def _request_statements(self, statement: str):
        _api_link = ('https://financialmodelingprep.com/api/v3/' +
                     str(statement) + '/'
                     + self.ticker + '?limit=' + str(self.years_back)
                     + '&apikey=' + str(FMP.api_key))
        request_response = (requests.get(_api_link,
                                         headers=self.req_header).json())

        if len(request_response) < 2:
            raise ValueError('must be more than 2 years to perform valuation')

        print('running API request')

        return request_response

    @functools.cached_property
    def _income_statement(self):
        return self._request_statements(statement='income-statement')

    @functools.cached_property
    def _cash_flow_statement(self):
        return self._request_statements(statement='cash-flow-statement')

    @functools.cached_property
    def _balanced_sheet(self):
        return self._request_statements(statement='balance-sheet-statement')

    @property
    def _yoy_grwoth_(self):
        income_resp = self._income_statement
        yoy_growth = []
        for counter, statement in enumerate(income_resp):
            yoy_growth.append([statement['calendarYear'],
                               statement['revenue']])
            if counter > 0:
                yoy_growth[counter-1].append((yoy_growth[counter-1][1] -
                                              yoy_growth[counter][1]) /
                                             yoy_growth[counter][1])

        mean_growth = mean([yoy[2] for yoy in yoy_growth[:-1]])

        return round(mean_growth, 3)

    @property
    def _operating_margin(self):
        op_margin = [(x['operatingIncome']/x['revenue'])
                     for x in self._income_statement]
        print(op_margin)
        return mean(op_margin)

    @functools.cached_property
    def input_fileds(self):
        # revenues
        thisRev, lastRev = (self._income_statement[0]['revenue'],
                            self._income_statement[1]['revenue'])
        # EBIT or operating income
        thisEbit, lastEbit = (self._income_statement[0]['operatingIncome'],
                              self._income_statement[1]['operatingIncome'])
        # book value of equity
        thisBVOE, lastBVOE = (self._balanced_sheet[0]['totalStockholdersEquity'],
                              self._balanced_sheet[1]['totalStockholdersEquity'])
        # book value of debt, most likely long term debt
        thisBVOD, lastBVOD = (self._balanced_sheet[0]['longTermDebt'],
                              self._balanced_sheet[1]['longTermDebt'])
        # Cash
        thisCash, lastCash = ((self._balanced_sheet[0]['cashAndShortTermInvestments'] +
                               self._balanced_sheet[0]['longTermInvestments']),
                              (self._balanced_sheet[1]['cashAndShortTermInvestments'] +
                               self._balanced_sheet[1]['longTermInvestments']))
        # number of shares outstanding
        shares = self._income_statement[0]['weightedAverageShsOut']
        # effective tax rate
        effTax = (self._income_statement[0]['incomeTaxExpense'] /
                  self._income_statement[0]['incomeBeforeTax'])
        if effTax < 0:
            effTax = 0

        # sales to capital ratio
        net_income = self._income_statement[0]['netIncome']
        shareholder_equity = self._balanced_sheet[0]['totalStockholdersEquity']
        sales_to_cap = net_income/(thisBVOD+shareholder_equity-thisCash)

        # cost of capital
        stock_price = yf.Ticker(self.ticker).info['currentPrice']
        equity = stock_price * self._income_statement[0]['weightedAverageShsOut']
        
        

        return {'Revenues': [thisRev, lastRev],
                'EBIT': [thisEbit, lastEbit],
                'BVOE': [thisBVOE, lastBVOE],
                'BVOD': [thisBVOD, lastBVOD],
                'Cash': [thisCash, lastCash],
                'Shares': shares,
                'EffectiveTax': effTax,
                'sales_to_cap': sales_to_cap
                }
    
    def check_yahoo(self):
        return yf.Ticker(self.ticker).info


if __name__ == '__main__':
    print(DATA_DCF('SNOW', 5).input_fileds)
    print(DATA_DCF())