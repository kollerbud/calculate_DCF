import imp
from api_keys import FMP
import requests
import yfinance as yf

class BuildDCF:
    
    def __init__(self, ticker:str, years_back:int) -> None:
        self.ticker = str(ticker).upper()
        self.years_back = years_back # could've less than # of years situation (new IPO)
        self.req_header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'}

    def _request_statements(self, statement: str):
        if statement not in ['income-statement', 'cash-flow-statement', 'balance-sheet-statement']:
            raise ValueError ('use "income-statement, cash-flow-statement, balance-sheet-statement"')

        _api_link = ('https://financialmodelingprep.com/api/v3/'+str(statement)+'/'
                     +self.ticker+'?limit='+str(self.years_back)
                     +'&apikey='+str(FMP.api_key))
        request_response = requests.get(_api_link, headers=self.req_header).json()
        
        if len(request_response) == 0:
            raise ValueError('nothing return')

        return request_response
    
    def _yoy_revenue_growth(self):
        income_resp = self._request_statements(statement='income-statement')

        yoy_growth=[]
        for counter, statement in enumerate(income_resp):
            yoy_growth.append([statement['calendarYear'], statement['revenue']])
            if counter > 0:
                yoy_growth[counter-1].append((yoy_growth[counter-1][1]-yoy_growth[counter][1])/yoy_growth[counter][1])
        return yoy_growth
    
    def input_fileds(self):
        pass
        
    

if __name__ == '__main__':
    print(BuildDCF('nvda', 5)._yoy_revenue_growth())
