'''main script run by cloud function'''
import functions_framework
from statements_to_bq import FinancialsToBigquery
from ticker_info import CompanyOverviewInfo
from ticker_list import get_list_of_ticker


@functions_framework.http
def main(request=None):
    'run function to upload data of a company ticker'

    if request.args:
        _ticker = request.args.get('ticker')

        _ticker = str(_ticker).upper()

    ticker_list = get_list_of_ticker()

    if _ticker not in ticker_list:
        statements = FinancialsToBigquery(ticker=_ticker)
        print(statements.income_to_bigquery)
        print(statements.balanced_to_bigquery)
        company_info = CompanyOverviewInfo(ticker=_ticker)
        print(company_info.info)


    return f"ticker {_ticker} ran"
