from statements_to_bq import FinancialsToBigquery
from ticker_info import CompanyOverviewInfo


print(FinancialsToBigquery(ticker='nvda')._balanced_transform)