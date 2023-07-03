import sys
sys.path += ['../calculate_DCF/',
             '../calculate_DCF/src/finance_section',
             '../calculate_DCF/src/news_section',
             ]
from src.finance_section.statements_to_bq import FinancialsToBigquery


def test_statements():
    'test statement API calls'

    return None