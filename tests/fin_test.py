import sys
sys.path += ['../calculate_DCF/src',
             '../calculate_DCF',]
from src.finance_section.statements_to_bq import FinancialsToBigquery


def test_statements():
    'test statement API calls'

    return None