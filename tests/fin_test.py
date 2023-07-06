import sys
from pathlib import Path
# ../Project_pipeline_1
sys.path += [str(Path(__file__).resolve().parent.parent),
             ]
from src.finance_section.statements_to_bq import FinancialsToBigquery


def test_statements():
    'test statement API calls'

    return None
