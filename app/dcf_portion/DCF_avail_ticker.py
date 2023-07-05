#import sys
#sys.path +=['../calculate_DCF/app/dcf_portion/',
#            '../app/dcf_portion/']
from google.cloud import bigquery
from goog_auth import gcp_credentials


def available_ticker():
    '''
    get a list of avaible ticker for calculation
    '''
    query_str = '''
            SELECT DISTINCT ticker
            FROM all_data.income_statement
            ;
            '''
    client = bigquery.Client(credentials=gcp_credentials())
    # run query
    query_job = client.query(query=query_str)
    query_job.result()
    query_results = []
    for row in query_job:
        query_results.append(row['ticker'])

    return query_results
