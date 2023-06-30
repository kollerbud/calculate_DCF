from google.cloud import bigquery
from goog_auth import gcp_credentials


def get_list_of_ticker():
    '''
    return a list of company tickers to get news for
    '''

    query_str = '''
                select Company_ticker
                from all_data.unique_companies
                '''
    client = bigquery.Client(credentials=gcp_credentials())
    # run query
    query_job = client.query(query=query_str)
    query_job.result()
    query_results = []
    for row in query_job:
        query_results.append(row['Company_ticker'])

    return query_results
