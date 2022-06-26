from google.cloud import bigquery


def get_list_of_ticker():
    '''
    get a list of all tickers with financial data
    '''
    client = bigquery.Client()
    return client