from google.cloud import bigquery

def available_ticker():
    '''
    get a list of avaible ticker for calculation
    '''
    query_str = '''
            SELECT DISTINCT ticker
            FROM all_data.income_statement
            ;
            '''
    client = bigquery.Client()
    # run query
    query_job = client.query(query=query_str)
    query_job.result()
    query_results = []
    for row in query_job:
        query_results.append(row['ticker'])

    return query_results

if __name__ == '__main__':
    print(available_ticker())