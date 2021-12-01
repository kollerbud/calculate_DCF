from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'C:\Users\Li\Downloads\logical-dream-288204-17621de4247c.json'

def big_query():
    client = bigquery.Client()
    query = ''' select distinct (company_name)
                from `bigquery-public-data.sec_quarterly_financials.numbers`
                limit 10
            '''
    query_job = client.query(query)
    for row in query_job:
        print(row)


if __name__ == '__main__':
    pass