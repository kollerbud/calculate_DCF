from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'D:\Download\logical-dream-288204-09f96ef061e9.json'

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
    big_query()