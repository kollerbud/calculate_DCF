from google.cloud import bigquery

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'D:\Download\logical-dream-288204-09f96ef061e9.json'


# steps need: query by CIK (need to build a CIK look up table)
#

def comany_quick_summ_big_query(CIK: int = 1045810):
    '''
    Query company financial info from Google Big Query
    Note: big query data is behind by about 1 year (4 quarters)
    most recent four quarter data would be done by Yahoo finance 
    '''

    client = bigquery.Client()
    query = ''' SELECT *
                FROM `bigquery-public-data.sec_quarterly_financials.quick_summary`
                WHERE form = "10-Q" and company_name LIKE "NVIDIA%" and fiscal_period_focus = "Q3"
                ORDER BY date_filed DESC
                LIMIT 10
                ;
            '''
    



if __name__ == '__main__':
    pass