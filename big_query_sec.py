from google.cloud import bigquery
import sqlite3

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'D:\Download\logical-dream-288204-09f96ef061e9.json'

# steps need: query by CIK (need to build a CIK look up table)
#

class DCFNumber:
    '''
    gather 5 years of quarterly earning reports for DCF model
    load CIK(company) financial figures into SQLite

    '''
    def __init__(self, CIK: str = 1045810) -> None:
        self.CIK = CIK
        self.conn = sqlite3.connect(r'file:D:\projects\Project_pipeline_1\Big_query_financials.db?mode=rw', uri=True)
        self.cur = self.conn.cursor()
    
    def creat_table(self) -> None:
        sql_create_tables = '''CREATE TABLE IF NOT EXISTS big_query_sec(
                                "Company_name" TEXT,
                                "measure_tag" TEXT,
                                "value" INTEGER,
                                "units" TEXT,
                                "number_of_quarters" INTEGER,
                                "central_index_key" INTEGER,
                                "period_end_date" TEXT,
                                "form" TEXT,
                                "fiscal_period_focus" INTEGER,
                                "fiscal_year" INTEGER
                                );

                               CREATE TABLE IF NOT EXISTS yahoo_sec(
                                ""
                                ""
                                );
                            '''
        self.cur.executescript(sql_create_tables)
        
        
    

    def download_10Q_financials(self) -> None:
        '''
        Query company financial info from Google Big Query
        Note: big query data is behind by about 1 year (4 quarters)
        most recent four quarter data would be done by Yahoo finance 
        '''
        
        client = bigquery.Client()
        query = ''' SELECT *
                    FROM `bigquery-public-data.sec_quarterly_financials.quick_summary`
                    WHERE form = "10-Q" AND central_index_key = @CIK
                    ORDER BY date_filed DESC
                    LIMIT 1
                    ;
                '''
        job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('CIK', 'INTEGER', self.CIK)])
        query_job = client.query(query=query, job_config=job_config)

        for row in query_job:
            # row[1] company_name
            # row[2] measure_tag
            # row[4] value
            # row[5] unit
            # row[6] number_of_quarters
            # row[8] central_index_key
            # row [11] fiscal_year_end
            # row[12] form
            # row[14] fiscal_period_focus
            # row[13] fiscal_year
            upload_statement =  ''' INSERT INTO big_query_sec("Company_name", 
                                                              "measure_tag",
                                                              "value",
                                                              "units",
                                                              "number_of_quarters",
                                                              "central_index_key",
                                                              "period_end_date",
                                                              "form",
                                                              "fiscal_period_focus",
                                                              "fiscal_year")
                                    VALUES(?,?,?,?,?,?,?,?,?);
                                '''
            self.cur.execute(upload_statement, (row[1], row[2], row[4], row[5], row[6], row[8], row[11], row[12], row[14],row[13],))
            self.conn.commit()

    def downlaod_yfinance(self) -> None:
        pass

            

if __name__ == '__main__':
    For_DCF('0001640147').download_10Q_financials()