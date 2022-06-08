import apache_beam as beam
from apache_beam.pipeline import PipelineOptions
from api_keys import G_KEYS
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dcf_portion/hello_google.json'

'''
For income statement
'''


def parse_file(csvfile):
    data = csvfile.split(',')

    csv_data = {'ticker': data[0],
                'time_period': data[1],
                'income_before_tax': data[2],
                'research_development': data[3],
                'net_income': data[4],
                'sga': data[5],
                'gross_profit': data[6],
                'interest_expense': data[7],
                'operating_income': data[8],
                'income_tax_expense': data[9],
                'total_revenue': data[10],
                'cost_of_revenue': data[11]
                }

    return csv_data


data_schema = '''ticker:STRING,
                 time_period:DATE,
                 income_before_tax:FLOAT,
                 research_development:FLOAT,
                 net_income:FLOAT,
                 sga:FLOAT,
                 gross_profit:FLOAT,
                 interest_expense:FLOAT,
                 operating_income:FLOAT,
                 income_tax_expense:FLOAT,
                 total_revenue:FLOAT,
                 cost_of_revenue:FLOAT
              '''

if __name__ == '__main__':

    _bucket = G_KEYS.bucket
    _project = G_KEYS.project

    option = PipelineOptions(
        project=_project,
        temp_location=f'gs://{_bucket}/subfolder'
    )

    p = beam.Pipeline(options=option)
    parse_csv = (
        p
        | 'read input file' >> beam.io.ReadFromText(f'gs://{_bucket}/*_income_statement.csv', skip_header_lines=1)
        | 'parse file' >> beam.Map(parse_file)
        # | 'print out json before parsing' >> beam.Map(print) # be careful with print statement, it will change data format (json) and cause upload error
        | 'write to Bigquery' >> beam.io.WriteToBigQuery(
            f'{_project}:all_data.income_statement',
            schema=data_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )

    p.run().wait_until_finish()


'''
to do:
-critical one, mask bucket & project
-add a check to bigquery to stop duplicated ticker & period, only allow in new data
'''
