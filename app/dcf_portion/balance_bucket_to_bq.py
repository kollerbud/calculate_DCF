import apache_beam as beam
from apache_beam.pipeline import PipelineOptions
from api_keys import G_KEYS
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'app/dcf_portion/hello_google.json'

'''
For balance sheet
'''


def parse_file(csvfile):
    data = csvfile.split(',')
    # make sure dict keys match up
    csv_data = {'ticker': data[0],
                'period': data[1],
                'long_term_debt': data[2],
                'total_stock_holder': data[3],
                'cash': data[4],
                'long_term_invest': data[5],
                'short_term_debt': data[6]
                }

    return csv_data


# make sure dict keys match up
data_schema = 'ticker:STRING,period:DATE,long_term_debt:FLOAT,total_stock_holder:FLOAT,cash:FLOAT,long_term_invest:FLOAT,short_term_debt:FLOAT'

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
        | 'read input file' >> beam.io.ReadFromText(f'gs://{_bucket}/*_balance_sheet.csv', skip_header_lines=1)
        | 'parse file' >> beam.Map(parse_file)
        # | 'print out json before parsing' >> beam.Map(print) # be careful with print statement, it will change data format (json) and cause upload error
        | 'write to Bigquery' >> beam.io.WriteToBigQuery(
            f'{_project}:all_data.balance_sheet',
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
