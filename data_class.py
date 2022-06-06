from dataclasses import dataclass
from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'hello_google.json'

@dataclass
class dcf:
    ticker: str
    client = bigquery.Client()

    def __post_init__(self):
        print(f'post init {self.ticker}')



print(dcf('inv').client)