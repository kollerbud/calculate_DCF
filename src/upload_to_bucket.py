import os
from google.cloud import storage
from goog_auth import gcp_credentials


def upload_news_to_bq(bucket_name):
    storage_client = storage.Client(credentials=gcp_credentials())
    bucket = storage_client.get_bucket(bucket_name)

    csv_files = [filename for filename in os.listdir() if filename.endswith('.csv')]

    for csv_file in csv_files:
        blob = bucket.blob(csv_file)
        blob.upload_from_filename(csv_file)

    return blob.public_url


if __name__ == '__main__':
    upload_news_to_bq(bucket_name='dcf_news_bucket')
