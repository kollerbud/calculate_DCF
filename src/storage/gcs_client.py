import pandas as pd
from google.cloud import storage
import os

class GCSDataManager:
    '''
    handles saving and loading data from gcs bucket
    '''

    def __init__(self, bucket_name: str, local_cache_dir: str = './local_data'):

        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name=bucket_name)
        self.local_cache_dir = local_cache_dir

        os.makedirs(self.local_cache_dir, exist_ok=True)

    def save_dataframe_to_parquet(self, df: pd.DataFrame, destination_blob_name: str):
        'saves a pandas dadtaframe to local cache first, then mirrors to gcs'
        if df.empty:
            print(f'dataframe is empty, skipping upload to {destination_blob_name}')
            return None
        # save locally
        local_path = os.path.join(self.local_cache_dir, destination_blob_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_parquet(local_path, index=False)
        # upload to gcs
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_path, content_type='application/octet-stream')
        print(f"Successfully uploaded {len(df)} rows to gs://{self.bucket.name}/{destination_blob_name}")

    def load_parquet_to_dataframe(
            self,
            source_blob_name: str,
            force_refresh: bool = False,
        ) -> pd.DataFrame:
        local_path = os.path.join(self.local_cache_dir, source_blob_name)
        # swtich to pull from gcs bucket
        if force_refresh or not os.path.exists(local_path):
            reason = 'force refresh' if force_refresh else 'local cache missing'
            print(f"[{reason}] Downloading {source_blob_name} from GCS...")

            blob = self.bucket.blob(source_blob_name)
            if not blob.exists():
                print(f"Notice: Blob {source_blob_name} does not exist in GCS yet.")
                return pd.DataFrame()
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            blob.download_to_filename(local_path)
            print(f"Successfully cached to {local_path}")
        else:
            print(f"[Cache Hit] Loaded {source_blob_name} from local storage.")

        try:
            return pd.read_parquet(local_path)
        except Exception as e:
            print(f'error reading local parquet file {local_path}: {e}')
            return pd.DataFrame()