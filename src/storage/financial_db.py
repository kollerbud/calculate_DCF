import pandas as pd
from src.storage.gcs_client import GCSDataManager

class FinancialDatabaseManager:
    
    def __init__(self, bucket_name: str):
        self.storage = GCSDataManager(bucket_name=bucket_name)
        self.base_path = 'financial_numeric'
    
    def upsert_financial_data(
        self, 
        ticker: str, 
        new_data_df: pd.DataFrame,
        force_refresh: bool = False
        ):
        '''
        merges new extracted data with existing gcs data,
        if force_refresh=True, then overwrites history with new_data_df
        '''
        
        blob_path = f"{self.base_path}/ticker={ticker}/historical_financials.parquet"
        
        if force_refresh:
            print(f"[{ticker}] FORCE REFRESH: Overwriting entire history.")
            final_df = new_data_df
        else:
            existing_df = self.storage.load_parquet_to_dataframe(blob_path)
            
            if existing_df.empty:
                print(f"[{ticker}] No existing data found. Creating new database entry.")
                final_df = new_data_df
            else:
                print(f"[{ticker}] Found existing data ({len(existing_df)} years). Merging new data...")
                # 2. Concatenate old and new data
                combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
                
                # 3. Drop duplicates based on 'year'. 
                # keep='last' ensures the newly extracted data overwrites the old data for that year!
                final_df = combined_df.drop_duplicates(subset=['year'], keep='last')
        
        final_df = final_df.sort_values(by='year').reset_index(drop=True)
        self.storage.save_dataframe_to_parquet(final_df, blob_path)
        print(f"[{ticker}] Database updated. Total years stored: {len(final_df)}\n")