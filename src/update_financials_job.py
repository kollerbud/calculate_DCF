from src.data_processor import FinancialDataProcessor
from src.storage.financial_db import FinancialDatabaseManager
import time

def run_database_update():
    # 1. Configuration
    bucket_name = "dcf-financial-data"
    
    # Your 20 companies. To add a new one, just type it here.
    portfolio_tickers = [
        # "AAPL", "MSFT",
        # "NVDA", "AMD", "GOOGL", 
        'AMZN'
        # "META", "AMZN", "TSLA", "BRK.B", "JPM",
        # ... add as many as you want ...
    ]
    
    # Initialize your AI extractor and your DB manager
    processor = FinancialDataProcessor(provider="qwen", model_name="qwen2.5-7b-instruct")
    db_manager = FinancialDatabaseManager(bucket_name=bucket_name)

    # 2. Execution Loop
    for ticker in portfolio_tickers:
        print(f"========== Processing {ticker} ==========")
        try:
            # Default to pulling 5 years of data. 
            # (If the DB already has 2019-2023, and you pull 5 years including 2024, 
            # the upsert logic will cleanly add 2024 and ignore the duplicates!)
            extracted_df = processor.extract_historical_data(ticker, years=5)
            
            if not extracted_df.empty:
                # Upsert cleanly into the data lake
                db_manager.upsert_financial_data(ticker, extracted_df, force_refresh=False)
            
            # Optional: Sleep for a few seconds to avoid hitting SEC EDGAR rate limits
            time.sleep(2) 
            
        except Exception as e:
            print(f"Failed to process {ticker}: {e}")

if __name__ == "__main__":
    run_database_update()