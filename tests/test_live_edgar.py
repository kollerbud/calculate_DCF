import pytest
import pandas as pd
import sys
import os

# Ensure the 'src' directory is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from data_processor import FinancialDataProcessor

# We mark this so we can easily skip it if we just want to run fast local tests
@pytest.mark.live
class TestLiveSECData:
    
    @pytest.fixture(scope="class")
    def live_processor(self):
        """
        Scope="class" means this fixture runs exactly ONCE for the whole test class.
        It reaches out to SEC EDGAR, downloads the real 10-K data, and caches it in 
        DuckDB. This prevents us from getting rate-limited by the SEC during testing.
        
        We use Apple (CIK: 320193) because their SEC reporting is highly standardized.
        """
        print("\n[!] Fetching LIVE data from SEC EDGAR. This may take a few seconds...")
        # 2 years of 10-K data is plenty to verify that the concept tags exist
        processor = FinancialDataProcessor(
            cik='320193', # Apple Inc.
            years_statement=2, 
            filing_type='10-K',
            persist_data=False 
        )
        yield processor
        processor.close()

    @pytest.mark.parametrize("metric", [
        'revenue',
        'operating_income',
        'net_income',
        'operating_cash_flow',
        'capex',
        'interest_expense',
        'tax_expense',
        'cash',
        'equity',
        'long_term_debt',
        'shares_outstanding'
    ])
    def test_live_metric_aliases_resolve(self, live_processor, metric):
        """
        This test loops through every core metric in your model.
        It asserts that your exact aliases OR your fuzzy matching logic 
        successfully found actual data in a real, modern SEC filing.
        """
        df = live_processor.get_time_series(metric)
        
        # 1. Did we find data?
        assert not df.empty, f"CRITICAL: '{metric}' returned empty. Aliases may be out of date."
        
        # 2. Is the data properly formatted?
        assert 'value' in df.columns
        assert 'end_date' in df.columns
        
        # 3. Did we get actual numerical values?
        latest_value = df['value'].iloc[0]
        assert isinstance(latest_value, (int, float))
        
        # 4. Shares outstanding sanity check (Apple should have > 1 Billion shares)
        if metric == 'shares_outstanding':
            assert latest_value > 1_000_000_000, f"Shares outstanding suspiciously low: {latest_value}"

    def test_live_balance_sheet_integration(self, live_processor):
        """Verify the aggregate dictionary methods work on live data."""
        bs_metrics = live_processor.get_balance_sheet_metrics()
        
        assert bs_metrics['cash'] > 0
        assert bs_metrics['equity'] > 0
        # Check that the dictionary keys match what the DCF_calculator expects
        assert 'long_term_debt' in bs_metrics
        assert 'short_term_debt' in bs_metrics