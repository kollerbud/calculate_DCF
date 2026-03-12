import pytest
import pandas as pd
from unittest.mock import patch
import sys
import os

# Ensure the 'src' directory is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from data_processor import FinancialDataProcessor

@pytest.fixture
def processor_factory():
    """
    A factory fixture that allows us to pass a custom "fake" DataFrame 
    into the FinancialDataProcessor, completely bypassing the live SEC Edgar API.
    """
    def _make_processor(mock_dataframe):
        # We patch BOTH the fetch method and the save method so it runs purely in-memory
        with patch('data_processor.FinancialDataProcessor._fetch_and_transform_data', return_value=mock_dataframe):
            with patch('data_processor.FinancialDataProcessor._save_to_parquet'):
                # Initialize the processor. It will consume our mock_dataframe.
                proc = FinancialDataProcessor(cik='123456', persist_data=False)
                return proc
    return _make_processor


def create_mock_row(concept_id, value, stmt_type, date='2025-12-31'):
    """Helper to quickly generate rows for our mock SEC database."""
    return {
        'concept_id': concept_id,
        'value': value,
        'statement_type': stmt_type,
        'end_date': pd.to_datetime(date),
        'cik': '0000123456',
        'form': '10-K'
    }


# --- STATIC METHOD TESTS ---

def test_string_normalization():
    """Test that the fuzzy matcher strips punctuation and normalizes cases correctly."""
    assert FinancialDataProcessor._normalize("Net Income (Loss)") == "net income loss"
    assert FinancialDataProcessor._normalize("Total stockholders' equity") == "total stockholders equity"
    assert FinancialDataProcessor._normalize("Earnings  Per-Share") == "earnings per share"


def test_token_similarity():
    """Test the math behind the Jaccard-style token overlap."""
    # "net income" (2 tokens) vs "net income loss" (3 tokens) = 2 matches. 2 / min(2, 3) = 1.0
    score = FinancialDataProcessor._token_similarity("net income", "net income loss")
    assert score == 1.0
    
    # "revenue" vs "net sales" = 0 matches
    score = FinancialDataProcessor._token_similarity("revenue", "net sales")
    assert score == 0.0


# --- DATA RETRIEVAL TESTS ---

def test_exact_match_retrieval(processor_factory):
    """Test that an exact string match (defined in METRIC_ALIASES) is pulled instantly."""
    df = pd.DataFrame([
        create_mock_row("Revenue", 10000.0, "Income Statement")
    ])
    proc = processor_factory(df)
    
    result = proc.get_time_series('revenue')
    assert not result.empty
    assert result['value'].iloc[0] == 10000.0


def test_fuzzy_match_retrieval(processor_factory):
    """Test that slight variations in labels are caught by the fuzzy matcher."""
    df = pd.DataFrame([
        # This exact string is NOT in METRIC_ALIASES, but is very close to "Total equity"
        create_mock_row("Total Shareholders Equity (Deficit)", 5000.0, "Balance Sheet")
    ])
    proc = processor_factory(df)
    
    result = proc.get_time_series('equity')
    assert not result.empty
    assert result['value'].iloc[0] == 5000.0


def test_exclusion_patterns(processor_factory):
    """Test that 'per share' metrics are successfully blocked when searching for Net Income."""
    df = pd.DataFrame([
        # Because "per share" is in METRIC_EXCLUDE_PATTERNS for 'net_income', 
        # the fuzzy matcher must ignore this, even though it contains the words "Net income".
        create_mock_row("Net income per share", 4.5, "Income Statement")
    ])
    proc = processor_factory(df)
    
    result = proc.get_time_series('net_income')
    assert result.empty # Should be blocked!


def test_statement_type_filter(processor_factory):
    """Test that cross-statement contamination is blocked."""
    df = pd.DataFrame([
        # "Property, plant and equipment" exists on the Balance Sheet (Total Value)
        # AND on the Cash Flow statement (CapEx spending). 
        # If we ask for CapEx, it must ONLY pull from the Cash Flow statement.
        create_mock_row("Property, plant and equipment, net", 50000.0, "Balance Sheet")
    ])
    proc = processor_factory(df)
    
    result = proc.get_time_series('capex')
    assert result.empty # Blocked because statement_type != 'Cash Flow'


def test_value_bounds_filtering(processor_factory):
    """Test that anomalous values (like shares < 100k) are dropped."""
    df = pd.DataFrame([
        # 50.0 is way too low for outstanding shares (likely an EPS number mislabeled by the SEC)
        create_mock_row("CommonStockSharesOutstanding", 50.0, "Income Statement"),
        # 500 million is a realistic share count
        create_mock_row("Weighted average shares outstanding - diluted", 500_000_000.0, "Income Statement")
    ])
    proc = processor_factory(df)
    
    result = proc.get_time_series('shares_outstanding')
    
    # The 50.0 value should be entirely purged by METRIC_VALUE_BOUNDS
    assert len(result) == 1
    assert result['value'].iloc[0] == 500_000_000.0