import pytest
import pandas as pd
from unittest.mock import MagicMock
import sys
import os

# Ensure the 'src' directory is in the Python path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from DCF_calculator import DCFModel, DCFScenario


@pytest.fixture
def mock_loader():
    """
    Pytest Fixture: Sets up a fresh mock data loader before each test.
    Instead of `self.mock_loader`, pytest injects this into any test that requests it.
    """
    loader = MagicMock()

    # Default mock values for balance sheet items
    loader.get_latest_value.side_effect = lambda metric: {
        'cash': 10000.0,
        'long_term_debt': 4000.0,
        'short_term_debt': 1000.0,
        'shares_outstanding': 1000.0
    }.get(metric, 0.0)

    return loader

def create_ts_df(values):
    """Helper to create a time-series DataFrame from a list of values."""
    if not values:
        return pd.DataFrame()
    return pd.DataFrame({'value': values})


# --- TESTS ---

def test_fabless_business_model_detection(mock_loader):
    """Test if a high-margin, low-capex company is correctly identified as Fabless."""
    # Revenue: $10B growing from $5B. OCF: $4B. CapEx: -$200M (2% of Revenue)
    mock_loader.get_time_series.side_effect = lambda metric: {
        'revenue': create_ts_df([10000, 8000, 6000, 5000]),
        'operating_cash_flow': create_ts_df([4000, 3000, 2000, 1500]),
        'capex': create_ts_df([-200, -150, -100, -100])
    }.get(metric, pd.DataFrame())

    model = DCFModel(mock_loader)

    # In pytest, we use simple 'assert' statements
    assert model.raw['valid'] is True
    assert model.raw['business_model'] == "Fabless"
    assert model.raw['capex_to_rev'] < 0.10


def test_idm_business_model_detection(mock_loader):
    """Test if a heavy-capex company is correctly identified as an IDM."""
    # Revenue: $10B. CapEx: -$3B (30% of Revenue - typical for Intel/Micron)
    mock_loader.get_time_series.side_effect = lambda metric: {
        'revenue': create_ts_df([10000, 10500, 11000, 12000]),
        'operating_cash_flow': create_ts_df([2000, 3000, 4000, 5000]),
        'capex': create_ts_df([-3000, -4000, -3500, -3000])
    }.get(metric, pd.DataFrame())

    model = DCFModel(mock_loader)

    assert model.raw['valid'] is True
    assert model.raw['business_model'] == "IDM/Foundry"
    assert model.raw['capex_to_rev'] > 0.10


def test_acquisition_anomaly_margin_floor(mock_loader):
    """Test if the model enforces the 20% margin floor for Fabless companies with accounting anomalies."""
    # AMD-like scenario: Massive revenue, but negative OCF due to acquisition amortization
    mock_loader.get_time_series.side_effect = lambda metric: {
        'revenue': create_ts_df([20000, 18000, 15000]),
        'operating_cash_flow': create_ts_df([-1000, -500, 1000]), # Negative!
        'capex': create_ts_df([-500, -400, -300]) # Fabless capex
    }.get(metric, pd.DataFrame())

    model = DCFModel(mock_loader)

    assert model.raw['business_model'] == "Fabless"
    assert model.raw['avg_margin'] < 0 # Raw margin is negative

    scenarios = model.generate_scenarios()
    avg_scenario = next(s for s in scenarios if s.name == "Average")

    # The model should have stepped in and floored the starting margin at 20%
    assert avg_scenario.margin_start == 0.20
    assert avg_scenario.wacc == 0.105 # Fabless WACC


def test_cycle_trough_normalization(mock_loader):
    """Test if IDMs in a cycle trough get normalized terminal margins."""
    # Intel-like scenario: Negative growth, terrible current margins, heavy capex
    mock_loader.get_time_series.side_effect = lambda metric: {
        'revenue': create_ts_df([50000, 60000, 70000]), # Shrinking
        'operating_cash_flow': create_ts_df([5000, 15000, 25000]),
        'capex': create_ts_df([-20000, -25000, -20000]) # Massive capex
    }.get(metric, pd.DataFrame())

    model = DCFModel(mock_loader)

    assert model.raw['business_model'] == "IDM/Foundry"

    scenarios = model.generate_scenarios()
    avg_scenario = next(s for s in scenarios if s.name == "Average")

    # Growth should be floored at 3% (GDP), not projected negative for 10 years
    assert avg_scenario.growth_start == 0.03
    # Margin should be floored at 5% to start, but normalize to 22% by terminal year
    assert avg_scenario.margin_start == 0.05
    assert avg_scenario.margin_end == 0.22
    assert avg_scenario.wacc == 0.085 # IDM WACC


def test_invalid_data_handling(mock_loader):
    """Test that the model gracefully handles missing data."""
    mock_loader.get_time_series.side_effect = lambda metric: pd.DataFrame()

    model = DCFModel(mock_loader)
    assert model.raw['valid'] is False


def test_dcf_math_calculation(mock_loader):
    """Test the actual mathematical output of the DCF."""
    mock_loader.get_time_series.side_effect = lambda metric: {
        'revenue': create_ts_df([100, 90]), # $100 Rev
        'operating_cash_flow': create_ts_df([30, 25]),
        'capex': create_ts_df([-10, -10])
    }.get(metric, pd.DataFrame())

    # Cash = 100, Debt = 50, Shares = 10. Net Cash = 50.
    mock_loader.get_latest_value.side_effect = lambda metric: {
        'cash': 100.0, 'long_term_debt': 50.0, 'short_term_debt': 0.0, 'shares_outstanding': 10.0
    }.get(metric, 0.0)

    model = DCFModel(mock_loader)

    # Force a simple scenario: 10% growth, 20% margin flat, 10% WACC, 3% terminal
    test_scenario = DCFScenario("Test", wacc=0.10, growth_start=0.10, margin_start=0.20, margin_end=0.20, terminal_growth=0.03)

    result = model.calculate_dcf(test_scenario, years=5) # 5 year model for easy math

    # Check that the price is calculated and valid
    assert result['price'] > 0
    assert result['name'] == "Test"