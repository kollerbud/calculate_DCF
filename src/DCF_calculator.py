from typing import Dict, Optional
import duckdb
from pathlib import Path
import pandas as pd
from dataclasses import dataclass
import functools
from FinancialDataETL import FinancialDataETL


@dataclass
class UnifiedDCFCalculator:
    """
    Unified DCF Calculator that uses parquet files from ETL process
    """
    cik: str
    base_path: str = "financial_data"
    years_statement: int = 5
    risk_free_rate: float = 0.0261
    filing_type: str = "10-K"

    def __post_init__(self):
        self.cik = str(self.cik).zfill(10)
        self.parquet_path = Path(self.base_path) / f"{self.cik}.parquet"
        self._ensure_data_exists()
        self._init_duckdb()

    def _ensure_data_exists(self):
        """Ensure parquet file exists, if not run ETL process"""
        if not self.parquet_path.exists():
            print(f"Parquet file not found for CIK {self.cik}. Running ETL process...")
            etl = FinancialDataETL(base_path=self.base_path, cik=self.cik)
            etl.process_company(mode="overwrite")

    def _init_duckdb(self):
        """Initialize DuckDB connection and register parquet file"""
        self.con = duckdb.connect(database=':memory:')
        self.con.sql(f"""
            CREATE OR REPLACE VIEW financial_data AS
            SELECT * FROM parquet_scan('{str(self.parquet_path)}')
        """)

    @functools.cached_property
    def _raw_calculations(self) -> Dict:
        """Calculate basic financial metrics with error handling"""
        # Get revenue data with fallback concepts
        revenue_concepts = ['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax']
        revenues = self._get_financial_data(revenue_concepts, 'revenue')

        # Get operating income with fallback concepts
        op_income_concepts = ['OperatingIncomeLoss', 'OperatingIncome', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxes']
        operating_income = self._get_financial_data(op_income_concepts, 'operating_income')
        
        # get shares outstanding
        shares_concepts = [
            'EntityCommonStockSharesOutstanding',
            'CommonStockSharesOutstanding',
            'WeightedAverageNumberOfDilutedSharesOutstanding',
            'CommonStockSharesIssued'
        ]
        shares_data = self._get_financial_data(shares_concepts, 'shares_outstanding')

        # Calculate growth rates with validation
        revenues_list = revenues['value'].tolist()
        yoy_growth = []
        if len(revenues_list) >= 2:
            yoy_growth = [(x - y)/y for x, y in zip(revenues_list[:-1], revenues_list[1:])]

        # Calculate operating margin with merge validation
        operating_margins = []
        if not revenues.empty and not operating_income.empty:
            merged_data = pd.merge(
                revenues[['end_date', 'value']],
                operating_income[['end_date', 'value']],
                on='end_date',
                suffixes=('_rev', '_op')
            )
            if not merged_data.empty:
                operating_margins = (merged_data['value_op'] / merged_data['value_rev']).tolist()

        # Get balance sheet items with fallbacks
        balance_sheet_items = {
            'cash': ['Cash', 'CashAndCashEquivalentsAtCarryingValue'],
            'debt': ['LongTermDebt', 'LongTermDebtAndCapitalLeaseObligations'],
            'equity': ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest']
        }

        balance_values = {}
        for item, concepts in balance_sheet_items.items():
            balance_values[item] = self._get_latest_balance_sheet_value(concepts)

        return {
            'revenues': revenues_list,
            'operating_margins': operating_margins or [0.15],  # Fallback to 15% margin if none found
            'yoy_growth': sum(yoy_growth)/len(yoy_growth) if yoy_growth else 0,
            'cash': balance_values.get('cash', 0),
            'long_term_debt': balance_values.get('debt', 0),
            'equity': balance_values.get('equity', 0),
            'shares_outstanding': shares_data['value'].iloc[0]
        }

    def _get_financial_data(self, concepts: list, metric_name: str) -> pd.DataFrame:
        """Helper to get financial data with concept fallbacks"""
        for concept in concepts:
            query = f"""
                SELECT value, end_date
                FROM financial_data
                WHERE concept_id = '{concept}'
                AND form = '{self.filing_type}'
                ORDER BY end_date DESC
                LIMIT {self.years_statement}
            """
            result = self.con.sql(query).df()
            if not result.empty:
                return result
        print(f"Warning: No data found for {metric_name} concepts: {concepts}")
        return pd.DataFrame()

    def _get_latest_balance_sheet_value(self, concepts: list) -> float:
        """Get latest balance sheet value with concept fallbacks"""
        for concept in concepts:
            query = f"""
                SELECT value
                FROM financial_data
                WHERE concept_id = '{concept}'
                AND form = '{self.filing_type}'
                ORDER BY filing_date DESC
                LIMIT 1
            """
            result = self.con.sql(query).df()
            if not result.empty:
                return result['value'].iloc[0]
        return 0.0
    
    def _project_financials(self) -> Dict:
        """Project future financials with validation"""
        raw = self._raw_calculations

        # Validate input data
        if not raw['revenues']:
            raise ValueError("No revenue data available for projection")

        # Use average margin or last available margin
        if raw['operating_margins']:
            avg_margin = sum(raw['operating_margins']) / len(raw['operating_margins'])
        else:
            avg_margin = 0.10  # Fallback to 10% margin
            print("Using fallback operating margin of 10%")

        # Calculate growth rates with sanity checks
        initial_growth = raw['yoy_growth']
        if initial_growth <= 0:
            initial_growth = self.risk_free_rate  # Use risk-free rate as floor

        growth_rates = [
            max(initial_growth * (1 - n/10), self.risk_free_rate)
            for n in range(1, 16)
        ]

        # Project revenues and EBIT
        current_revenue = raw['revenues'][0]
        projected_revenues = []
        projected_ebit = []

        for rate in growth_rates:
            current_revenue *= (1 + rate)
            projected_revenues.append(current_revenue)
            projected_ebit.append(current_revenue * avg_margin)

        return {
            'projected_revenues': projected_revenues,
            'projected_ebit': projected_ebit,
            'growth_rates': growth_rates
        }


    def _calculate_growth_rates(self, periods: int = 15) -> list:
        """Calculate future growth rates"""
        initial_growth = self._raw_calculations['yoy_growth']
        return [
            initial_growth - (initial_growth - self.risk_free_rate) / 10 * n
            for n in range(1, periods + 1)
        ]

    def _project_financials(self) -> Dict:
        """Project future financials based on growth rates"""
        growth_rates = self._calculate_growth_rates()
        latest_revenue = self._raw_calculations['revenues'][0]
        avg_operating_margin = sum(self._raw_calculations['operating_margins']) / len(self._raw_calculations['operating_margins'])

        projected_revenues = []
        projected_ebit = []

        current_revenue = latest_revenue
        for growth_rate in growth_rates:
            current_revenue *= (1 + growth_rate)
            projected_revenues.append(current_revenue)
            projected_ebit.append(current_revenue * avg_operating_margin)

        return {
            'projected_revenues': projected_revenues,
            'projected_ebit': projected_ebit,
            'growth_rates': growth_rates
        }

    def calculate_wacc(self, market_risk_premium: float = 0.0523, debt_cost: float = 0.0232) -> float:
        """Calculate WACC"""
        total_capital = self._raw_calculations['long_term_debt'] + self._raw_calculations['equity']
        debt_weight = self._raw_calculations['long_term_debt'] / total_capital
        equity_weight = self._raw_calculations['equity'] / total_capital

        # For beta, we'll need to add market data integration later
        # For now, using a placeholder beta of 1.2
        beta = 1.2

        cost_of_equity = self.risk_free_rate + (beta * market_risk_premium)
        tax_rate = 0.21  # Assuming standard corporate tax rate

        wacc = (debt_weight * debt_cost * (1 - tax_rate)) + (equity_weight * cost_of_equity)
        return wacc

    def calculate_dcf(self, wacc: Optional[float] = None) -> Dict:
        """Calculate DCF valuation"""
        if wacc is None:
            wacc = self.calculate_wacc(market_risk_premium=0.055)

        projections = self._project_financials()

        # Calculate free cash flows
        fcf = []
        for idx, ebit in enumerate(projections['projected_ebit']):
            tax = ebit * 0.21  # Assuming standard tax rate
            fcf_value = (ebit - tax) * (1 - 0.2)  # Assuming reinvestment rate of 20%
            present_value = fcf_value / ((1 + wacc) ** (idx + 1))
            fcf.append(present_value)

        # Terminal value calculation
        terminal_growth = self.risk_free_rate  # Conservative terminal growth
        terminal_fcf = fcf[-1] * (1 + terminal_growth)
        terminal_value = terminal_fcf / (wacc - terminal_growth)
        terminal_value_pv = terminal_value / ((1 + wacc) ** len(fcf))

        # Enterprise value
        enterprise_value = sum(fcf) + terminal_value_pv

        # Equity value
        equity_value = enterprise_value + self._raw_calculations['cash'] - self._raw_calculations['long_term_debt']
        
        # calculate projectaed price
        shares = self._raw_calculations['shares_outstanding']
        return {
            'enterprise_value': enterprise_value,
            'equity_value': equity_value,
            'wacc': wacc,
            'projected_fcf': fcf,
            'terminal_value': terminal_value,
            'growth_rates': projections['growth_rates'],
            'shares_outstanding': shares,
            'price_per_share': equity_value/shares if shares > 0 else 0,
        }

    def close(self):
        """Close DuckDB connection"""
        self.con.close()

if __name__ == '__main__':
    # Example usage
    calculator = UnifiedDCFCalculator(
        cik="0001045810",
        filing_type='10-Q',
        years_statement=10,
        risk_free_rate=0.02
    )
    try:
        dcf_results = calculator.calculate_dcf(wacc=0.035)
        print(dcf_results)
        print(f"WACC: {dcf_results['wacc']:.2%}")
        print(f"projected price {dcf_results['price_per_share']}")
    finally:
        calculator.close()