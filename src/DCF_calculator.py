import functools
from dataclasses import dataclass
from typing import Dict, Optional
import pandas as pd
from data_processor import FinancialDataProcessor

@dataclass
class DCFModel:
    """
    Performs DCF calculations using data provided by FinancialDataLoader.
    """
    data_loader: FinancialDataProcessor
    risk_free_rate: float = 0.0261
    beta: float = 1.2

    @functools.cached_property
    def _raw_calculations(self) -> Dict:
        """Calculate basic financial metrics"""
        # Get revenue data
        revenue_concepts = ['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax']
        revenues = self.data_loader.get_financial_data(revenue_concepts, 'revenue')

        # Get operating income
        op_income_concepts = ['OperatingIncomeLoss', 'OperatingIncome', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxes']
        operating_income = self.data_loader.get_financial_data(op_income_concepts, 'operating_income')

        # Get shares outstanding
        shares_concepts = [
            'EntityCommonStockSharesOutstanding',
            'CommonStockSharesOutstanding',
            'WeightedAverageNumberOfDilutedSharesOutstanding',
            'CommonStockSharesIssued'
        ]
        shares_data = self.data_loader.get_financial_data(shares_concepts, 'shares_outstanding')

        # Calculate growth rates
        revenues_list = revenues['value'].tolist() if not revenues.empty else []
        yoy_growth = []
        if len(revenues_list) >= 2:
            yoy_growth = [(x - y)/y for x, y in zip(revenues_list[:-1], revenues_list[1:])]

        # Calculate operating margins
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

        # Get balance sheet items
        balance_sheet_items = {
            'cash': ['Cash', 'CashAndCashEquivalentsAtCarryingValue'],
            'equity': ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
            'long_term_debt': ['LongTermDebt', 'LongTermDebtAndCapitalLeaseObligations'],
            'short_term_debt': ['ShortTermDebt', 'DebtCurrent'],
            'interest_expense': ['InterestExpense', 'InterestPaid'],
            'tax_expense': ['IncomeTaxExpenseBenefit', 'IncomeTaxPaid'],
        }

        balance_values = {}
        for item, concepts in balance_sheet_items.items():
            balance_values[item] = self.data_loader.get_latest_balance_sheet_value(concepts)
        # Add tax rate calculation
        pretax_income = self.data_loader.get_latest_balance_sheet_value(
            ['IncomeLossFromContinuingOperationsBeforeIncomeTaxes']
        )
        tax_expense = balance_values.get('tax_expense', 0)
        effective_tax_rate = tax_expense / pretax_income if pretax_income != 0 else 0.21
        
        return {
            'revenues': revenues_list,
            'operating_margins': operating_margins or [0.15],
            'yoy_growth': sum(yoy_growth)/len(yoy_growth) if yoy_growth else 0,
            'cash': balance_values.get('cash', 0),
            'short_term_debt': balance_values.get('short_term_debt', 0),            
            'long_term_debt': balance_values.get('long_term_debt', 0),
            'interest_expense': balance_values.get('interest_expense', 0),
            'effective_tax_rate': effective_tax_rate,
            'equity': balance_values.get('equity', 0),
            'shares_outstanding': shares_data['value'].iloc[0] if not shares_data.empty else 0
        }

    def _project_financials(self) -> Dict:
        """Project future financials"""
        raw = self._raw_calculations

        if not raw['revenues']:
            raise ValueError("No revenue data available for projection")

        avg_margin = (sum(raw['operating_margins']) / len(raw['operating_margins'])) if raw['operating_margins'] else 0.10

        initial_growth = raw['yoy_growth']
        if initial_growth <= 0:
            initial_growth = self.risk_free_rate

        growth_rates = [max(initial_growth * (1 - n/10), self.risk_free_rate) for n in range(1, 16)]
        # this growth_rates will go into negative growth
        # growth_rates = [(initial_growth * (1 - n/10)) for n in range(1, 16)]

        current_revenue = raw['revenues'][0] if raw['revenues'] else 0
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

    def calculate_wacc(self, market_risk_premium: float = 0.0523) -> float:
        """Calculate WACC"""
        raw = self._raw_calculations
        
        equity = raw.get('equity', 0)
        long_term_debt = raw.get('long_term_debt', 0)
        short_term_debt = raw.get('short_term_debt', 0)
        total_debt = long_term_debt + short_term_debt
        
        # Calculate cost of debt
        interest_expense = raw.get('interest_expense', 0)
        cost_of_debt = interest_expense / total_debt if total_debt > 0 else 0
        
        # Calculate weights
        total_capital = total_debt + equity
        if total_capital == 0:
            return 0.0  # Prevent division by zero
            
        debt_weight = total_debt / total_capital
        equity_weight = equity / total_capital
        
        # Cost of equity (CAPM)
        cost_of_equity = self.risk_free_rate + self.beta * market_risk_premium
        
        # Use effective tax rate with fallback
        tax_rate = raw.get('effective_tax_rate', 0.21)
        
        wacc = (debt_weight * cost_of_debt * (1 - tax_rate)) + (equity_weight * cost_of_equity)
        return wacc
        

    def calculate_dcf(self, wacc: Optional[float] = None) -> Dict:
        """Calculate DCF valuation"""
        wacc = wacc or self.calculate_wacc()
        projections = self._project_financials()

        fcf = []
        for idx, ebit in enumerate(projections['projected_ebit']):
            tax = ebit * 0.21
            fcf_value = (ebit - tax) * 0.8  # 20% reinvestment
            present_value = fcf_value / ((1 + wacc) ** (idx + 1))
            fcf.append(present_value)

        terminal_growth = self.risk_free_rate
        terminal_fcf = fcf[-1] * (1 + terminal_growth) if fcf else 0
        terminal_value = terminal_fcf / (wacc - terminal_growth) if wacc > terminal_growth else 0
        terminal_value_pv = terminal_value / ((1 + wacc) ** len(fcf)) if fcf else 0

        enterprise_value = sum(fcf) + terminal_value_pv
        equity_value = enterprise_value + self._raw_calculations['cash'] - self._raw_calculations['long_term_debt']
        shares = self._raw_calculations['shares_outstanding']

        return {
            'enterprise_value': enterprise_value,
            'equity_value': equity_value,
            'wacc': wacc,
            'projected_fcf': fcf,
            'terminal_value': terminal_value,
            'growth_rates': projections['growth_rates'],
            'shares_outstanding': shares,
            'price_per_share': equity_value / shares if shares > 0 else 0,
        }

    def close(self):
        """Close data loader connection"""
        self.data_loader.close()

if __name__ == '__main__':
    data_loader = FinancialDataProcessor(cik="1045810",
                                      years_statement=5, # as a way of using real growth rate to test different growth rate
                                      filing_type='10-K')
    dcf_model = DCFModel(data_loader=data_loader, risk_free_rate=0.04, beta=1)
    valuation = dcf_model.calculate_dcf(wacc=0.08)
    dcf_model.close()
    print(valuation)