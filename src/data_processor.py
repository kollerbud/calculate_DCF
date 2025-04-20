from pathlib import Path
import duckdb
from typing import Dict
import pandas as pd
from FinancialDataETL import FinancialDataETL


# @dataclass
class FinancialDataProcessor:

    def __init__(self,
            cik: str,
            base_path: str = 'financial_data',
            filing_type: str = '10-K',
            years_statement: int = 5
        ):
        self.cik = str(cik).zfill(10)
        self.base_path = base_path
        self.parquet_path = Path(self.base_path) / f'{self.cik}.parquet'
        self.filing_type = filing_type
        self.years_statement = years_statement
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

    def get_financial_data(self, concepts: list, metric_name: str) -> pd.DataFrame:
        """Get financial data with concept fallbacks"""
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

    def get_latest_balance_sheet_value(self, concepts: list) -> float:
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

    def _calculate_growth_rate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate year-over-year growth rates"""
        if df.empty:
            return pd.DataFrame()
        df = df.sort_values('end_date')
        df['growth_rate'] = df['value'].pct_change() * 100
        return df

    def get_income_statement_metrics(self) -> Dict:
        """Get income statement metrics"""
        # Revenue metrics
        revenue_concepts = ['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax']
        revenue_data = self.get_financial_data(revenue_concepts, 'revenue')
        revenue_growth = self._calculate_growth_rate(revenue_data)

        # Net income metrics
        net_income_concepts = ['NetIncomeLoss', 'ProfitLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic']
        net_income_data = self.get_financial_data(net_income_concepts, 'net_income')
        net_income_growth = self._calculate_growth_rate(net_income_data)

        # Operating expenses
        cogs_concepts = ['CostOfGoodsAndServicesSold', 'CostOfRevenue']
        opex_concepts = ['OperatingExpenses', 'GeneralAndAdministrativeExpense', 'SellingGeneralAndAdministrativeExpense']

        # Interest and forex
        interest_expense_concepts = ['InterestExpense', 'InterestIncomeExpenseNet']
        forex_concepts = ['ForeignCurrencyTransactionGainLossBeforeTax', 'ForeignCurrencyTransactionGainLoss']

        return {
            'revenue': revenue_growth,
            'net_income': net_income_growth,
            'cogs': self.get_financial_data(cogs_concepts, 'cost_of_goods_sold'),
            'operating_expenses': self.get_financial_data(opex_concepts, 'operating_expenses'),
            'interest_expense': self.get_financial_data(interest_expense_concepts, 'interest_expense'),
            'forex_impact': self.get_financial_data(forex_concepts, 'forex_impact')
        }

    def get_balance_sheet_metrics(self) -> Dict:
        """Get balance sheet metrics"""
        # Current assets
        cash_concepts = ['CashAndCashEquivalentsAtCarryingValue', 'Cash']
        ar_concepts = ['AccountsReceivableNetCurrent', 'AccountsNotesAndLoansReceivableNetCurrent']
        inventory_concepts = ['InventoryNet', 'InventoryFinishedGoodsNetOfReserves']

        # Non-current assets
        ppe_concepts = ['PropertyPlantAndEquipmentNet', 'PropertyPlantAndEquipmentGross']
        intangible_concepts = ['IntangibleAssetsNet', 'IntangibleAssetsNetExcludingGoodwill']

        # Liabilities and equity
        total_liabilities_concepts = ['Liabilities', 'LiabilitiesCurrent']
        equity_concepts = ['StockholdersEquity', 'CommonStockValue']

        return {
            'cash': self.get_latest_balance_sheet_value(cash_concepts),
            'accounts_receivable': self.get_latest_balance_sheet_value(ar_concepts),
            'inventory': self.get_latest_balance_sheet_value(inventory_concepts),
            'ppe': self.get_latest_balance_sheet_value(ppe_concepts),
            'intangible_assets': self.get_latest_balance_sheet_value(intangible_concepts),
            'total_liabilities': self.get_latest_balance_sheet_value(total_liabilities_concepts),
            'stockholders_equity': self.get_latest_balance_sheet_value(equity_concepts)
        }

    def get_cash_flow_metrics(self) -> Dict:
        """Get cash flow metrics"""
        # Operating cash flow components
        operating_cf_concepts = ['NetCashProvidedByUsedInOperatingActivities']
        depreciation_concepts = ['DepreciationDepletionAndAmortization', 'Depreciation']
        capex_concepts = ['PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpendituresIncurredButNotYetPaid']

        # Working capital changes
        ar_change_concepts = ['IncreaseDecreaseInAccountsReceivable']
        inventory_change_concepts = ['IncreaseDecreaseInInventories']
        ap_change_concepts = ['IncreaseDecreaseInAccountsPayable']

        return {
            'operating_cash_flow': self.get_financial_data(operating_cf_concepts, 'operating_cash_flow'),
            'depreciation': self.get_financial_data(depreciation_concepts, 'depreciation'),
            'capex': self.get_financial_data(capex_concepts, 'capital_expenditures'),
            'ar_change': self.get_financial_data(ar_change_concepts, 'accounts_receivable_change'),
            'inventory_change': self.get_financial_data(inventory_change_concepts, 'inventory_change'),
            'ap_change': self.get_financial_data(ap_change_concepts, 'accounts_payable_change')
        }

    def get_dividend_metrics(self) -> Dict:
        """Get dividend related metrics"""
        dividend_per_share_concepts = ['CommonStockDividendsPerShareDeclared', 'CommonStockDividendsPerShareCashPaid']
        dividend_yield_concepts = ['DividendYield']
        payout_ratio_concepts = ['PayoutRatio', 'DividendPayoutRatio']

        return {
            'dividend_per_share': self.get_financial_data(dividend_per_share_concepts, 'dividend_per_share'),
            'dividend_yield': self.get_financial_data(dividend_yield_concepts, 'dividend_yield'),
            'payout_ratio': self.get_financial_data(payout_ratio_concepts, 'payout_ratio')
        }

    def shares_outstanding(self) -> Dict:
        shares_concepts = [
            'EntityCommonStockSharesOutstanding',
            'CommonStockSharesOutstanding',
            'WeightedAverageNumberOfDilutedSharesOutstanding',
            'CommonStockSharesIssued'
        ]
        shares_data = self.get_financial_data(shares_concepts, 'shares_outstanding')

        return shares_data

    def get_latest_metric_value(self, concepts: list, metric_name: str) -> float:
        """Generic function to get the latest value for a given concept list."""
        # Reusing the logic from get_latest_balance_sheet_value for broader use
        for concept in concepts:
            query = f"""
                SELECT value
                FROM financial_data
                WHERE concept_id = '{concept}'
                -- Using filing_date DESC might be more reliable for latest point-in-time value
                ORDER BY filing_date DESC, end_date DESC
                LIMIT 1
            """
            result = self.con.sql(query).df()
            if not result.empty:
                print(f"Found value for {metric_name} using concept: {concept}")
                return result['value'].iloc[0]
        print(f"Warning: No data found for {metric_name} concepts: {concepts}")
        return 0.0

    def get_total_assets(self) -> float:
        """Get latest total assets value."""
        # Common concepts for Total Assets
        concepts = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent'] # Assets is usually comprehensive
        # If 'Assets' isn't found, could try summing Current and Noncurrent, but 'Assets' is preferred
        return self.get_latest_metric_value(concepts, 'Total Assets')

    def get_total_liabilities(self) -> float:
        """Get latest total liabilities value."""
        # Common concepts for Total Liabilities
        concepts = ['Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent'] # Liabilities is comprehensive
        # If 'Liabilities' isn't found, could try summing Current and Noncurrent
        return self.get_latest_metric_value(concepts, 'Total Liabilities')

    def get_latest_dividend_per_share(self) -> float:
        """Get the most recently declared or paid dividend per share."""
        # Common concepts for Dividends Per Share
        concepts = [
            'CommonStockDividendsPerShareDeclared',
            'CommonStockDividendsPerShareCashPaid',
            'DividendsCommonStockCash' # This might be total dividends, needs check
            # Add other potential concepts if needed
        ]
        # We need the latest value reported, likely from the latest filing date
        return self.get_latest_metric_value(concepts, 'Dividend Per Share')

    def close(self):
        """Close DuckDB connection"""
        self.con.close()


if __name__ == '__main__':
    data_loader = FinancialDataProcessor(cik="1045810",
                                      years_statement=5, # as a way of using real growth rate to test different growth rate
                                      filing_type='10-K')

    print(data_loader.get_latest_dividend_per_share())