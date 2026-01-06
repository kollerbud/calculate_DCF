import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from FinancialDataETL import FinancialDataETL


class FinancialDataProcessor:

    METRIC_ALIASES = {
        'revenue': [
            "revenues", "Revenue", "Net sales", "Net revenue", "Net revenues",
            "Total net sales", "Contract Revenue",
            "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"
        ],
        'operating_income': [
            "operating_income", "Operating Income", "Operating income",
            "Operating Income (Loss)", "OperatingIncomeLoss",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxes"
        ],
        'net_income': [
            "net_income", "Net Income", "Net Income (Loss)",
            "Net Income from Continuing Operations",
            "Basic Net Income Available to Common Shareholders", "NetIncomeLoss"
        ],
        'interest_expense': [
            "interest_expense", "Interest Expense", "Interest expense",
            "Interest expense, net", "Interest Expense (non-operating)",
            "InterestPaid"
        ],
        'tax_expense': [
            "tax_expense", "Income Tax Expense", "Income tax provision",
            "Provision for Income Taxes", "Provision for (benefit from) income taxes",
            "IncomeTaxExpenseBenefit", "IncomeTaxPaid"
        ],
        'cash': [
            "cash", "Cash and cash equivalents", "Cash and Cash Equivalents",
            "Cash, cash equivalents and restricted cash",
            "CashAndCashEquivalentsAtCarryingValue"
        ],
        'equity': [
            "equity", "Total stockholders' equity", "Total Stockholders' Equity",
            "Total equity", "Stockholders' equity", "Shareholders' equity",
            "StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
        ],
        'long_term_debt': [
            "long_term_debt", "Long Term Debt", "Long-Term Debt", "Long-term debt",
            "Long-term debt, net", "LongTermDebt", "LongTermDebtAndCapitalLeaseObligations"
        ],
        'short_term_debt': [
            "short_term_debt", "Short-term debt", "Short-term borrowings",
            "Current portion of long-term debt", "ShortTermDebt", "DebtCurrent"
        ],
        'capex': [
            "capex", "Payments to acquire property, plant and equipment",
            "Purchases of property, plant and equipment",
            "Purchases of property and equipment", "Capital expenditures"
        ],
        'shares_outstanding': [
            "shares_outstanding", "Common Stock Shares Outstanding",
            "Weighted average shares outstanding - diluted",
            "Shares Outstanding (Diluted)", "EntityCommonStockSharesOutstanding",
            "CommonStockSharesOutstanding", "WeightedAverageNumberOfDilutedSharesOutstanding"
        ]
    }

    def __init__(self,
            cik: str,
            base_path: str = 'financial_data',
            filing_type: str = '10-K',
            years_statement: int = 5
        ):
        self.cik = str(cik).zfill(10)
        self.base_path = base_path
        self.parquet_path = Path(self.base_path) / FinancialDataETL.CONSOLIDATED_FILE
        self.filing_type = filing_type
        self.years_statement = years_statement

        self._ensure_data_exists()
        self._init_duckdb()

    def _ensure_data_exists(self):
        """
        check if CIK exists in the consolidated file, otherwise run ETL
        """
        data_exists = False
        if self.parquet_path.exists():
            con = duckdb.connect(':memory:')
            try:
                count = con.execute(f"""
                    SELECT count(*) FROM parquet_scan('{str(self.parquet_path)}')
                    WHERE cik = '{self.cik}' AND form = '{self.filing_type}'
                """).fetchone()[0]
                if count > 0:
                    data_exists = True
            except Exception:
                pass

        if not data_exists:
            print(f"Data missing for CIK {self.cik} in {self.parquet_path}. Running ETL...")
            etl = FinancialDataETL(base_path=self.base_path, cik=self.cik, filing_type=self.filing_type)
            etl.process_company()

    def _init_duckdb(self):
        """Initialize DuckDB connection and register parquet file"""
        self.con = duckdb.connect(database=':memory:')
        self.con.sql(f"""
            CREATE OR REPLACE VIEW financial_data AS
            SELECT * FROM parquet_scan('{str(self.parquet_path)}')
        """)
        
    def _get_aliases(self, metric_name: str) -> List[str]:
        return self.METRIC_ALIASES.get(metric_name, [metric_name])

    def get_time_series(
            self, metric_name: str,
            custom_aliases: Optional[List[str]] = None
        ) -> pd.DataFrame:
        """
        get all values for a standardized concept over time
        """
        aliases = custom_aliases or self._get_aliases(metric_name)
        safe_aliases = [a.replace("'", "''") for a in aliases]
        alias_list_str = "', '".join(safe_aliases)
        
        query = f"""
            SELECT value, end_date, concept_id
            FROM financial_data
            WHERE cik = '{self.cik}'
            AND form = '{self.filing_type}'
            AND concept_id IN ('{alias_list_str}')
            ORDER BY end_date DESC
            LIMIT {self.years_statement * 5}
        """
        df_all = pd.DataFrame()
        try:
            df_all = self.con.sql(query).df()
        except Exception as e:
            print(f'error feteching time series for {metric_name}: {e}')
            return pd.DataFrame()
        
        if df_all.empty:
            return pd.DataFrame()
        
        for alias in aliases:
            subset = df_all[df_all['concept_id'] == alias].copy()
            
            if not subset.empty:
                subset['end_date'] = pd.to_datetime(subset['end_date'])
                return subset[['value', 'end_date']].sort_values(by='end_date', ascending=False).head(self.years_statement)
        
        return pd.DataFrame()

    def get_latest_value(self, metric_name: str) -> float:
        """
        get the most recent value for a concept
        """
        df = self.get_time_series(metric_name)
        if not df.empty:
            return float(df['value'].iloc[0])
        return 0.0

    def get_income_statement_metrics(self) -> Dict:
        """
        retrive income statement metrics
        """
        return {
            'revenues': self.get_time_series('revenues'),
            'operating_income': self.get_time_series('operating_income'),
            'net_income': self.get_time_series('net_income'),
            'interest_expense': self.get_time_series('interest_expense'),
            'tax_expense': self.get_time_series('tax_expense'),
        }

    def get_balance_sheet_metrics(self) -> Dict:
        """
        retrive standardized balance sheet metrics
        """
        return {
            'cash': self.get_latest_value('cash'),
            'equity': self.get_latest_value('equity'),
            'long_term_debt': self.get_latest_value('long_term_debt'),
            'short_term_debt': self.get_latest_value('short_term_debt'),
        }

    def get_shares_outstanding(self) -> float:
        """
        special handling for shares, usually have different names
        """        
        return self.get_latest_value('shares_outstanding')

    def close(self):
        """Close DuckDB connection"""
        self.con.close()


if __name__ == '__main__':
    # cik 104169 walmart
    # 1730168 broadcom
    #  2488 amd
    #  320193 apple
    # 1045810 nvidia
    # 1652044  google
    # 1018724 amazon
    process = FinancialDataProcessor(
        cik=2488,
        years_statement= 3
    )

    print(
        process.get_income_statement_metrics(),
        '*************************************',
        process.get_balance_sheet_metrics(),
    )