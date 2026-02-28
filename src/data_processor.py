import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from edgar import Company, set_identity
from edgar.xbrl import XBRLS
import re


set_identity('google@google.com')

class FinancialDataProcessor:

    METRIC_ALIASES = {
        'revenue': [
            "Contract Revenue",  "Total net revenue", "Revenue", "Net sales", "Net revenue",
            "Total net sales",
            "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"
        ],
        'operating_income': [
            "operating_income", "Operating Income", "Operating income",
            "Operating Income (Loss)", "OperatingIncomeLoss",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxes"
        ],
        'net_income': [
            "Profit or Loss", "Net Income", "Net income",
            "Net Income from Continuing Operations",
            "Basic Net Income Available to Common Shareholders", "NetIncomeLoss"
        ],
        'operating_cash_flow': [
            "operating_cash_flow",
            "Net Cash from Operating Activities",
            "NetCashProvidedByUsedInOperatingActivities",
            "Net cash provided by (used in) operating activities",
            "Net cash provided by operating activities",
            "Net cash provided by operating activities, continuing operations",
            "Net Cash Provided by (Used in) Operating Activities"
        ],
        'capex': [
            "capex",
            "Property and equipment, net",
            "Capital expenditures",
            "Payments to acquire property, plant and equipment",
            "Purchases of property, plant and equipment",
            "Purchases of property and equipment",
        ],
        'interest_expense': [
            "Interest expense",
            "Interest expense, net", "Interest Expense (non-operating)",
            "InterestPaid"
        ],
        'tax_expense': [
            "tax_expense", "Income Tax Expense", "Income tax provision (benefit)",
            "Provision for Income Taxes", "Provision for (benefit from) income taxes",
            "IncomeTaxExpenseBenefit", "IncomeTaxPaid"
        ],
        'cash': [
            "cash", "Cash and cash equivalents", "Cash and Cash Equivalents",
            "Cash, cash equivalents and restricted cash",
            "CashAndCashEquivalentsAtCarryingValue"
        ],
        'equity': [
            "equity", "Total stockholders' equity", "Total stockholders’ equity",
            "Total equity", "Total shareholders' equity", "Shareholders' equity",
            "StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
        ],
        'long_term_debt': [
            "long_term_debt", "Long Term Debt", "Long-Term Debt", "Long-term debt",
            "Long-term debt, net of current portion", "LongTermDebt", "LongTermDebtAndCapitalLeaseObligations"
        ],
        'short_term_debt': [
            "short_term_debt", "Short-term debt", "Short-term debt",
            "Current portion of long-term debt", "ShortTermDebt", "DebtCurrent"
        ],

        'shares_outstanding': [
            "shares_outstanding", "Diluted",
            "Common Stock Shares Outstanding",
            "Weighted average shares outstanding - diluted",
            "Shares Outstanding (Diluted)", "EntityCommonStockSharesOutstanding",
            "CommonStockSharesOutstanding", "WeightedAverageNumberOfDilutedSharesOutstanding"
        ]
    }

    def __init__(self,
            cik: str,
            base_path: str = 'financial_data',
            filing_type: str = '10-K',
            years_statement: int = 5,
            persist_data: bool = False
        ):
        self.cik = str(cik).zfill(10)
        self.base_path = Path(base_path)
        self.filing_type = filing_type
        self.years_statement = years_statement
        self.persist_data = persist_data
        self.parquet_path = self.base_path / 'consolidated_financials.parquet'

        self._raw_data = self._fetch_and_transform_data()

        self._init_duckdb()

        if self.persist_data:
            self._save_to_parquet()

    def _fetch_and_transform_data(self):
        try:
            company = Company(self.cik)
            filings = company.get_filings(form=self.filing_type).latest(self.years_statement)

            if not filings:
                print(f"No {self.filing_type} filings found for {self.cik}")
                return pd.DataFrame()

            xbrls = XBRLS.from_filings(filings=filings)
            statements = {
                'Income Statement': xbrls.statements.income_statement().to_dataframe(),
                'Balance Sheet': xbrls.statements.balance_sheet().to_dataframe(),
                'Cash Flow': xbrls.statements.cashflow_statement().to_dataframe(),
            }
            all_records = []
            for stmt_name, df in statements.items():
                if df.empty: continue

                df = df.reset_index()
                if 'label' in df.columns:
                    df['concept_id'] = df['label'].astype(str).str.strip()
                else:
                    df['concept_id'] = df.iloc[:, 0].astype(str).str.strip()

                date_cols = [c for c in df.columns if re.match(r'^\d{4}-\d{2}-\d{2}$', str(c))]
                if not date_cols: continue

                melted = df.melt(
                    id_vars=['concept_id'],
                    value_vars=date_cols,
                    var_name='end_date',
                    value_name='value'
                )
                melted['statement_type'] = stmt_name
                all_records.append(melted)

            if not all_records:
                return pd.DataFrame()

            combined = pd.concat(all_records, ignore_index=True)
            combined['cik'] = self.cik
            combined['end_date'] = pd.to_datetime(combined['end_date'])
            combined['value'] = pd.to_numeric(combined['value'], errors='coerce')
            combined['form'] = self.filing_type

            return combined.dropna(subset=['value'])

        except Exception as e:
            print(f'error fetching dta for {self.cik}: {e}')
            return pd.DataFrame()

    def _init_duckdb(self):
        """Initialize DuckDB connection and register parquet file"""
        self.con = duckdb.connect(database=':memory:')
        if not self._raw_data.empty:
            self.con.register('financial_data', self._raw_data)
        else:
            # Create empty table structure if no data
            self.con.execute("""
                CREATE TABLE financial_data (
                    concept_id VARCHAR,
                    end_date TIMESTAMP,
                    value DOUBLE,
                    statement_type VARCHAR,
                    cik VARCHAR,
                    form VARCHAR
                )
            """)

    def _save_to_parquet(self):

        if self._raw_data.empty:
            return None

        self.base_path.mkdir(parents=True, exist_ok=True)

        try:
            if self.parquet_path.exists():
                existing_df = pd.read_parquet(self.parquet_path)
                combined = pd.concat([existing_df, self._raw_data], ignore_index=True)
                final_df = combined.drop_duplicates(
                    subset=['cik', 'concept_id', 'end_date', 'form'],
                    keep='last'
                    )
            else:
                final_df = self._raw_data

            final_df.to_parquet(self.parquet_path, index=False)
            print(f'save data for {self.cik} to {self.parquet_path}')

        except Exception as e:
            print(f'parquet not saved: {e}')

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

        try:
            df_all = self.con.sql(query).df()
        except Exception as e:
            print(f'error feteching time series for {metric_name}: {e}')
            return pd.DataFrame()

        if df_all.empty:
            return pd.DataFrame()

        df_all['end_date'] = pd.to_datetime(df_all['end_date'])
        # create a priority mapping of 'aliases'
        priority_map = {alias: i for i, alias in enumerate(aliases)}
        df_all['priority'] = df_all['concept_id'].map(priority_map)
        # sort by date
        df_all = df_all.sort_values(by=['end_date', 'priority'], ascending=[False, True])
        # drop duplicates on 'end_date'
        df_clean = df_all.drop_duplicates(subset=['end_date'], keep="first")

        return df_clean[['value', 'end_date']].head(self.years_statement)

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
        retrive income statement metrics, for debugging raw data
        """
        return {
            'revenues': self.get_time_series('revenue'),
            'operating_income': self.get_time_series('operating_income'),
            'net_income': self.get_time_series('net_income'),
            'interest_expense': self.get_time_series('interest_expense'),
            'tax_expense': self.get_time_series('tax_expense'),
        }

    def get_balance_sheet_metrics(self) -> Dict:
        """
        retrive standardized balance sheet metrics, for debugging raw data
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
        cik=1045810,
        years_statement= 4
    )
    # print(process.get_time_series(metric_name='long_term_debt'))
    debug_df = process.con.sql("""
        SELECT DISTINCT concept_id
        FROM financial_data
        WHERE LOWER(concept_id) LIKE '%equity%'
          -- OR LOWER(concept_id) LIKE '%expense%'
    """).df()
    print("AVAILABLE SHARE LABELS IN DB:")
    print(debug_df.to_string())

    print(
        # process.get_income_statement_metrics(),
        process.get_balance_sheet_metrics(),
    )