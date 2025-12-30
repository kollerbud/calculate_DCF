import pandas as pd
import re
from pathlib import Path
from edgar import Company, set_identity, MultiFinancials
from edgar.xbrl import XBRLS

# SEC identity
set_identity('name_company@gmail.com')

class FinancialDataETL:

    def __init__(self, base_path: str, cik: str):
        """
        Initialize ETL process

        Args:
            base_path: Root directory for Parquet files
            cik: Company CIK number
        """
        self.base_path = Path(base_path)
        self.cik = str(cik).zfill(10)  # Store CIK as string
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_table_path(self) -> Path:
        """Generate path for Parquet files using CIK number"""
        return self.base_path / f"{self.cik}.parquet"

    def extract_and_transform(self) -> pd.DataFrame:
        """
        get standardized financials using edgartools and
        transform them into a single long-format DataFrame
        """
        print('get financials for CIK {self.cik}')

        try:
            company = Company(self.cik)
            financials = company.get_financials()
            if not financials:
                raise Exception('no financials')
        except Exception as e:
            raise Exception(f'failed to get data from edgar')

        # 1. Get the raw dataframes (Don't forget the parentheses!)
        dfs = {
            'Income Statement': financials.income_statement().to_dataframe(),
            'Balance Sheet': financials.balance_sheet().to_dataframe(),
            'Cash Flow': financials.cashflow_statement().to_dataframe(),
        }

        all_records = []
        for statement_type, df in dfs.items():
            df = df.reset_index()

            if 'label' in df.columns:
                df['concept'] = df['label']
            else:
                df.rename(columns={df.columns[0]: 'concept'}, inplace=True)

            date_cols = []
            for col in df.columns:
                if re.match(r'^\d{4}-\d{2}-\d{2}$', str(col)):
                    date_cols.append(col)
            if not date_cols:
                print(f"Warning: No date columns found for {statement_type}")
                continue

            # melt only the valid date columns
            melted = df.melt(
                id_vars=['concept'],
                value_vars=date_cols,
                var_name='end_date',
                value_name='value'
            )
            melted['statement_type'] = statement_type
            all_records.append(melted)

        if not all_records:
            return pd.DataFrame()

        combined_df = pd.concat(all_records, ignore_index=True)
        # Final Cleanup
        combined_df['cik'] = self.cik
        combined_df['end_date'] = pd.to_datetime(combined_df['end_date'])
        # Coerce values to numbers (handling any stray non-numeric characters)
        combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')

        # Drop rows where value is NaN (but keep 0.0)
        return combined_df.dropna(subset=['value'])


    def load_to_parquet(
        self,
        df: pd.DataFrame,
        mode: str = "overwrite"
    ) -> None:
        """Load DataFrames to Parquet file"""

        file_path = self._get_table_path()

        if mode =='append' and file_path.exists():
            existing_df = pd.read_parquet(file_path)
            df = pd.concat([existing_df, df]).drop_duplicates(
                subset=['concept', 'end_date', 'statement_type'],
                keep='last'
            )
        df.to_parquet(file_path, index=False)
        print(f"Saved {len(df)} standardized records to {file_path}")

    def process_company(self, mode: str = "overwrite") -> None:
        """Complete ETL process for a company"""
        df = self.extract_and_transform()
        self.load_to_parquet(df, mode)

if __name__ == '__main__':
    # cik 104169 walmart
    # 1730168 broadcom
    #  2488 amd
    #  320193 apple
    # 1045810 nvidia
    # 1652044  google
    # 1018724 amazon

    # label to check for, income statement
    # revenues -> "Contract Revenue"/"Revenue"
    # operating_income -> "Operating Income"
    # interest_expense -> "Interest Expense"
    # tax_expense -> 
    # pretax_income
    # shares_outstanding

    # balance sheet
    # cash
    # short_term_debt
    # long_term_debt
    # equity

    company = Company(cik_or_ticker=1652044)
    filings = company.get_filings(form="10-K").head(5)

    multi_financials = MultiFinancials.extract(filings)
    multi_income = multi_financials.income_statement()
    multi_balance = multi_financials.balance_sheet()
    multi_cash = multi_financials.cashflow_statement()
    
    xbrl = XBRLS.from_filings(filings)

    financials = company.get_financials()

    print(
        # facts.query().by_concept('Revenue').to_dataframe()
        # df_income.loc[df_income['label']=='Revenue']
        # financials.get_revenue(),
        # financials.get_net_income(),
        # financials.get_free_cash_flow()
    )
    print(
        # multi_income
    )
    
    print(
        xbrl.query().by_concept("InterestPaid").to_dataframe()
    )