import pandas as pd
import re
from pathlib import Path
from edgar import Company, set_identity, MultiFinancials
from edgar.xbrl import XBRLS

# SEC identity
set_identity('name_company@gmail.com')

class FinancialDataETL:
    CONSOLIDATED_FILE = 'consolidated_financials.parquet'

    def __init__(self, cik: str, base_path: str='financial_data',
                 years: int = 10, filing_type: str = '10-K'):
        """
        Initialize ETL process

        Args:
            base_path: Root directory for Parquet files
            cik: Company CIK number
        """
        self.cik = str(cik).zfill(10)
        self.base_path = Path(base_path)

        self.filing_type = filing_type
        self.years = years

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.file_path = self.base_path / self.CONSOLIDATED_FILE


    def extract_and_transform(self) -> pd.DataFrame:
        """
        get financials, stitch them and applies label mapping
        """
        print(f'getting last {self.years} years of {self.filing_type} for CIK {self.cik}')

        try:
            company = Company(self.cik)

            filings = company.get_filings(form=self.filing_type).latest(self.years)
            if not filings:
                print(f'no {self.filing_type} filings found for {self.cik}')
                return pd.DataFrame()
            xbrls = XBRLS.from_filings(filings)

            dfs = {
                'Income Statement': xbrls.statements.income_statement().to_dataframe(),
                'Balance Sheet': xbrls.statements.balance_sheet().to_dataframe(),
                'Cash Flow': xbrls.statements.cashflow_statement().to_dataframe(),
            }
        except Exception as e:
            print(f'failed to get data: {e}')
            return pd.DataFrame()

        all_records = []

        for statement_type, df in dfs.items():
            if df.empty: continue

            df = df.reset_index()

            if 'label' in df.columns:
                df['concept_id'] = df['label'].astype(str).str.strip()
            else:
                df.rename(columns={df.columns[0]: 'concept_id'}, inplace=True)
                df['concept_id'] = df['concept_id'].astype(str).str.strip()
            # --- DATE HANDLING (Regex) ---
            # Finds columns like "2025-11-02"
            date_cols = [c for c in df.columns if re.match(r'^\d{4}-\d{2}-\d{2}$', str(c))]
            if not date_cols:
                continue
            # melt to long format (concept_id | end_data| value)
            melted = df.melt(
                id_vars=['concept_id'],
                value_vars=date_cols,
                var_name='end_date',
                value_name='value'
            )

            melted['statement_type'] = statement_type
            all_records.append(melted)

        if not all_records:
            return pd.DataFrame()

        # combien and clean up
        combined_df = pd.concat(all_records, ignore_index=True)
        combined_df['cik'] = self.cik
        combined_df['end_date'] = pd.to_datetime(combined_df['end_date'])
        combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
        combined_df['form'] = self.filing_type

        return combined_df.dropna(subset=['value'])

    def save_consolidated(self, new_df: pd.DataFrame) -> None:
        """
        append new data to consolidated parquet file.
        remove existing data for this cik before appending to avoid duplicates
        """

        if new_df.empty:
            return None

        if self.file_path.exists():
            try:
                existing_df = pd.read_parquet(self.file_path)
                # remove old data for this specific cik
                combined = pd.concat([existing_df, new_df], ignore_index=True)

                final_df = combined.drop_duplicates(
                    subset=['cik', 'concept_id', 'end_date', 'form'],
                    keep='last',
                )
            except Exception as e:
                print(f"Warning: Could not read existing file ({e}). Overwriting.")
                final_df = new_df
        else:
            final_df = new_df

        final_df.to_parquet(self.file_path, index=False)
        print(f"Saved/Updated records for {self.cik}. Total rows in DB: {len(final_df)}")

    def process_company(self) -> None:
        df = self.extract_and_transform()
        self.save_consolidated(df)

if __name__ == '__main__':
    # cik 104169 walmart
    # 1730168 broadcom
    #  2488 amd
    #  320193 apple
    # 1045810 nvidia
    # 1652044  google
    # 1018724 amazon

    company = Company(cik_or_ticker="1730168")
    filings = company.get_filings(form="10-K").latest(4)
    # annual_report = company.latest('10-K')

    multi_financials = MultiFinancials.extract(filings)
    multi_income = multi_financials.income_statement()
    multi_balance = multi_financials.balance_sheet()
    multi_cash = multi_financials.cashflow_statement()

    xbrl = XBRLS.from_filings(filings)

    # financials = company.get_financials()

    # f = FinancialDataETL(cik=2488)

    # print(
    #     f.process_company()
    # )
    print(
        # multi_income.to_dataframe()
        # '-----------------income statement----------------------------',
        # xbrl.statements.income_statement().to_dataframe(),
        # '--------------balance sheet------------------------------',
        # xbrl.statements.balance_sheet().to_dataframe(),
        # '--------------cash flow------------------------------',
        xbrl.statements.cashflow_statement().to_dataframe(),
    )