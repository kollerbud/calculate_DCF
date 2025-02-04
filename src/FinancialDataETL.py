import pandas as pd
from datetime import datetime
from typing import Dict, List
from pathlib import Path
from extractEdgar import EDGARClient, FinancialFact


class FinancialDataETL:

    def __init__(self, base_path: str, cik: str):
        """
        Initialize ETL process

        Args:
            base_path: Root directory for Parquet files
            cik: Company CIK number
        """
        self.base_path = str(base_path)  # Store as string
        self.cik = self._format_cik(str(cik))  # Store CIK as string
        Path(self.base_path).mkdir(parents=True, exist_ok=True)

    def _format_cik(self, cik: str) -> str:
        """Format CIK to 10 digits with leading zeros"""
        return str(cik).zfill(10)

    def _get_table_path(self) -> Path:
        """Generate path for Parquet files using CIK number"""
        return Path(self.base_path) / f"{self.cik}.parquet"

    def extract_company_data(self) -> Dict[str, List[FinancialFact]]:
        """Extract all financial data from SEC API"""
        try:
            client = EDGARClient(cik=self.cik)
            return client.get_all_facts()
        except Exception as e:
            raise Exception(f"Failed to extract data for CIK {self.cik}: {str(e)}")

    def transform_to_dataframes(
        self,
        facts_by_concept: Dict[str, List[FinancialFact]],
    ) -> Dict[str, pd.DataFrame]:
        """
        Transform financial facts into normalized DataFrames
        Returns:
            Dictionary containing 'facts' and 'concepts' DataFrames
        """
        facts_records = []
        concepts_records = set()

        try:
            for key, facts in facts_by_concept.items():
                taxonomy, concept, unit = key.split(':')

                # Add concept metadata if facts exist
                if facts:
                    concepts_records.add((
                        concept,
                        taxonomy,
                        facts[0].label or '',  # Handle None labels
                        unit
                    ))

                # Process each fact
                for fact in facts:
                    fact_record = {
                        'cik': self.cik,
                        'concept_id': concept,
                        'taxonomy': taxonomy,
                        'value': fact.value,
                        'unit': unit,
                        'start_date': fact.start_date,
                        'end_date': fact.end_date,
                        'filing_date': fact.filing_date,
                        'frame': fact.frame,
                        'form': fact.form
                    }
                    facts_records.append(fact_record)

            # Create DataFrames
            facts_df = pd.DataFrame(facts_records)
            concepts_df = pd.DataFrame(
                concepts_records,
                columns=['concept_id', 'taxonomy', 'label', 'unit']
            )

            # Convert dates and handle missing values
            date_cols = ['start_date', 'end_date', 'filing_date']
            for col in date_cols:
                facts_df[col] = pd.to_datetime(facts_df[col], errors='coerce')

            # Add metadata
            current_time = datetime.now()
            facts_df['updated_at'] = current_time
            concepts_df['updated_at'] = current_time

            return {'facts': facts_df, 'concepts': concepts_df}

        except Exception as e:
            raise Exception(f"Transformation failed: {str(e)}")

    def load_to_parquet(
        self,
        dataframes: Dict[str, pd.DataFrame],
        mode: str = "append"
    ) -> None:
        """Load DataFrames to Parquet file"""
        try:
            facts_df = dataframes['facts']
            concepts_df = dataframes['concepts']

            if facts_df.empty:
                print(f"No facts data for CIK {self.cik}")
                return

            combined_df = facts_df.merge(
                concepts_df,
                on=['concept_id', 'taxonomy', 'unit'],
                how='left'
            )

            file_path = self._get_table_path()

            if mode == "append" and file_path.exists():
                existing_df = pd.read_parquet(file_path)
                composite_key = ['concept_id', 'start_date', 'end_date', 'filing_date', 'form']
                combined_df = pd.concat([existing_df, combined_df]).drop_duplicates(
                    subset=composite_key,
                    keep='last'
                )

            combined_df.to_parquet(file_path, index=False)

        except Exception as e:
            raise Exception(f"Loading failed: {str(e)}")

    def process_company(self,
                        mode: str = "append") -> None:
        """Complete ETL process for a company"""
        try:
            facts = self.extract_company_data()
            dataframes = self.transform_to_dataframes(facts)
            self.load_to_parquet(dataframes, mode)
            print(f"Successfully processed CIK {self.cik}")
        except Exception as e:
            print(f"Error processing CIK {self.cik}: {str(e)}")
