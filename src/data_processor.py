import os
import pandas as pd
from pydantic import BaseModel, Field
from edgar import Company, set_identity
from crewai import Agent, Task, Crew, LLM
from dotenv import load_dotenv

set_identity('google@google.com')
load_dotenv()

class StandardizedFinancials(BaseModel):
    year: int = Field(..., description="The fiscal year of the report.")
    revenue: float = Field(..., description="Total revenue in millions.")
    ebit: float = Field(..., description="Operating Income (EBIT) in millions.")
    tax_expense: float = Field(..., description="Provision for income taxes in millions.")
    pre_tax_income: float = Field(..., description="Income before taxes (EBT) in millions.")
    depreciation_and_amortization: float = Field(..., description="Total D&A in millions.")
    capital_expenditure: float = Field(..., description="Capital expenditures (Purchases of property & equipment) in millions. Output as a positive number.")
    net_working_capital_change: float = Field(..., description="Change in net working capital in millions.")
    total_debt: float = Field(..., description="Short-term + Long-term debt in millions.")
    cash_and_equivalents: float = Field(..., description="Cash and cash equivalents in millions.")
    shares_outstanding: float = Field(..., description="Total weighted average shares outstanding (diluted) in millions.")    


class FinancialDataProcessor:
    'use llm to extract standardized financial metrics'

    def __init__(self,
            provider: str = 'ollama_cloud',
            model_name: str = 'qwen3.5:cloud',
        ):
        
        self.llm = self._get_llm(provider, model_name)
        self.extractor_agent = Agent(
            role='Senior Forensic Accountant',
            goal='Extract precise numeric data from raw financial statements and map them to standard DCF inputs.',
            backstory=(
                "You are a meticulous forensic accountant. You read raw SEC 10-K tables "
                "and extract the exact numeric values requested. You understand that "
                "'Net Sales' means Revenue, and 'Purchases of Property' means CapEx. "
                "You strictly format outputs as requested and never hallucinate numbers."
            ),
            verbose=True,
            allow_delegation=False,
            llm=self.llm   
        )
        
    def _get_llm(self, provider: str, model_name: str):
        """Factory method to route to the correct LLM provider using CrewAI's native LLM wrapper."""
        provider = provider.lower()
        
        if provider == "ollama":
            # CrewAI requires the 'ollama/' prefix so it knows to route locally
            return LLM(
                model=f"ollama/{model_name}",
                base_url="http://localhost:11434"
            )
        
        elif provider == "ollama_cloud":
            # Remote/Cloud hosted Ollama using OpenAI Compatibility Mode
            cloud_url = os.getenv("OLLAMA_CLOUD_URL")
            if not cloud_url:
                raise ValueError("OLLAMA_CLOUD_URL missing. Set to http://your-ip:11434")
            
            # Ensure the URL ends with /v1 so it hits Ollama's OpenAI-compatible endpoint
            clean_url = cloud_url.rstrip('/')
            if not clean_url.endswith("/v1"):
                clean_url = f"{clean_url}/v1"
            
            # LiteLLM requires *some* string for the API key, even if Ollama ignores it
            api_key = os.getenv("OLLAMA_CLOUD_API_KEY", "ollama-dummy-key") 
            
            return LLM(
                model=model_name,
                base_url=cloud_url,
                api_key=api_key
            )
            
            
        elif provider == "deepseek":
            api_key = os.getenv("DEEPSEEK_API_KEY")
            if not api_key:
                raise ValueError("DEEPSEEK_API_KEY environment variable is missing in .env")
            return LLM(
                model=f"deepseek/{model_name}", 
                api_key=api_key, 
                base_url="https://api.deepseek.com"
            )
            
        elif provider == "qwen":
            api_key = os.getenv("DASHSCOPE_API_KEY")
            if not api_key:
                raise ValueError("DASHSCOPE_API_KEY environment variable is missing in .env")
            
            # The Full Hijack: Force the underlying OpenAI client to reroute to DashScope
            os.environ["OPENAI_API_KEY"] = api_key
            os.environ["OPENAI_API_BASE"] = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
            # https://dashscope-intl.aliyuncs.com/api/v1
            #https://dashscope-intl.aliyuncs.com/compatible-mode/v1
            
            return LLM(
                model=f"openai/{model_name}" 
            )

        elif provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable is missing in .env")
            return LLM(model=model_name, api_key=api_key)
            
        else:
            raise ValueError(f"Unsupported provider: {provider}. Choose ollama, deepseek, qwen, or openai.")
    
    def _get_markdown_from_filing(self, filing) -> str:
        """Helper function: Takes a specific EDGAR filing and returns markdown."""
        financials = filing.obj().financials
        
        # Notice the added () to properly execute the method before calling to_markdown()
        raw_text = f"--- INCOME STATEMENT ---\n{financials.income_statement().to_markdown()}\n\n"
        raw_text += f"--- BALANCE SHEET ---\n{financials.balance_sheet().to_markdown()}\n\n"
        raw_text += f"--- CASH FLOW STATEMENT ---\n{financials.cashflow_statement().to_markdown()}"
        
        return raw_text
    
    def extract_historical_data(self, ticker: str, years: int = 5) -> pd.DataFrame:
        """
        The multi-year pipeline. Loops through the last N filings, 
        uses the LLM to extract data cleanly, and returns a Pandas DataFrame.
        """
        print(f"Fetching last {years} years of 10-K filings for {ticker}...")
        company = Company(ticker)
        
        # Get the last N annual reports (10-K)
        # .head(years) gets the most recent ones
        filings = company.get_filings(form="10-K").head(years) 
        
        historical_data = []

        for filing in filings:
            filing_year = str(filing.filing_date.year)
            print(f"Asking LLM to extract data for fiscal year {filing_year}...")
            
            # 1. Get just this specific year's tables
            raw_financials = self._get_markdown_from_filing(filing)
            
            # 2. Tell the LLM to extract it
            extraction_task = Task(
                description=(
                    f"Review the following financial statements for {ticker} for the year {filing_year}.\n\n"
                    f"{raw_financials}\n\n"
                    "Extract the metrics for this specific fiscal year. "
                    "Ensure all values are represented in MILLIONS."
                ),
                agent=self.extractor_agent,
                expected_output="Raw, valid JSON representing the financial data.",
                output_pydantic=StandardizedFinancials
            )
            
            crew = Crew(agents=[self.extractor_agent], tasks=[extraction_task], verbose=False)
            result = crew.kickoff()
            
            # 3. Add to our historical list
            dcf_dict = result.pydantic.model_dump()
            historical_data.append(dcf_dict)

        # 4. Convert the list of strict dictionaries into a clean Pandas DataFrame
        df = pd.DataFrame(historical_data)
        
        # Sort so the oldest year is first, newest is last (standard for financial modeling)
        df = df.sort_values(by='year').reset_index(drop=True)
        
        print(f"Successfully extracted {len(df)} years of history!")
        return df




if __name__ == '__main__':
    process = FinancialDataProcessor(provider='qwen', model_name='qwen2.5-7b-instruct')
    hist_df = process.extract_historical_data(ticker='nvda', years=2)
    print(hist_df)