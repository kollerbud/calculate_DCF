from typing import Dict
from ollama import Client
from data_processor import FinancialDataProcessor
import asyncio


def format_for_llm(financial_data: Dict) -> str:
    """
    Format financial data into a clear text prompt for LLaMA model.
    
    Args:
        financial_data: Dictionary returned by get_llm_inputs()
        
    Returns:
        Formatted string ready to be used as LLaMA prompt
    """
    prompt_parts = []
    
    # Format Income Statement Data
    income_data = financial_data['income_statement']
    income_text = "Income Statement Analysis:\n"
    
    # Revenue Growth
    if not income_data['revenue'].empty:
        revenue_growth = income_data['revenue'].sort_values('end_date', ascending=False)
        income_text += f"\nRevenue Growth Rates (Year-over-Year):\n"
        for _, row in revenue_growth.iterrows():
            income_text += f"- {row['end_date']}: {row['growth_rate']:.1f}%\n"
    
    # Net Income Growth
    if not income_data['net_income'].empty:
        net_income_growth = income_data['net_income'].sort_values('end_date', ascending=False)
        income_text += f"\nNet Income Growth Rates:\n"
        for _, row in net_income_growth.iterrows():
            income_text += f"- {row['end_date']}: {row['growth_rate']:.1f}%\n"
    
    # Operating Expenses
    if not income_data['operating_expenses'].empty:
        opex = income_data['operating_expenses'].sort_values('end_date', ascending=False)
        income_text += f"\nOperating Expenses:\n"
        for _, row in opex.iterrows():
            income_text += f"- {row['end_date']}: ${row['value']:,.0f}\n"
    
    prompt_parts.append(income_text)
    
    # Format Balance Sheet Data
    balance_data = financial_data['balance_sheet']
    balance_text = "\nBalance Sheet Analysis:\n"
    
    # Current Assets
    balance_text += "\nCurrent Assets:\n"
    balance_text += f"- Cash: ${balance_data['cash']:,.0f}\n"
    balance_text += f"- Accounts Receivable: ${balance_data['accounts_receivable']:,.0f}\n"
    balance_text += f"- Inventory: ${balance_data['inventory']:,.0f}\n"
    
    # Non-current Assets
    balance_text += "\nNon-current Assets:\n"
    balance_text += f"- Property, Plant & Equipment: ${balance_data['ppe']:,.0f}\n"
    balance_text += f"- Intangible Assets: ${balance_data['intangible_assets']:,.0f}\n"
    
    # Liabilities and Equity
    balance_text += "\nLiabilities and Equity:\n"
    balance_text += f"- Total Liabilities: ${balance_data['total_liabilities']:,.0f}\n"
    balance_text += f"- Stockholders' Equity: ${balance_data['stockholders_equity']:,.0f}\n"
    
    prompt_parts.append(balance_text)
    
    # Format Cash Flow Data
    cash_flow_data = financial_data['cash_flow']
    cash_flow_text = "\nCash Flow Analysis:\n"
    
    # Operating Cash Flow
    if not cash_flow_data['operating_cash_flow'].empty:
        ocf = cash_flow_data['operating_cash_flow'].sort_values('end_date', ascending=False)
        cash_flow_text += "\nOperating Cash Flow:\n"
        for _, row in ocf.iterrows():
            cash_flow_text += f"- {row['end_date']}: ${row['value']:,.0f}\n"
    
    # Capital Expenditures
    if not cash_flow_data['capex'].empty:
        capex = cash_flow_data['capex'].sort_values('end_date', ascending=False)
        cash_flow_text += "\nCapital Expenditures:\n"
        for _, row in capex.iterrows():
            cash_flow_text += f"- {row['end_date']}: ${row['value']:,.0f}\n"
    
    # Working Capital Changes
    cash_flow_text += "\nWorking Capital Changes (Most Recent):\n"
    for metric in ['ar_change', 'inventory_change', 'ap_change']:
        if not cash_flow_data[metric].empty:
            value = cash_flow_data[metric].iloc[0]['value']
            cash_flow_text += f"- {metric.replace('_', ' ').title()}: ${value:,.0f}\n"
    
    prompt_parts.append(cash_flow_text)
    
    # Format Dividend Data
    dividend_data = financial_data['dividends']
    dividend_text = "\nDividend Analysis:\n"
    
    if not dividend_data['dividend_per_share'].empty:
        dps = dividend_data['dividend_per_share'].sort_values('end_date', ascending=False)
        dividend_text += "\nDividend Per Share:\n"
        for _, row in dps.iterrows():
            dividend_text += f"- {row['end_date']}: ${row['value']:.2f}\n"
    
    if not dividend_data['dividend_yield'].empty:
        yield_data = dividend_data['dividend_yield'].iloc[0]['value']
        dividend_text += f"\nCurrent Dividend Yield: {yield_data:.2f}%\n"
    
    if not dividend_data['payout_ratio'].empty:
        payout = dividend_data['payout_ratio'].iloc[0]['value']
        dividend_text += f"Dividend Payout Ratio: {payout:.2f}%\n"
    
    prompt_parts.append(dividend_text)
    
    # Combine all parts with system prompt
    system_prompt = """You are a financial analyst assistant. Based on the following financial data, provide a comprehensive analysis of the company's financial performance, focusing on:
1. Growth trends and profitability
2. Financial position and stability
3. Cash flow management
4. Shareholder returns

Financial Data:
"""
    
    final_prompt = system_prompt + '\n'.join(prompt_parts)
    return final_prompt


class FinancialAnalyzer:
    def __init__(self, model_name: str = "llama2:13b"):
        """
        Initialize the financial analyzer with Ollama client.
        
        Args:
            model_name: Name of the Ollama model to use
        """
        self.client = Client(host='http://localhost:11434')
        self.model_name = model_name
    
    def analyze_financials(self, financial_data: Dict) -> str:
        """
        Analyze financial data using the Ollama model.
        
        Args:
            financial_data: Dictionary from get_llm_inputs()
            
        Returns:
            Model's analysis of the financial data
        """
        prompt = format_for_llm(financial_data)
        
        # Generate response from the model
        response = self.client.chat(
            model=self.model_name,
            messages=[
                {
                    'role': 'user',
                    'content': prompt
                }
            ]
        )
        
        return response['message']['content']
    
    def analyze_financials_stream(self, financial_data: Dict):
        """
        Stream analysis of financial data using the Ollama model.
        
        Args:
            financial_data: Dictionary from get_llm_inputs()
            
        Returns:
            Generator yielding chunks of the model's analysis
        """
        prompt = format_for_llm(financial_data)
        
        stream = self.client.chat(
            model=self.model_name,
            messages=[
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            stream=True
        )
        
        for chunk in stream:
            if 'message' in chunk and 'content' in chunk['message']:
                yield chunk['message']['content']

# Example usage:
if __name__ == '__main__':
    # Initialize processor and analyzer
    # Initialize processor and analyzer
    processor = FinancialDataProcessor(cik='0000320193')
    analyzer = FinancialAnalyzer(model_name="llama3.2:1b")

    # Get financial data
    financial_data = processor.get_llm_inputs()

    # Option 1: Get complete analysis
    analysis = analyzer.analyze_financials(financial_data)
    print("Complete Analysis:", analysis)

    # Option 2: Stream the analysis
    def stream_analysis():
        for chunk in analyzer.analyze_financials_stream(financial_data):
            print(chunk, end='', flush=True)

    # Run streaming analysis
    stream_analysis()

    # Clean up
    processor.close()
