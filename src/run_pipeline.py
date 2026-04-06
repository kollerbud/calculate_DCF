from src.data_processor import FinancialDataProcessor
from src.DCF_calculator import DCFModel
# from sentiment_pipeline.news_extractors import YahooNewsExtractor
from src.investment_committee import InvestmentCommittee

def gather_dcf_data(ticker: str, cik: str) -> str:
    """Runs the DCF pipeline and formats the output for the LLM."""
    print(f"\n--- Running Quant Pipeline for {ticker} ({cik}) ---")
    try:
        loader = FinancialDataProcessor(cik=cik, years_statement=4, filing_type='10-K', persist_data=False)
        model = DCFModel(loader)
        
        if not model.raw['valid']:
            return "DCF Calculation Failed: Missing fundamental data from SEC."

        # Grab the "Average" scenario as our baseline for the LLM
        scenarios = model.generate_scenarios()
        avg_scenario = next(s for s in scenarios if s.name == "Average")
        result = model.calculate_dcf(avg_scenario)
        
        # Package this into a clean dictionary for the LLM to read
        dcf_summary = {
            "business_model": model.raw['business_model'],
            "historical_cagr": f"{model.raw['cagr']:.1%}",
            "4_yr_avg_margin": f"{model.raw['avg_margin']:.1%}",
            "capex_to_revenue": f"{model.raw['capex_to_rev']:.1%}",
            "wacc_used": f"{avg_scenario.wacc:.1%}",
            "implied_share_price": f"${result['price']:.2f}",
            "terminal_growth_rate": f"{avg_scenario.terminal_growth:.1%}"
        }
        return json.dumps(dcf_summary, indent=2)
    except Exception as e:
        return f"Error calculating DCF: {e}"

def gather_news_data(ticker: str) -> str:
    """Scrapes Yahoo News and formats the headlines/summaries for the LLM."""
    print(f"\n--- Running Sentiment Pipeline for {ticker} ---")
    try:
        extractor = YahooNewsExtractor(ticker)
        df_news = extractor.fetch_metadata()
        
        if df_news.empty:
            return "No recent news found."
            
        # We don't need the full HTML body for the Sentiment Agent, just titles and summaries
        # limits to top 5 articles to save LLM context window limits
        news_list = []
        for _, row in df_news.head(5).iterrows():
            news_list.append(f"Date: {row['publish_date']} | Title: {row['title']} | Summary: {row['summary']}")
            
        return "\n".join(news_list)
    except Exception as e:
        return f"Error gathering news: {e}"

def main():
    # Example: Let's run the pipeline for AMD
    target_ticker = "AMD"
    target_cik = "2488" 
    
    # 1. Gather Data
    dcf_text = gather_dcf_data(target_ticker, target_cik)
    news_text = gather_news_data(target_ticker)
    
    # Placeholder for Phase 3 (RAG over 10-K)
    sec_text = "SEC 10-K RAG pipeline is currently under construction. Assume fundamental business operations are steady."

    # 2. Spin up the AI Committee
    # Set to "ollama" and your preferred model (e.g., "llama3" or "qwen2.5-coder")
    committee = InvestmentCommittee(provider="ollama", model_name="llama3.2:latest")
    
    # 3. Kick off the debate
    final_memo = committee.run_analysis(
        ticker=target_ticker,
        dcf_data=dcf_text,
        news_data=news_text,
        sec_data=sec_text
    )
    
    print("\n==================================================")
    print("             FINAL INVESTMENT MEMO                ")
    print("==================================================")
    print(final_memo)

if __name__ == "__main__":
    main()