import os
from dotenv import load_dotenv
from crewai import Agent, LLM

# Load API keys from the .env file
load_dotenv()

class InvestmentCommittee:
    def __init__(self, provider: str = "ollama_cloud", model_name: str = "gpt-oss:20b-cloud"):
        """
        Initializes the committee with a flexible LLM backend.
        Supported providers: 'ollama', 'deepseek', 'qwen', 'openai'
        """
        print(f"Initializing Investment Committee using {provider.upper()} model: {model_name}...")
        
        # Initialize the dynamic LLM
        self.llm = self._get_llm(provider, model_name)
        
        # Build the Agents
        self.quant = self._create_quant_agent()
        self.fundamental = self._create_fundamental_agent()
        self.sentiment = self._create_sentiment_agent()
        self.portfolio_manager = self._create_pm_agent()

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
            return LLM(
                model=f"openai/{model_name}", # Qwen uses OpenAI compatible routing
                api_key=api_key, 
                base_url="https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
            )

        elif provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable is missing in .env")
            return LLM(model=model_name, api_key=api_key)
            
        else:
            raise ValueError(f"Unsupported provider: {provider}. Choose ollama, deepseek, qwen, or openai.")

    # --- AGENT DEFINITIONS ---
    def _create_quant_agent(self) -> Agent:
        return Agent(
            role='Lead Quantitative Analyst',
            goal='Determine if the stock is undervalued based strictly on DCF mathematics and margin of safety.',
            backstory=(
                "You are a ruthless, mathematically driven Quantitative Analyst. "
                "You do not care about news, product hype, or CEO personalities. You only care about "
                "cash flow, WACC, and terminal growth. You look at Discounted Cash Flow (DCF) models "
                "and compare the Implied Share Price to the current market price. "
                "If the math doesn't show a clear margin of safety, you reject the stock."
            ),
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )

    def _create_fundamental_agent(self) -> Agent:
        return Agent(
            role='Senior Fundamental Analyst',
            goal='Assess the economic moat, systemic risks, and strategic health of the business from SEC filings.',
            backstory=(
                "You are a meticulous, long-term Fundamental Equity Analyst. You ignore short-term price "
                "movements and mathematical formulas. Your job is to read the 'ground truth' in SEC 10-K "
                "and 10-Q filings. You look for deep competitive advantages (economic moats), supply chain "
                "vulnerabilities, and regulatory risks. You focus on whether the underlying business is "
                "structurally strengthening or deteriorating."
            ),
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )

    def _create_sentiment_agent(self) -> Agent:
        return Agent(
            role='Short-Term Momentum Trader',
            goal='Gauge the short-term market psychology and identify immediate catalysts based on recent news.',
            backstory=(
                "You are a fast-paced Momentum Trader. You thrive on market psychology, news cycles, "
                "and narrative velocity. You don't care what a company will do in 10 years; you care what "
                "the stock will do next week. You review recent news headlines and summaries to spot "
                "immediate catalysts (e.g., earnings surprises, product launches, macro panics) and "
                "measure the current market mood."
            ),
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )

    def _create_pm_agent(self) -> Agent:
        return Agent(
            role='Lead Portfolio Manager',
            goal='Synthesize conflicting reports and make a final, decisive investment ruling.',
            backstory=(
                "You are the Lead Portfolio Manager of a multi-billion dollar fund. Your Quant, Fundamental "
                "Analyst, and Sentiment Trader report directly to you. They will often disagree. "
                "Your job is to synthesize their perspectives, weigh the quantitative valuation against "
                "the fundamental moat and short-term momentum, and make a final, decisive investment call "
                "(Strong Buy, Accumulate, Hold, Trim, or Strong Sell). You must justify why certain factors "
                "outweighed others."
            ),
            verbose=True,
            allow_delegation=True, 
            llm=self.llm
        )

    def run_analysis(self, ticker: str, dcf_data: str, news_data: str, sec_data: str = "SEC data pending RAG implementation."):
        """
        Creates the tasks with injected real-world data and kicks off the AI committee.
        """
        from crewai import Task, Crew, Process
        
        print(f"\n[{ticker}] Convening the Investment Committee...")

        task_quant = Task(
            description=(
                f"Analyze this DCF output for {ticker}:\n{dcf_data}\n\n"
                "Look at the Implied Share Price vs the Current Market Price. "
                "Assess the historical CAGR and the WACC used. Is there a margin of safety? "
                "Output a strict BUY, SELL, or HOLD rating based ONLY on this math."
            ),
            agent=self.quant,
            expected_output="A strict BUY/SELL/HOLD rating based on DCF math, with 3 bullet points."
        )

        task_fundamental = Task(
            description=(
                f"Analyze this company's fundamental positioning for {ticker}:\n{sec_data}\n\n"
                "What is the state of their economic moat? What are the top 2 headwinds and tailwinds?"
            ),
            agent=self.fundamental,
            expected_output="An assessment of the economic moat and top 2 headwinds/tailwinds."
        )

        task_sentiment = Task(
            description=(
                f"Analyze these recent news summaries for {ticker}:\n{news_data}\n\n"
                "What is the short-term market momentum? Are there immediate catalysts? "
                "Output a Sentiment Score from 1-10."
            ),
            agent=self.sentiment,
            expected_output="A sentiment score from 1-10 and a summary of the market mood."
        )

        task_pm = Task(
            description=(
                f"Review the analysis from your Quant, Fundamental Analyst, and Sentiment Trader for {ticker}. "
                "Weigh their conflicting viewpoints. The Quant's math is important, but if the Fundamental "
                "moat is crumbling or the Sentiment is catastrophic, you must adjust the risk profile. "
                "Output a final investment thesis and a definitive rating."
            ),
            agent=self.portfolio_manager,
            expected_output="A final rating (Strong Buy to Strong Sell) and a 2-paragraph synthesis.",
            context=[task_quant, task_fundamental, task_sentiment] # Forces PM to wait for and read the others
        )

        crew = Crew(
            agents=[self.quant, self.fundamental, self.sentiment, self.portfolio_manager],
            tasks=[task_quant, task_fundamental, task_sentiment, task_pm],
            process=Process.sequential,
            verbose=True
        )

        return crew.kickoff()