from src.investment_committee import InvestmentCommittee
from crewai import Task, Crew

def run_quick_test():
    print("Initializing connection test...")
    
    # 1. Instantiate the committee with your cloud provider
    # (Swap the model name to whatever is actually loaded on your cloud server)
    committee = InvestmentCommittee(provider="ollama_cloud", model_name="gemma4:31b")
    
    # 2. Create a micro-task that requires almost zero computation
    ping_task = Task(
        description="Respond with: tell me about your knowledge cut-off date",
        expected_output="A single success sentence.",
        agent=committee.quant # Just grabbing the Quant agent for the test
    )
    
    # 3. Spin up a mini-crew with just the one agent and task
    test_crew = Crew(
        agents=[committee.quant],
        tasks=[ping_task],
        verbose=True
    )
    
    print("\n--- Sending Ping to LLM ---")
    result = test_crew.kickoff()
    
    print("\n=== FINAL RESULT ===")
    print(result)
    
    

def test_llm_provider(provider_name: str, model_name: str):
    """Tests the _get_llm router by spinning up a single agent."""
    print(f"\n{'='*60}")
    print(f"🔄 TESTING ROUTE: Provider='{provider_name}' | Model='{model_name}'")
    print(f"{'='*60}")

    try:
        # 1. This triggers your _get_llm() method
        committee = InvestmentCommittee(provider=provider_name, model_name=model_name)
        
        # 2. Create a micro-task that requires almost zero computation
        ping_task = Task(
            description="tell me about your knowledge cut off",
            expected_output="A single success sentence.",
            agent=committee.quant # Just grabbing one agent for the test
        )
        
        # 3. Spin up a mini-crew 
        test_crew = Crew(
            agents=[committee.quant],
            tasks=[ping_task],
            verbose=False # Keep it clean so we just see the result
        )
        
        print(f"📡 Sending ping to {provider_name}...")
        result = test_crew.kickoff()
        
        print("\n✅ SUCCESS! Received Response:")
        print(f"   {result.raw}")
        
    except Exception as e:
        print("\n❌ CONNECTION FAILED!")
        print(f"   Error: {str(e)}")

if __name__ == "__main__":
    # run_quick_test()
    test_llm_provider("ollama", "qwen2.5:3b")