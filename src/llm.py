import sys
import json
import re
from pathlib import Path
from llama_index.core import Settings
from llama_index.llms.ollama import Ollama

from DCF_calculator import DCFModel
from data_processor import FinancialDataProcessor

# Ensure the src directory is in the Python path
sys.path.append(str(Path(__file__).parent))

# --- Configuration ---
TARGET_CIK = "1045810" # Example CIK from DCF_calculator.py
RISK_FREE_RATE = 0.04  # Example assumption
BETA = 1.0            # Example assumption
MARKET_RISK_PREMIUM = 0.0523 # Example from DCF_calculator.py
WACC_OVERRIDE = 0.08 # Set to None to have both script and LLM calculate it (or try to)
PROJECTION_YEARS = 15 # Match the projection period if possible
GROWTH_DECAY_RATE = 0.1 # Match the 1 - n/10 logic approximately for the LLM prompt
REINVESTMENT_RATE = 0.20 # Match the (EBIT - Tax) * 0.8 assumption for FCF
EFFECTIVE_TAX_RATE = 0.21 # Standard tax rate assumption used

OLLAMA_MODEL = "deepseek-r1:14b"
OLLAMA_REQUEST_TIMEOUT = 60 # Increased timeout for complex calculation request

# --- 1. Get Financial Inputs using Python ---
print(f"Fetching financial data for CIK: {TARGET_CIK}...")
data_loader = None
script_inputs = {}
script_results = {}

try:
    data_loader = FinancialDataProcessor(cik=TARGET_CIK, years_statement=5, filing_type='10-K')
    # Fetch the raw data points needed for DCF calculation, similar to DCF_calculator._raw_calculations
    # This mirrors the inputs the DCFModel class would use.
    raw_calc_inputs = DCFModel(data_loader=data_loader, risk_free_rate=RISK_FREE_RATE, beta=BETA)._raw_calculations #
    script_inputs = { # Store the key inputs used
        "CIK": TARGET_CIK,
        "Historical Revenues (Most Recent First)": raw_calc_inputs.get('revenues', []),
        "Historical Operating Margins (Avg)": (sum(raw_calc_inputs['operating_margins']) / len(raw_calc_inputs['operating_margins'])) if raw_calc_inputs.get('operating_margins') else 0.15, # Use average or fallback
        "Initial YoY Growth (Avg)": raw_calc_inputs.get('yoy_growth', RISK_FREE_RATE), # Use calculated or fallback
        "Cash": raw_calc_inputs.get('cash', 0),
        "Short Term Debt": raw_calc_inputs.get('short_term_debt', 0),
        "Long Term Debt": raw_calc_inputs.get('long_term_debt', 0),
        "Total Debt": raw_calc_inputs.get('short_term_debt', 0) + raw_calc_inputs.get('long_term_debt', 0),
        "Interest Expense": raw_calc_inputs.get('interest_expense', 0),
        "Equity": raw_calc_inputs.get('equity', 0),
        "Effective Tax Rate (Used)": raw_calc_inputs.get('effective_tax_rate', EFFECTIVE_TAX_RATE),
        "Shares Outstanding": raw_calc_inputs.get('shares_outstanding', 0),
        "Risk-Free Rate": RISK_FREE_RATE,
        "Beta": BETA,
        "Market Risk Premium": MARKET_RISK_PREMIUM,
        "WACC Override": WACC_OVERRIDE,
        "Projection Years": PROJECTION_YEARS,
        "Growth Decay Rate Per Year": GROWTH_DECAY_RATE,
        "Terminal Growth Rate (Risk-Free Rate)": RISK_FREE_RATE,
        "Reinvestment Rate (as % of NOPAT)": REINVESTMENT_RATE,
    }

    print("Financial data fetched.")
    # --- 2. Calculate DCF using Python Script ---
    print("Calculating DCF using DCF_calculator.py...")
    dcf_model_script = DCFModel(data_loader=data_loader, risk_free_rate=RISK_FREE_RATE, beta=BETA) #
    script_results = dcf_model_script.calculate_dcf(wacc=WACC_OVERRIDE) #
    # Add WACC used to the results if it was calculated
    if WACC_OVERRIDE is None:
         script_results['wacc_calculated'] = script_results.get('wacc')
    else:
         script_results['wacc_provided'] = WACC_OVERRIDE

    print("Python DCF calculation complete.")

except ImportError as e:
     print(f"Error importing required modules: {e}")
     exit(1)
except Exception as e:
    print(f"Error during data fetching or Python DCF calculation: {e}")
    # Decide if script should exit or try to proceed without Python results
    exit(1) # Exit if we can't get data or run the baseline calculation
finally:
    if data_loader:
        data_loader.close()
        print("Data loader closed.")

# --- 3. Prepare Prompt for LLM ---
print("Preparing prompt for LLM DCF calculation...")

# Create a detailed prompt explaining the task and providing the data
prompt = f"""
You are a financial analyst AI. Perform a Discounted Cash Flow (DCF) valuation for the company associated with CIK {script_inputs['CIK']} based *only* on the following data and assumptions:

**Financial Data & Assumptions:**
* Historical Revenues (Most Recent First): {script_inputs['Historical Revenues (Most Recent First)']}
* Average Historical Operating Margin: {script_inputs['Historical Operating Margins (Avg)']}
* Average Initial YoY Growth Rate: {script_inputs['Initial YoY Growth (Avg)']}
* Cash: {script_inputs['Cash']:,.2f}
* Total Debt (Short + Long Term): {script_inputs['Total Debt']:,.2f}
* Interest Expense (Annual): {script_inputs['Interest Expense']:,.2f}
* Total Equity: {script_inputs['Equity']:,.2f}
* Shares Outstanding: {script_inputs['Shares Outstanding']:,.0f}
* Risk-Free Rate: {script_inputs['Risk-Free Rate']:.4f}
* Beta: {script_inputs['Beta']:.2f}
* Market Risk Premium: {script_inputs['Market Risk Premium']:.4f}
* Effective Tax Rate: {script_inputs['Effective Tax Rate (Used)']:.4f}
* Projection Years: {script_inputs['Projection Years']}
* Growth Decay Rate: Assume the initial growth rate decreases linearly towards the terminal growth rate over the projection period. (e.g. initial_growth * (1 - n/{script_inputs['Projection Years']}))
* Terminal Growth Rate: {script_inputs['Terminal Growth Rate (Risk-Free Rate)']:.4f} (equal to Risk-Free Rate)
* Reinvestment Rate: Assume {script_inputs['Reinvestment Rate (as % of NOPAT)']:.0%} of NOPAT (Net Operating Profit After Tax) is reinvested each year. Free Cash Flow (FCF) = NOPAT * (1 - Reinvestment Rate).



**Output Requirements:**
Please provide the results in a JSON format ONLY, enclosed in ```json ... ``` markdown block. The JSON object MUST contain these keys with numerical float values:
- "calculated_wacc": (float, the WACC you calculated or the override value used)
- "projected_fcf_pv": (list of floats, ***IMPORTANT: This MUST be a valid JSON list containing ONLY the calculated numerical present values for each of the {script_inputs['Projection Years']} projection years, separated by commas. Do NOT include '...' or formulas. Calculate each value first.*** Example: [100.0, 95.5, 90.1, ...])
- "terminal_value_pv": (float, present value of the terminal value)
- "enterprise_value": (float)
- "equity_value": (float)
- "price_per_share": (float)

Ensure the final output contains ONLY the JSON object within the markdown block. Do not include any other text or explanation outside the JSON block in the final output.
"""

# --- 4. Configure LLM ---
print("Configuring LlamaIndex Settings for direct LLM call...")
try:
    # Using Ollama directly via LlamaIndex settings
    Settings.llm = Ollama(model=OLLAMA_MODEL, request_timeout=OLLAMA_REQUEST_TIMEOUT, temperature=0.0) # Set temperature low for calculation
    print(f"LLM set to: {OLLAMA_MODEL}")
except Exception as e:
    print(f"Error configuring Ollama model: {e}")
    print(f"Please ensure Ollama is running and model '{OLLAMA_MODEL}' is available.")
    exit(1)

# --- 5. Query LLM for Calculation ---
print(f"Sending prompt to {OLLAMA_MODEL} for DCF calculation (this may take a while)...")
llm_response_raw = ""
llm_results = {}
try:
    # Use .complete() for a single completion task
    response = Settings.llm.complete(prompt)
    llm_response_raw = str(response)
    print("LLM response received.")

    # --- 6. Extract LLM Results ---
    print("Attempting to parse LLM response...")
    # Try to extract JSON - improved regex to handle potential leading/trailing whitespace
    json_match = re.search(r"```json\s*(\{.*?\})\s*```", llm_response_raw, re.DOTALL)
    if not json_match:
         # Fallback: try finding JSON without backticks, more broadly
        json_match = re.search(r"\{\s*\"calculated_wacc\":.*?\s*\}", llm_response_raw, re.DOTALL) # Look for start of expected JSON

    if json_match:
        json_string = json_match.group(1)
        try:
            llm_results = json.loads(json_string)
            # **NEW:** Add validation for projected_fcf_pv structure
            if 'projected_fcf_pv' in llm_results:
                if not isinstance(llm_results['projected_fcf_pv'], list) or \
                   not all(isinstance(item, (int, float)) for item in llm_results['projected_fcf_pv']):
                    print("Warning: LLM 'projected_fcf_pv' is not a list of numbers. Parsing might be inaccurate.")
                    # Optionally attempt to clean it, but likely indicates LLM didn't follow instructions
                elif len(llm_results['projected_fcf_pv']) != PROJECTION_YEARS:
                     print(f"Warning: LLM 'projected_fcf_pv' list has {len(llm_results['projected_fcf_pv'])} items, expected {PROJECTION_YEARS}.")


            print("LLM JSON response parsed successfully.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from LLM response: {e}")
            print("LLM Raw Response snippet:\n", llm_response_raw[:1000]) # Print more context on error
    else:
        print("Could not find JSON block in the LLM response.")
        print("LLM Raw Response snippet:\n", llm_response_raw[:1000])

except Exception as e:
     print(f"Error during LLM query execution: {e}")
     print("LLM Raw Response (if any):\n", llm_response_raw)


# --- 7. Compare Results --- (Comparison logic remains the same)
print("\n\n--- DCF Calculation Comparison ---")
print(f"CIK: {TARGET_CIK}")

# Function to safely format values
def format_value(value, is_currency=True, is_percent=False):
    if value is None or not isinstance(value, (int, float)):
        return "N/A"
    try:
        if is_percent:
            return f"{value:.2%}"
        if is_currency:
            return f"${value:,.2f}"
        else: # For shares or other non-currency numbers
             return f"{value:,.0f}"
    except (ValueError, TypeError):
        return "N/A"

comparison_keys = [
    ('WACC Used', 'wacc', 'calculated_wacc', False, True), # Attr name, LLM key, is_currency, is_percent
    ('Enterprise Value', 'enterprise_value', 'enterprise_value', True, False),
    ('Equity Value', 'equity_value', 'equity_value', True, False),
    ('Price Per Share', 'price_per_share', 'price_per_share', True, False),
    # Correctly get PV of TV from script results if available
    ('PV of Terminal Value', 'terminal_value_pv', 'terminal_value_pv', True, False),
    ('Shares Outstanding', 'shares_outstanding', None, False, False), # Only from script
]

print("\nMetric                 | Python Script        | LLM Calculation")
print("-----------------------|----------------------|--------------------")

for name, script_key, llm_key, is_curr, is_perc in comparison_keys:
     # Special handling for PV of TV which isn't directly in script_results dict name
     if name == 'PV of Terminal Value':
          # Calculate PV(TV) from script results: TV / (1+WACC)^N
         script_tv = script_results.get('terminal_value')
         script_wacc = script_results.get('wacc', WACC_OVERRIDE) # Use calculated or overridden WACC
         script_fcf = script_results.get('projected_fcf')
         script_val = None # Default to None
         if script_tv is not None and script_wacc is not None and script_fcf is not None:
             n = len(script_fcf)
             if n > 0 and script_wacc is not None and script_wacc != -1: # Avoid division by zero or invalid WACC
                 script_val = script_tv / ((1 + script_wacc) ** n)
     elif name == 'WACC Used':
        script_val = script_results.get('wacc', WACC_OVERRIDE) # Always show the WACC value used
     else:
        script_val = script_results.get(script_key)


     llm_val = llm_results.get(llm_key) if llm_key else None

     # Special handling for shares outstanding (input, not calculated by LLM)
     if name == 'Shares Outstanding':
         llm_val_str = "(Not Calculated by LLM)"
         script_val_str = format_value(script_val, is_currency=False, is_percent=False)
     else:
        script_val_str = format_value(script_val, is_currency=is_curr, is_percent=is_perc)
        llm_val_str = format_value(llm_val, is_currency=is_curr, is_percent=is_perc)


     print(f"{name:<23}| {script_val_str:<20} | {llm_val_str:<18}")

# Compare PV of FCFs
# Need to calculate PV(FCF) for the script results
script_fcf_pv = []
script_wacc_used = script_results.get('wacc', WACC_OVERRIDE)
if script_wacc_used is not None and script_wacc_used != -1: # Check for valid WACC
    raw_fcf = script_results.get('projected_fcf', [])
    if raw_fcf: # Check if list exists
      for i, fcf_val in enumerate(raw_fcf):
          if isinstance(fcf_val, (int, float)): # Check if value is numeric
              pv = fcf_val / ((1 + script_wacc_used)**(i+1))
              script_fcf_pv.append(pv)
          else:
              script_fcf_pv.append(None) # Append None if non-numeric
    else:
         script_fcf_pv = ["N/A"] # Handle empty raw FCF list
else:
    script_fcf_pv = ["N/A"] # Handle invalid WACC


llm_fcf_pv = llm_results.get('projected_fcf_pv', ["N/A"])
if not isinstance(llm_fcf_pv, list): # Ensure LLM output is treated as a list
     llm_fcf_pv = ["N/A"]


print("\nPV of Projected FCFs (First 5 Years):")
print("Year | Python Script | LLM Calculation")
print("-----|---------------|-----------------")
# Determine safe range for comparison, handle potential N/A lists
max_len = 0
if isinstance(script_fcf_pv, list) and script_fcf_pv != ["N/A"]:
    max_len = max(max_len, len(script_fcf_pv))
if isinstance(llm_fcf_pv, list) and llm_fcf_pv != ["N/A"]:
     max_len = max(max_len, len(llm_fcf_pv))

for i in range(min(5, max_len)):
     py_fcf = script_fcf_pv[i] if isinstance(script_fcf_pv, list) and i < len(script_fcf_pv) else "N/A"
     llm_fcf = llm_fcf_pv[i] if isinstance(llm_fcf_pv, list) and i < len(llm_fcf_pv) else "N/A"
     print(f"{i+1:<5}| {format_value(py_fcf):<13} | {format_value(llm_fcf):<17}")


print("\n--- Raw LLM Response ---")
print(llm_response_raw) # Print full raw response for debugging
print("------------------------")

print("\nScript finished.")




"""
**Calculation Steps:**

1.  **Project Revenue:** Start with the most recent historical revenue ({script_inputs['Historical Revenues (Most Recent First)'][0] if script_inputs['Historical Revenues (Most Recent First)'] else 'N/A'}). Project revenue for {script_inputs['Projection Years']} years. Calculate the growth rate for each year, starting with {script_inputs['Initial YoY Growth (Avg)']} and decaying linearly towards the terminal growth rate ({script_inputs['Terminal Growth Rate (Risk-Free Rate)']}). Ensure growth doesn't go below the terminal rate.
2.  **Project EBIT:** Calculate projected Earnings Before Interest and Taxes (EBIT) using the projected revenue and the Average Historical Operating Margin ({script_inputs['Historical Operating Margins (Avg)']}).
3.  **Calculate NOPAT:** Calculate Net Operating Profit After Tax (NOPAT) for each projected year: NOPAT = EBIT * (1 - Effective Tax Rate).
4.  **Calculate Projected FCF:** Calculate Free Cash Flow (FCF) for each projected year: FCF = NOPAT * (1 - Reinvestment Rate).
5.  **Calculate WACC:**
    * Calculate Cost of Equity using CAPM: Cost of Equity = Risk-Free Rate + Beta * Market Risk Premium.
    * Calculate Cost of Debt: Cost of Debt = Interest Expense / Total Debt. If Total Debt is zero, Cost of Debt is effectively zero for WACC calculation.
    * Calculate Equity Weight: Equity / (Total Debt + Equity).
    * Calculate Debt Weight: Total Debt / (Total Debt + Equity).
    * Calculate WACC = (Equity Weight * Cost of Equity) + (Debt Weight * Cost of Debt * (1 - Effective Tax Rate)).
    {'* **IMPORTANT: Use the provided WACC override:** WACC = ' + str(script_inputs['WACC Override']) if script_inputs['WACC Override'] is not None else '* Calculate WACC as described above.'}
6.  **Calculate Present Value (PV) of FCF:** Discount each year's projected FCF back to the present using the calculated (or provided) WACC. PV(FCF_n) = FCF_n / (1 + WACC)^n.
7.  **Calculate Terminal Value (TV):** Calculate the terminal value using the Gordon Growth Model based on the FCF of the *last* projection year (Year {script_inputs['Projection Years']}): TV = (FCF_Year{script_inputs['Projection Years']} * (1 + Terminal Growth Rate)) / (WACC - Terminal Growth Rate).
8.  **Calculate Present Value (PV) of TV:** Discount the Terminal Value back to the present: PV(TV) = TV / (1 + WACC)^{script_inputs['Projection Years']}.
9.  **Calculate Enterprise Value:** Enterprise Value = Sum of PV of FCFs + PV of TV.
10. **Calculate Equity Value:** Equity Value = Enterprise Value + Cash - Total Debt.
11. **Calculate Price Per Share:** Price Per Share = Equity Value / Shares Outstanding.
"""