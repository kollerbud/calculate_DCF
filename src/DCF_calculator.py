from dataclasses import dataclass
from typing import Dict, List
from data_processor import FinancialDataProcessor

@dataclass
class DCFScenario:
    """
    custom scenarios
    """
    name: str
    wacc: float
    growth_start: float
    margin_start: float
    margin_end: float
    terminal_growth: float = 0.03

class DCFModel:
    """
    Performs DCF calculations using data provided by FinancialDataLoader.
    """
    def __init__(self, data_loader: FinancialDataProcessor):
        self.data = data_loader
        self.raw = self._extract_financials()


    def _extract_financials(self) -> Dict:

        rev = self.data.get_time_series('revenue').head(4)
        ocf = self.data.get_time_series('operating_cash_flow').head(4)
        capex = self.data.get_time_series('capex').head(4)
        # latest balance sheet numbers
        cash = self.data.get_latest_value('cash')
        debt = self.data.get_latest_value('long_term_debt')
        shares = self.data.get_latest_value('shares_outstanding')

        if rev.empty or ocf.empty or len(rev) <2:
            return {'valid': False}

        # cyclical smoothing
        rev_vals = rev['value'].tolist()
        cagr = (rev_vals[0] / rev_vals[-1]) ** (1 / (len(rev_vals) - 1)) - 1 if rev_vals[-1] > 0 else 0
        # average fcf margin
        # fcf = ocf - abs(capex)
        historical_margins = []
        for i in range(min(len(rev), len(ocf), len(capex))):
            fcf = ocf['value'].iloc[i] - abs(capex['value'].iloc[i])
            historical_margins.append(fcf/rev['value'].iloc[i])

        avg_margin = sum(historical_margins)/len(historical_margins)
        latest_margin = historical_margins[0]

        # semiconductor nuance--fabless vs IDM
        # if capex is consistently >10% of revenue, they are likely fab-based company
        capex_to_rev = abs(capex['value'].iloc[0])/rev['value'].iloc[0]
        business_model = "IDM/Foundry" if capex_to_rev > 0.10 else "Fabless"

        # cap unrealistic extremes
        cagr = min(max(cagr, -0.10), 0.60)

        return {
            'valid': True,
            'last_revenue': rev_vals[0],
            'cagr': cagr,
            'avg_margin': avg_margin,
            'latest_margin': latest_margin,
            'cash': cash,
            'debt': debt,
            'shares': shares,
            'business_model': business_model,
            'capex_to_rev': capex_to_rev
        }
    def generate_scenarios(self) -> List[DCFScenario]:
        """
        Creates semi-specific scenarios using Wall Street 'sanity checks'
        to bridge the gap between historical GAAP data and forward-looking market pricing.
        """
        hist_growth = self.raw['cagr']
        hist_margin = self.raw['avg_margin']
        biz_model = self.raw['business_model']

        # Wall Street ignores non-cash acquisition amortizations. A top-tier
        # Fabless company operates at a bare minimum 20% FCF margin.
        if biz_model == "Fabless":
            start_margin = max(0.20, hist_margin)
            normalized_terminal_margin = max(0.25, start_margin) # Fabless mature at high margins
            base_wacc = 0.105 # Higher beta / higher risk

        # IDMs are highly cyclical. If historical margin is crushed due to heavy CapEx,
        # we floor it slightly, but aggressively normalize it in the terminal phase.
        elif biz_model == "IDM/Foundry":
            start_margin = max(0.05, hist_margin) # Prevent negative starting cash flows
            normalized_terminal_margin = 0.22 # Assume fabs are built and paying off by Year 10
            base_wacc = 0.085 # Lower beta / stable dividend payers

        # Semiconductors are in a secular mega-trend. Even if a company is in a
        # short-term cyclical recession (negative hist_growth), the market expects
        # long-term positive growth. We floor the starting growth at the GDP growth rate (3%).
        base_growth = max(0.03, hist_growth)

        return [
            DCFScenario("Average", base_wacc, base_growth, start_margin, normalized_terminal_margin, 0.03),
            # Optimistic: Market prices in an "AI Supercycle" (Higher growth, expanding margins)
            DCFScenario("Optimistic", base_wacc - 0.01, base_growth * 1.3, start_margin * 1.2, normalized_terminal_margin * 1.15, 0.04),
            # Pessimistic: Cycle stays lower for longer
            DCFScenario("Pessimistic", base_wacc + 0.01, base_growth * 0.5, start_margin * 0.8, normalized_terminal_margin * 0.8, 0.02)
        ]

    def calculate_dcf(self, scenario: DCFScenario, years: int = 10) -> Dict:
        rev = self.raw['last_revenue']
        discounted_fcf = 0

        last_fcf = 0
        for year in range(1, years + 1):
            # Fade growth to terminal rate
            growth = scenario.growth_start - ((scenario.growth_start - scenario.terminal_growth) * (year / years))
            # Fade margin to normalized terminal margin (fixes Intel)
            margin = scenario.margin_start - ((scenario.margin_start - scenario.margin_end) * (year / years))

            rev *= (1 + growth)
            fcf = rev * margin
            discounted_fcf += fcf / ((1 + scenario.wacc) ** year)
            last_fcf = fcf

        # Terminal Value (Gordon Growth)
        tv = (last_fcf * (1 + scenario.terminal_growth)) / (scenario.wacc - scenario.terminal_growth)
        pv_tv = tv / ((1 + scenario.wacc) ** years)

        enterprise_value = discounted_fcf + pv_tv
        equity_value = enterprise_value + self.raw['cash'] - self.raw['debt']
        price = equity_value / self.raw['shares'] if self.raw['shares'] > 0 else 0

        return {
            'name': scenario.name,
            'price': price,
            'start_growth': scenario.growth_start,
            'end_margin': scenario.margin_end
        }

    def close(self):
        self.data.close()

def evaluate_semi_industry():
    companies = {
        'Broadcom': 1730168, 'AMD': 2488, 'Nvidia': 1045810,
        'Intel': 50863, 'Texas Inst': 97476, 'Qualcomm': 804328, 'Micron': 723125,
    }

    for name, cik in companies.items():
        print(f"\n{name.upper()} (CIK: {cik})")
        print("-" * 65)

        try:
            loader = FinancialDataProcessor(cik=cik, years_statement=4, filing_type='10-K')
            model = DCFModel(loader)

            if not model.raw['valid']:
                print("  [!] Skipping: Missing fundamental data.")
                continue

            print(f"  Profile: {model.raw['business_model']} (CapEx/Rev: {model.raw['capex_to_rev']:.1%})")
            print(f"  Hist. CAGR: {model.raw['cagr']:.1%} | 4-Yr Avg Margin: {model.raw['avg_margin']:.1%}")

            scenarios = model.generate_scenarios()
            print(f"  {'SCENARIO':<15} | {'PRICE':>10} | {'GROWTH->TERM':>15} | {'END MARGIN':>10}")
            for s in scenarios:
                res = model.calculate_dcf(s)
                print(f"  {res['name']:<15} | ${res['price']:>9.2f} | {res['start_growth']:.1%} -> {s.terminal_growth:.1%} | {res['end_margin']:>9.1%}")

        except Exception as e:
            print(f"  [!] Error: {e}")
        finally:
            if 'model' in locals(): model.close()


if __name__ == '__main__':
    evaluate_semi_industry()