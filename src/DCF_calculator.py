import functools
from dataclasses import dataclass
from typing import Dict, Optional, List
import pandas as pd
from data_processor import FinancialDataProcessor

@dataclass
class DCFScenario:
    """
    custom scenarios
    """
    name: str
    revenue_growth_rates: List[float]
    fcf_margins: List[float]
    wacc: float
    terminal_growth_rate: float

@dataclass
class DCFModel:
    """
    Performs DCF calculations using data provided by FinancialDataLoader.
    """
    data_loader: FinancialDataProcessor
    risk_free_rate: float = 0.04
    
    @functools.cached_property
    def _raw_calculations(self) -> Dict:
        """
        calculate basic financial metrics
        changed to using fcf margin (OCF-capex)/revenue
        """
        revenues = self.data_loader.get_time_series('revenue')
        # operating cash flow
        ocf = self.data_loader.get_time_series('operating_cash_flow')
        capex = self.data_loader.get_time_series('capex')
        interest = self.data_loader.get_time_series('interest_expense')
        current_shares = self.data_loader.get_latest_value('shares_outstanding')

        # get balance sheet & income statement items
        metrics_to_fetch = [
            'cash', 'equity', 'long_term_debt', 'short_term_debt',
            'interest_expense', 'tax_expense', 'operating_income'
        ]

        balance_values = {}
        for metric in metrics_to_fetch:
            balance_values[metric] = self.data_loader.get_latest_value(metric)

        # calculate effective tax rate
        pretax_income_df = self.data_loader.get_time_series(
            'pretax_income',
            custom_aliases=['IncomeLossFromContinuingOperationsBeforeIncomeTaxes']
        )
        if not pretax_income_df.empty:
            pretax_income = pretax_income_df['value'].iloc[0]
        else:
            # Fallback: EBIT - Interest (Approximation)
            ebit = balance_values.get('operating_income', 0)
            pretax_income = ebit - balance_values.get('interest_expense', 0)

        tax_expense = balance_values.get('tax_expense', 0)

        if pretax_income > 0:
            effective_tax_rate = max(0.0, min(tax_expense/pretax_income, 0.30))
        else:
            effective_tax_rate = 0.21 # default value

        fcf_margins = []
        last_revenue = 0

        if not revenues.empty and not ocf.empty:
            last_revenue = revenues['value'].iloc[0]
            merged = pd.merge(
                revenues[['end_date', 'value']],
                ocf[['end_date', 'value']],
                on='end_date',
                suffixes=('_rev', '_ocf')
            )
            if not capex.empty:
                merged = pd.merge(
                    merged,
                    capex[['end_date', 'value']],
                    on='end_date',
                    how='left'
                ).rename(columns={'value': 'value_capex'})
            else:
                merged['value_capex'] = 0

            if not interest.empty:
                merged = pd.merge(
                    merged,
                    interest[['end_date', 'value']],
                    on='end_date',
                    how='left'
                ).rename(columns={'value': 'value_interest'})
            else:
                merged['value_interest'] = 0

            # FCF = OCF - CapEx + Interest * (1 - t)
            # merged['fcf'] = (merged['value_ocf'] - merged['value_capex'].abs() + (merged['value_interest'] * (1 - effective_tax_rate)))
            merged['fcf'] = merged['value_ocf'] + merged['value_capex']
            merged['fcf_margin'] = merged['fcf'] / merged['value_rev']
            fcf_margins = merged['fcf_margin'].tolist()

        revenues_list = revenues['value'].tolist() if not revenues.empty else []
        yoy_growth = []
        if len(revenues_list) >= 2:
            yoy_growth = [
                (revenues_list[i] - revenues_list[i+1])/revenues_list[i+1]
                for i in range(len(revenues_list)-1)
            ]
        avg_growth = sum(yoy_growth)/len(yoy_growth)
        avg_margin = sum(fcf_margins)/len(fcf_margins)

        return{
            'last_revenue': last_revenue,
            'avg_fcf_margin': avg_margin,
            'historical_growth': avg_growth,
            'cash': balance_values.get('cash', 0),
            'debt': balance_values.get('long_term_debt',0) + balance_values.get('short_term_debt',0),
            'shares_outstanding': current_shares,
        }

    def _calculate_decay(
        self, start_rate: float,
        end_rate: float, years: int = 5
    ) -> List[float]:
        """
        create a smooth decay curve from start to end rate
        """
        return [
            start_rate - ((start_rate - end_rate) * (i/years))
            for i in range(years)
        ]

    def generate_scenarios(self, use_wacc: float = None) -> List[DCFScenario]:
        """
        generates 3 scenarios based on historic performance
        """
        raw = self._raw_calculations
        base_growth = raw['historical_growth']
        base_margin = raw['avg_fcf_margin']

        # Sanity caps: If historical growth is crazy (>50%), temper the base for projections
        # to avoid unrealistic perpetual hyper-growth
        projection_start_growth = min(base_growth, 0.50)

        # Average Case
        avg_growth_rates = self._calculate_decay(projection_start_growth, self.risk_free_rate * 2)
        avg_scenario = DCFScenario(
            name='Average Case',
            revenue_growth_rates=avg_growth_rates,
            fcf_margins=[base_margin] * 5,
            wacc= use_wacc if use_wacc else 0.10, # <----- hardcore WACC
            terminal_growth_rate=0.03
        )
        # Optimistic Case
        opt_start = min(base_growth * 1.2, 0.60) # Cap at 60% start
        opt_growth_rates = self._calculate_decay(opt_start, self.risk_free_rate * 3)
        opt_margin = min(base_margin * 1.1, 0.60) # Cap margin at 60%
        opt_scenario = DCFScenario(
            name="Optimistic Case",
            revenue_growth_rates=opt_growth_rates,
            fcf_margins=[opt_margin] * 5,
            wacc=use_wacc if use_wacc else 0.10, # <----- hardcore WACC
            terminal_growth_rate=0.04
        )
        # Pessimistic Case
        pess_start = base_growth * 0.8
        pess_growth_rates = self._calculate_decay(pess_start, self.risk_free_rate)
        pess_margin = base_margin * 0.9
        pess_scenario = DCFScenario(
            name="Pessimistic Case",
            revenue_growth_rates=pess_growth_rates,
            fcf_margins=[pess_margin] * 5,
            wacc=use_wacc if use_wacc else 0.11, # Higher risk premium
            terminal_growth_rate=0.02
        )

        return [avg_scenario, opt_scenario, pess_scenario]

    def _project_financials(self, scenario: DCFScenario) -> Dict:
        """
        Project future financials
        """
        raw = self._raw_calculations
        current_revenue = raw['last_revenue']
        
        projected_revenues = []
        projected_fcf = []
        
        for i in range(5):
            growth_rate = scenario.revenue_growth_rates[i]
            if len(scenario.fcf_margins) > i:
                margin = scenario.fcf_margins[i]
            else:
                margin = scenario.fcf_margins[-1]
            
            current_revenue = current_revenue * (1+growth_rate)
            fcf = current_revenue * margin
            
            projected_revenues.append(current_revenue)
            projected_fcf.append(fcf)

        return {
            'projected_revenues': projected_revenues,
            'projected_fcf': projected_fcf
        }

    def calculate_dcf(self, scenario: DCFScenario) -> Dict:
        """Calculate DCF valuation"""
        raw = self._raw_calculations
        projections = self._project_financials(scenario)
        projected_fcf = projections['projected_fcf']
        
        wacc = scenario.wacc
        discounted_fcf = [fcf / ((1 + wacc) ** (i + 1)) for i, fcf in enumerate(projected_fcf)]
        # Terminal Value
        last_fcf = projected_fcf[-1]
        terminal_value = (last_fcf * (1 + scenario.terminal_growth_rate)) / (wacc - scenario.terminal_growth_rate)
        pv_terminal_value = terminal_value / ((1 + wacc) ** len(projected_fcf))

        enterprise_value = sum(discounted_fcf) + pv_terminal_value
        equity_value = enterprise_value + raw['cash'] - raw['debt']
        
        shares = raw['shares_outstanding']
        price = equity_value / shares if shares > 0 else 0

        return {
            'scenario_name': scenario.name,
            'implied_share_price': price,
            'equity_value': equity_value,
            'growth_rate_start': scenario.revenue_growth_rates[0],
            'growth_rate_end': scenario.revenue_growth_rates[-1],
            'avg_margin': sum(scenario.fcf_margins)/len(scenario.fcf_margins)
        }

    def close(self):
        self.data_loader.close()

if __name__ == '__main__':
    # cik 104169 walmart
    # 1730168 broadcom
    #  2488 amd
    #  320193 apple
    # 1045810 nvidia
    # 1652044  google
    # 1018724 amazon
    # 789019 msft
    loader = FinancialDataProcessor(cik=2488, years_statement=3)
    model = DCFModel(data_loader=loader)
    print(model._raw_calculations)
    scenarios = model.generate_scenarios(use_wacc=0.096)
    print(f"{'SCENARIO':<20} | {'PRICE':<10} | {'START GROWTH':<15}")
    print("-" * 50)
    for scen in scenarios:
        res = model.calculate_dcf(scen)
        print(f"{res['scenario_name']:<20} | ${res['implied_share_price']:<9.2f} | {res['growth_rate_start']:.1%}")
        
        # print(model._project_financials(scenario=scen))