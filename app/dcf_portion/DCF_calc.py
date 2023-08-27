import functools
from DCF_num import DiscountCashFlowRawData


class BuildDiscountCashFlowModel:
    '''
    '''

    def __init__(self,
                 ticker: str,
                 years_statement: int = 5,
                 risk_free_rate: float = 0.0261):
        '''
        class parameters:
        risk_free_rate: expected rate of return that have 0 risk, usually
            treasury bill
        '''
        self.risk_free_rate = risk_free_rate
        ticker = str(ticker).upper()
        self.calc_num = DiscountCashFlowRawData(
            ticker=ticker,
            years_statement=years_statement
        ).calculation_numbers()

    @functools.cached_property
    def growth(self) -> list[float]:
        '''
        parameters:
            none
        returns:
            growth rate projection for next 15 years
        '''
        rate_last_five = [
            self.calc_num['yoyGrowth']-(self.calc_num['yoyGrowth']-self.risk_free_rate)/10*n
            for n in range(1, 16)]

        return rate_last_five

    @property
    def rev_project(self) -> list[float]:
        '''
        return
            list of revenues projection based on growth rate
        '''
        rev_projects = []
        for idx, rate in enumerate(self.growth):
            if idx == 0:
                RevProject = self.calc_num['revenues'][0] * (1+rate)
                rev_projects.append(RevProject)

            RevProject = RevProject * (1+rate)
            rev_projects.append(RevProject)

        return rev_projects

    @property
    def ebit_project(self) -> list[float]:
        '''
        return
            list ebit projection based on revenues
        '''
        avg_ebit_perc = self.calc_num['operMargin']
        proj_ebit = [
            rev*avg_ebit_perc for rev in self.rev_project]
        return proj_ebit

    @property
    def tax_project(self) -> list[float]:
        '''
        return
            list taxes paid projected based on
        '''
        avg_tax_rate = self.calc_num['tax_rate']
        proj_tax = [
            ebit*avg_tax_rate
            for ebit in self.ebit_project
        ]
        return proj_tax

    @property
    def ebita(self) -> list[float]:
        '''
        ebit - tax
        return
            ebita numbers
        '''
        return [
            ebit-tax for ebit, tax
            in zip(self.ebit_project,
                   self.tax_project)
        ]

    @property
    def capex_project(self) -> list[float]:
        '''
        capex projection based on renveue
        return
            list of capex projections
        '''
        return [
            rev*self.calc_num['capex']
            for rev in self.rev_project
        ]

    @property
    def deprec_project(self) -> list[float]:
        '''
        '''
        return [
            rev*self.calc_num['depreciation']
            for rev in self.rev_project
        ]

    @property
    def unlevered_cashflow(self) -> list[float]:
        '''
        calculations:
            ebita + depreciation - capex - nwc
        '''
        return [
            ebita+depr-capex
            for ebita, depr, capex
            in zip(
                self.ebita,
                self.deprec_project,
                self.capex_project
            )
        ]

    def wacc(self,
             market_risk_prem: float = 0.0523,
             avg_debt_interest: float = 0.0232) -> float:
        '''
        parameter:
            risk_free_rate: risk free rate of return, google search
            market_risk: market risk premium, google search
            avg_notes_int: average interest rate of all interest of debt,
            google search

        return:
            A quick calculation of wacc
            wacc = (%debt * cost_of_debt * (1-tax_rate)) +
                    (% equity * cost of equity)
            cost_of_equity = risk free rate + (beta * market risk premium)
            cost_of_debt = average of debt notes
        '''
        beta = self.calc_num['beta']
        cost_of_equity = self.risk_free_rate + (beta * market_risk_prem)
        cost_of_debt = avg_debt_interest
        wacc = (
            (self.calc_num['debt_perc']*cost_of_debt*(1-self.calc_num['tax_rate'])) +
            (self.calc_num['equity_perc']*cost_of_equity)
        )
        return wacc

    def freeCashFlow(self,
                     _wacc: float = 0.12) -> list[float]:
        '''
        bring everything together, calcualte the present value of unlevered
        cashflow

        return:
            a list of present values of free cash flow for predicted
        '''
        override_wacc = float(_wacc)

        # present value of free cash flow
        # present value = future value / ((1+wacc)^year)
        unlevered_fcf = self.unlevered_cashflow
        fcf = []
        for idx, val in enumerate(unlevered_fcf):
            fcf.append(
                val/((1+override_wacc)**idx)
            )
        # terminal value(last one predicted) calculation
        terminal_value = (
            unlevered_fcf[-1]*(1+self.growth[-1]) /
            (override_wacc-self.growth[-1])
        )
        # present value of terminal value
        pv_of_tv = terminal_value/((1+override_wacc)**len(fcf))
        # enterprise value by adding all PVs together
        ev = sum(fcf) + pv_of_tv
        # enterprise value plus cash minus debt
        # equity value
        eq_v = ev + self.calc_num['cashMinusDebt']
        shares = self.calc_num['shares_outstanding']
        pred_price = eq_v/shares

        return {
            'discountFCF': [fcf],
            'pred_price': pred_price,
            'wacc_used': override_wacc
        }

    def wacc_fcf_curve(self):
        'calculate range of wacc vs predicted price'
        wacc_list = [_/100 for _ in range(5, 15)]
        price = [self.freeCashFlow(_wacc=(wacc))['pred_price']
                 for wacc in wacc_list]

        return {
            'wacc': wacc_list,
            'pred_price': price
        }

    def dcf_output(self):
        '''
        combine everything together, just before final
        calculation
        '''
        return {
            'growth_rates': [0] + self.growth,
            'revenue_proj': self.rev_project,
            'EBIT_proj': self.ebit_project,
            'Taxes_proj': self.tax_project,
            'Depreciation_proj': self.deprec_project,
            'Capex_proj': self.capex_project,
            'unLeveredFreeCash': self.unlevered_cashflow,
            'eps': self.calc_num['eps']

        }


if __name__ == '__main__':
    x = BuildDiscountCashFlowModel(
        ticker='nvda',
        years_statement=3,
        risk_free_rate=0.0381
    )
    #print(x.wacc_fcf_curve())
    print(x.dcf_output())