from DCF_prelim_calc import DCFDataInput
import functools


class BuildDCF(DCFDataInput):
    '''
    inherite input class for cleaner structure, but also means
    coupling..
    subclass variable: risk free rate
    put decision making points on the dcf method
    '''
    def __init__(self, ticker, risk_free_rate):
        self.risk_free_rate = risk_free_rate
        super().__init__(ticker=ticker)

    @functools.cached_property
    def growth(self) -> list:
        '''
        parameters:
            none
        returns:
            growth rate projection for next 11 years
        '''
        rate_first_five = [self.yoy_grwoth_] * 5
        rate_last_five = [
            self.yoy_grwoth_-(self.yoy_grwoth_-self.risk_free_rate)/6*n
            for n in range(1, 7)
            ]
        return rate_first_five + rate_last_five

    @property
    def rev_project(self) -> list:
        '''
        return
            list of revenues projection based on growth rate
        '''
        rev_projects = []
        for idx, rate in enumerate(self.growth):
            if idx == 0:
                RevProject = self.revenues[0] * (1+rate)
                rev_projects.append(RevProject)

            RevProject = RevProject * (1+rate)
            rev_projects.append(RevProject)

        return rev_projects

    @property
    def ebit_project(self) -> list:
        '''
        return
            list ebit projection based on revenues
        '''
        avg_ebit_perc = self.operating_margin
        proj_ebit = [
            rev*avg_ebit_perc for rev in self.rev_project]
        return proj_ebit

    @property
    def tax_project(self) -> list:
        '''
        return
            list taxes paid projected based on
        '''
        avg_tax_rate = self.tax_rate
        proj_tax = [
            ebit*avg_tax_rate
            for ebit in self.ebit_project
        ]
        return proj_tax

    @property
    def ebita(self) -> list:
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
    def capex_project(self) -> list:
        '''
        capex projection based on renveue
        return
            list of capex projections
        '''
        return [
            rev*self.capex
            for rev in self.rev_project
        ]

    @property
    def deprec_project(self) -> list:
        '''
        '''
        return [
            rev*self.depreciation
            for rev in self.rev_project
        ]

    @property
    def changeNWC(self) -> list:
        '''
        change in net working capital based on percentage
        of revenue
        return
            nwc based on revenue
        '''
        return [
            rev*self.nwc
            for rev in self.rev_project
        ]

    @property
    def unlevered_cashflow(self) -> list:
        '''
        calculations:
            ebita + depreciation - capex - nwc
        '''
        return [
            ebita+depr-capex-nwc
            for ebita, depr, capex, nwc
            in zip(
                self.ebita,
                self.deprec_project,
                self.capex_project,
                self.changeNWC
            )
        ]

    def wacc(self, market_risk: float = 0.058,
             avg_notes_int: float = 0.03
             ) -> list:
        '''
        parameter:
            market_risk: market risk premium, quick google search
            would get you a 5.8%
            avg_notes_int: average interest rate of all interest of debt,
            this one requires digging into balance sheet of the company's
            report
        return:
            A quick calculation of wacc
            wacc = (%debt * cost_of_debt * (1-tax_rate)) +
                    (% equity * cost of equity)
            cost_of_equity = risk free rate + (beta * market risk premium)
            cost_of_debt = average of debt notes
        '''
        beta = self.ticker_info['beta']
        cost_of_equity = self.risk_free_rate + (beta * market_risk)
        cost_of_debt = avg_notes_int
        wacc = (
            (self.wacc_cal['debt_perc']*cost_of_debt*(1+self.tax_rate)) +
            (self.wacc_cal['equity_perc']*cost_of_equity)
        )
        return wacc

    def freeCashFlow(self,
                     market_risk: float = 0.058,
                     avg_debt_int_rate: float = 0.03,
                     override_wacc=None) -> list:
        '''
        bring everything together, calcualte the present value of unlevered
        cashflow
        paramters:
            market_risk: market risk premium, same as the one used for wacc
            avg_debt_int_rate: average interest rate of debts, same as for wacc
            override_wacc: override wacc calculation and use a manual input
            instead
        return:
            a list of present values of free cash flow for predicted 
        '''
        if override_wacc is None:
            wacc_rate = self.wacc(
                market_risk=market_risk, avg_notes_int=avg_debt_int_rate
                )
        else:
            override_wacc = float(override_wacc)
            wacc_rate = override_wacc

        # present value of free cash flow
        # present value = future value / ((1+wacc)^year)
        unlevered_fcf = self.unlevered_cashflow
        fcf = []
        for idx, val in enumerate(unlevered_fcf):
            fcf.append(
                val/((1+wacc_rate)**idx)
            )
        # terminal value(last one predicted) calculation
        terminal_value = (
            unlevered_fcf[-1]*(1+self.growth[-1]) /
            (wacc_rate-self.growth[-1])
        )
        # present value of terminal value
        pv_of_tv = terminal_value/((1+wacc_rate)**len(fcf))
        # enterprise value by adding all PVs together
        ev = sum(fcf) + pv_of_tv
        # enterprise value plus cash minus debt
        # equity value
        eq_v = ev + self.cash_minus_debt
        shares = self.ticker_info['shares_outstanding']
        pred_price = eq_v/shares

        return {
            'fcf_pv': fcf,
            'price_predicted': pred_price,
            'wacc used': wacc_rate
        }


if __name__ == '__main__':
    t = 'abt'
    x = BuildDCF(t, risk_free_rate=0.0343)
    print(x.freeCashFlow(market_risk=0.0523, avg_debt_int_rate=0.027))
'''
'''