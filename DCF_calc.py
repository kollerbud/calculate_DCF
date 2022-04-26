import functools
from DCF_Input import DCF_DATA

class BuildDCF:

    def __init__(self,
                 current_rev: float,
                 growth_rate: float,
                 ebit_margin: float,
                 indus_growth_avg: float,
                 tax_rate: float,
                 sales_cap_ratio: float,
                 wacc: float,
                 debt: float,
                 cash: float,
                 shares: float,
                growth_taper: float = 0.5) -> None:

        self.cur_rev = current_rev
        self.growth_rate = growth_rate
        self.growth_taper = growth_taper
        self.ebit_margin = ebit_margin
        self.indus_growth_avg = indus_growth_avg
        self.tax_rate = tax_rate
        self.sales_cap = sales_cap_ratio
        self.wacc = wacc
        self.debt = debt
        self.cash = cash
        self.shares = shares


    @functools.cached_property
    def _revenue_projection(self):
        # first five year revenue projection
        first_five = [self.cur_rev*((1+self.growth_rate)**year)
                      for year in range(0, 6)]
        # last firve year revenue projection
        last_five = [first_five[-1]*(
                        (1+(self.growth_rate*self.growth_taper))**year)
                     for year in range(6, 12)]

        return first_five + last_five


    def model_eval(self):

        # grab revenues
        revenues = self._revenue_projection

        # EBIT percent over the projection years, converge to indus average
        ebit_yr_perc = []
        for year in range(1, 11):
            ebit_percent = self.indus_growth_avg - ((self.indus_growth_avg -
                                                     self.ebit_margin) /
                                                    10*(10-year))
            ebit_yr_perc.append(ebit_percent)

        # calculate EBIT every year, after tax
        # ***** need to look at this section
        ebits = [rev*(1-ebit_perc)*(1-self.tax_rate)
                 for rev, ebit_perc in zip(revenues, ebit_yr_perc)]

        # revenue diff for reinvestment calculation

        revs_diff = [y-x for x, y in zip(revenues, revenues[1:])]

        reinvestment = [rev/self.sales_cap for rev in revs_diff[1:]]
        # **** need to look at this section

        # free cash flow
        fcff = [ebit-re_inv for ebit, re_inv in zip(ebits, reinvestment)]

        # cumulative discount factor
        discount_factor = []
        for i in range(1,11):
            if i == 1:
                discount_factor.append(1/(1+self.wacc))
            discount_factor.append((1/(1+self.wacc))**i)

        # presen value
        pv = []
        for x,y in zip(fcff, discount_factor):
            pv.append(x*y)

        # terminal value
        present_terminal = fcff[-1]/(self.wacc)*discount_factor[-1]
        all_pv_terminal = present_terminal + sum(pv)

        # value of equity
        value_of_equity = all_pv_terminal - self.debt + self.cash

        return {'est_stock_price': value_of_equity/self.shares}







if __name__ == '__main__':
    raw_values = DCF_DATA('NVDA', 2).input_fileds

    dcf_2 = BuildDCF(current_rev=raw_values['Revenues'][0],
                         growth_rate=raw_values['growth_rate'],
                         ebit_margin=raw_values['oper_margin'],
                         indus_growth_avg= 0.086,
                         tax_rate=raw_values['EffectiveTax'],
                         sales_cap_ratio=raw_values['sales_to_cap'],
                         wacc=raw_values['wacc'],
                         debt=raw_values['BVOD'][0],
                         cash=raw_values['Cash'][0],
                         shares=raw_values['Shares'],
                         growth_taper=0.7).model_eval()
    print(dcf_2['est_stock_price'])
