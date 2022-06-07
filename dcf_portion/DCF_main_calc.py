from DCF_prelim_calc import DcfPrelim
from dataclasses import dataclass

@dataclass
class BuildDCF:
    cur_rev: float
    growth_rate: float
    ebit_margin: float
    indus_growth_avg: float
    tax_rate: float
    sales_cap: float
    wacc: float
    debt: float
    cash: float
    shares: float
    int_rate: float = None
    growth_taper: float = 0.85

    @property
    def _revenue_projection(self) -> list:
        # first five year revenue projection
        first_five = [self.cur_rev*((1+self.growth_rate)**year)
                      for year in range(1, 6)]

        # last firve year revenue projection
        last_five = [self.cur_rev*((
            1+(self.growth_rate*self.growth_taper))**year)
                     for year in range(6, 11)]

        return first_five + last_five

    def _ebit(self, termin_perc: float = None) -> list:
        # make sure list of revenue projection is the input
        # termin_perc = last 5 years of ebit % of revenue
        #  default value same as current ebit %
        rev = self._revenue_projection


    def disount_rate(self, int_rate=0, method='wacc'):

        if method == 'wacc':
            self.int_rate = self.wacc
            return self.int_rate

        if method == 'dcf':
            self.int_rate = int_rate
            return self.int_rate

    def prof_model_eval(self, growth_shift):

        # grab revenues
        revenues = self._revenue_projection(growth_shift=growth_shift)

        # EBIT percent over the projection years, converge to indus average
        ebit_yr_perc = []
        for year in range(1, 11):
            ebit_percent = self.indus_growth_avg - ((self.indus_growth_avg -
                                                     self.ebit_margin) /
                                                    10*(10-year))
            ebit_yr_perc.append(ebit_percent)

        # calculate EBIT every year, after tax
        ebits = [rev*(1-ebit_perc)*(1-self.tax_rate)
                 for rev, ebit_perc in zip(revenues, ebit_yr_perc)]
        # revenue diff for reinvestment calculation
        revs_diff = [y-x for x, y in zip(revenues, revenues[1:])]
        reinvestment = [rev/self.sales_cap for rev in revs_diff]
        reinvestment.append(reinvestment[-1])

        # free cash flow
        fcff = [ebit-re_inv for ebit, re_inv in zip(ebits, reinvestment)]

        # cumulative discount factor
        int_rate = self.disount_rate(method='wacc')
        discount_factor = []
        for i in range(1, 10):
            if i == 1:
                discount_factor.append(1/(1+int_rate))
            discount_factor.append((1/(1+int_rate))**i)

        # presen value
        pv = [x*y for x, y in zip(fcff, discount_factor)]
        print(pv)
        # terminal value
        'check present value calculation for terminal value'
        present_terminal = fcff[-1]/(self.wacc)*discount_factor[-1]

        all_pv_terminal = present_terminal + sum(pv)
        # value of equity
        value_of_equity = all_pv_terminal - self.debt + self.cash

        return {'est_stock_price': value_of_equity/self.shares}

    def dcf_model_eval(self, growth_taper):
        rev = self._revenue_projection(growth_taper=growth_taper)
        return None


if __name__ == '__main__':
    raw_values = DCF_DATA('amd').input_fileds()
    print(raw_values)


    dcf_2 = BuildDCF(cur_rev=sum(raw_values['Revenues'])/2,
                     growth_rate=raw_values['growth_rate'],
                     ebit_margin=raw_values['oper_margin'],
                     indus_growth_avg=0.20,
                     tax_rate=raw_values['EffectiveTax'],
                     sales_cap=raw_values['sales_to_cap'],
                     wacc=raw_values['wacc'],
                     debt=raw_values['BVOD'][0],
                     cash=raw_values['Cash'][0],
                     shares=raw_values['Shares'])._revenue_projection
    print(dcf_2)
    