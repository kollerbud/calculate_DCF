import functools


class BuildDCF:

    def __init__(self,
                 current_rev: float,
                 growth_rate: float,
                 ebit_margin: float,
                 indus_growth_avg: float,
                 tax_rate: float,
                 sales_cap_ratio: float,
                 growth_taper: float = 0.5) -> None:
        self.cur_rev = current_rev
        self.growth_rate = growth_rate
        self.growth_taper = growth_taper
        self.ebit_margin = ebit_margin
        self.indus_growth_avg = indus_growth_avg
        self.tax_rate = tax_rate
        self.sales_cap = sales_cap_ratio


    
    @functools.cached_property
    def _revenue_projection(self):
        # first five year revenue projection
        first_five = [self.cur_rev*((1+self.growth_rate)**year)
                      for year in range(0, 6)]
        # last firve year revenue projection
        last_five = [first_five[-1]*(
                        (1+self.growth_rate*self.growth_taper)**year)
                     for year in range(6, 12)]

        return first_five + last_five

    @property
    def _ebit(self):

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
        ebits = [rev*(1-ebit_perc)*(1-self.tax_rate)
                 for rev, ebit_perc in zip(revenues, ebit_yr_perc)]

        # revenue diff for reinvestment calculation
        revs_diff = [y-x for x, y in zip(revenues, revenues[1:])]
        reinvestment = [rev/self.sales_cap for rev in revs_diff[1:]]

        # free cash flow
        fcff = [ebit-re_inv for ebit, re_inv in zip(ebits, reinvestment)]

    def _free_cash_flow(self):
        pass


if __name__ == '__main__':
    print(BuildDCF(current_rev=100,
                   growth_rate=0.2,
                   ebit_margin=0.3,
                   indus_growth_avg=0.1,
                   sales_cap_ratio=2,
                   tax_rate=0.2)._ebit())