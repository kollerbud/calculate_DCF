import streamlit as st
import pandas as pd
from dcf_portion.DCF_avail_ticker import available_ticker
from datetime import datetime

st.title(
    '''
    Build a quick DCF Model, for FUN
    '''

)

avail_tickers = available_ticker()
_ticker = st.selectbox(
    label='select ticker to calculate dcf',
    options=avail_tickers
)
start = '2022-01-1'
end = datetime.now().date()

risk_free = st.text_input(
    label='enter risk free rate',
    value=0.0261
)
market_prem = st.text_input(
    label='enter market risk premium',
    value=0.0523
)
avg_debt_int = st.text_input(
    label='enter company average debt interest rate',
    value=0.03
)
wacc_override = st.text_input(
    'override wacc calculation with manual wacc?',
    value='No'
)
risk_free = float(risk_free)
market_prem = float(market_prem)
avg_debt_int = float(avg_debt_int)
if wacc_override != 'No':
    wacc_override = float(wacc_override)



if st.button('calculate'):
    data = BuildDCF(ticker=_ticker,
                    risk_free_rate=risk_free,
                    market_risk_prem=market_prem,
                    avg_debt_int=avg_debt_int)
    df_data = pd.DataFrame.from_dict(data.dcf_output())
    pred_price = data.freeCashFlow(override_wacc=wacc_override)['pred_price']
    show_wacc = data.freeCashFlow(override_wacc=wacc_override)['wacc_used']

    st.write(df_data)
    st.write(pred_price)
    st.write(show_wacc)




'''
One way to do it is to put all your heavy calculations behind an st.button().

if st.button('button'):
   ... heavy calculations ...


'''