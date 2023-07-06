'Streamlit script'
import streamlit as st
import pandas as pd
from dcf_portion.DCF_avail_ticker import available_ticker
from dcf_portion.DCF_calc import BuildDiscountCashFlowModel
from dcf_portion.DCF_news import GetNewsAndTitleSentiment



st.title(
    '''
    Build a quick DCF Model, for FUN
    '''

)
model_ran = False

with st.sidebar:
    avail_tickers = available_ticker()
    _ticker = st.selectbox(
        label='select ticker to calculate dcf',
        options=avail_tickers
    )

    risk_free = st.text_input(
        label='enter risk free rate',
        value=0.0381
    )
    years_used = st.number_input(
        label='years of financial statments to use',
        value=4,
        min_value=2,
        max_value=10
    )
    news_age = st.number_input(
        label='weeks of news',
        value=1,
        min_value=1,
    )

    risk_free = float(risk_free)
    years_used = float(years_used)

    if st.button('run model'):
        model = BuildDiscountCashFlowModel(
                    ticker=_ticker,
                    years_statement=years_used,
                    risk_free_rate=risk_free)
        model_ran = True

        _news = GetNewsAndTitleSentiment(ticker=_ticker,
                                        news_age=news_age)

if model_ran:
    col_1, col_2 = st.columns([3,2])
    wacc_price_data = pd.DataFrame(model.wacc_fcf_curve())
    with col_1:
        st.line_chart(
            data=wacc_price_data,
            x='wacc',
            y='pred_price'
            )
    with col_2:
        st.dataframe(wacc_price_data)

    st.dataframe(_news.sentiment_analysis())