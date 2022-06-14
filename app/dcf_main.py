import streamlit as st
import pandas as pd
import pandas_datareader.data as web


st.write('''
# my first app
hello *world!*
''')

start = '2022-01-1'
end = '2022-06-7'


df_stock = web.DataReader('nvda', 'yahoo', start, end)
df_stock = df_stock.filter(regex='Adj Close')


df_sp500 = web.DataReader('VOO', 'yahoo', start, end)
df_sp500 = df_sp500.filter(regex='Adj Close')

st.line_chart(data=df_stock)
st.line_chart(data=df_sp500)