import yfinance as yf

data = yf.download('VOO', start='2022-1-1', end='2022-4-11')
print(data)