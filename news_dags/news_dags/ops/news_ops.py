from dagster import op
from typing import List
import pandas as pd
from news_dags.DCF_data.ticker_list import get_list_of_ticker
from news_dags.DCF_data.news_scraper import GatherNews
from news_dags.DCF_data import upload_to_bq


@op(
    description='get a list of company tickers'
)
def company_ticker(context) -> List:

    ticker_list = get_list_of_ticker()
    context.log.info(f'companies in list: {ticker_list}')

    return ticker_list


@op(
    description='gather news from list of companies'
)
def gather_news(context, ticker_list: List) -> pd.DataFrame:
    df_news = pd.DataFrame()
    for ticker in ticker_list:
        context.log.info(f'tickers news pulled {ticker}')
        df_gather = GatherNews(ticker=ticker).gather_news()
        df_news = pd.concat([df_news, df_gather])     

    return df_news   


@op(
    description='gather analysts targets from list of companies'
)
def gather_analyst_targets(context):
    return None


@op(
    description='download news to a csv file'
)
def news_to_csv(context, df: pd.DataFrame) -> None:
    df.to_csv('news.csv', index=None)

@op(
    description='upload to Bigquery'
)
def news_to_bq(context, tickers: list[str]) -> None:
    upload_to_bq.upload_news_to_bq(ticker=tickers)