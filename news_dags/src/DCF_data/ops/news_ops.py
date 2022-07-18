from dagster import op
from typing import List
import pandas as pd
from DCF_data.ticker_list import get_list_of_ticker
from DCF_data.news_scraper import GatherNews
from DCF_data import upload_to_bucket
from datetime import datetime
import os


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
    today = datetime.today().date().strftime('%m-%d-%Y')
    save_to_path = os.getcwd()
    df.to_csv(f'{save_to_path}/news_{today}.csv', index=None)
    context.log.info(os.getcwd())

    return save_to_path

@op(
    description='upload to google cloud bucket'
)
def news_to_bucket(context, empty_) -> None:
    empty_ = None
    upload = upload_to_bucket.upload_news_to_bq
    upload(bucket_name='dcf_news_bucket')