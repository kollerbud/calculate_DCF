from __future__ import absolute_import
from dagster import job
from news_dags.news_dags.ops.news_ops import (
    company_ticker,
    gather_news,
    news_to_csv,
)


@job
def company_news_pipeline():
    # get a list of tickers to gather news for
    tickers = company_ticker()
    # compile list of recent news
    news = gather_news(tickers)
    # download as csv
    as_csv = news_to_csv(news)


if __name__ == '__main__':
    company_news_pipeline.execute_in_process()