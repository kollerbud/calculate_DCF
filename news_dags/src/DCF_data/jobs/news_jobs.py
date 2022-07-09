from dagster import job
from DCF_data.ops.news_ops import (
    company_ticker,
    gather_news,
    news_to_csv,
    news_to_bq,
)


@job
def company_news_pipeline():
    # get a list of tickers to gather news for
    tickers = company_ticker()
    # compile list of recent news
    news = gather_news(tickers)
    # download as csv
    as_csv = news_to_csv(news)
    # upload to Bigquery
    # to_bq = news_to_bq(tickers)

if __name__ == '__main__':
    None
    # company_news_pipeline.execute_in_process()