from news_scraper import GatherNews
from reddit_scraper import GatherReddit
from google.cloud import bigquery
import time


def upload_news_to_bq(ticker):
    client = bigquery.Client()
    table_id = 'all_data.gather_news'
    job_config = bigquery.LoadJobConfig(
        schema=[
                bigquery.SchemaField('ticker', 'STRING'),
                bigquery.SchemaField('title', 'STRING'),
                bigquery.SchemaField('publisher', 'STRING'),
                bigquery.SchemaField('link', 'STRING'),
                bigquery.SchemaField('providerPublishTime', 'DATE')
                ]
    )
    for t in ticker:
        df_data = GatherNews(ticker=t).gather_news()
        time.sleep(1)
        job = client.load_table_from_dataframe(
            df_data, table_id, job_config=job_config)
        print(f'finish {t}')
        job.result()


def upload_reddit_to_bq(ticker):
    client = bigquery.Client()
    table_id = 'all_data.gather_reddit'
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('ticker', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('upvotes', 'INTEGER'),
            bigquery.SchemaField('link', 'STRING')
            ]
    )
    for t in ticker:
        reddit_data = GatherReddit(ticker=t).search_subreddits()
        time.sleep(1)
        job = client.load_table_from_dataframe(
                reddit_data,
                table_id,
                job_config=job_config
                 )
        job.result()
        print(f'reddit said {t}')


if __name__ == '__main__':
    # 'nvda', 'amd', 'snow', 'axp', 'gs', 'intc', 'net', 'msft'
    ticker_list = ['sq', 'net', 'amd', 'nvda', 'snow', 'axp', 'msft', 'intc', 'gs', 'abt','qcom', 'mdt']
    upload_news_to_bq(ticker=ticker_list)
    upload_reddit_to_bq(ticker=ticker_list)
