'''news scraper'''
import multiprocessing as mp
import os
import uuid
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from finvizfinance.quote import finvizfinance
from google.cloud import bigquery
from news_text import ExtractYahooNewsText
from goog_auth import gcp_credentials


class GatherYahooNews:
    '''query news from yfinance of a ticker'''

    yahoo_news: pd.DataFrame = None

    def __init__(self,
                 ticker: str) -> None:
        self.ticker = str(ticker).upper()

    def __repr__(self) -> str:
        return repr(self.yahoo_news)

    def _query_yahoo_api(self) -> 'GatherYahooNews':
        '''
        gather news from yahoo and
        run Vader sentimental analysis on news title
        '''
        # yahoo news only have 8 news
        yahoo_news = []
        for news in yf.Ticker(self.ticker).news:
            news.update({'ticker': self.ticker})
            news.pop('type')
            news.pop('relatedTickers')
            try:
                news.pop('thumbnail')
            except KeyError:
                continue
            news['providerPublishTime'] = (
                datetime.utcfromtimestamp(
                    news['providerPublishTime']).strftime('%Y-%m-%d')
                )
            yahoo_news.append(news)

        self.yahoo_news = pd.DataFrame(yahoo_news)

        return self

    def _query_news_text(self) -> 'GatherYahooNews':

        '''use links to extract yahoo news texts body'''
        # check yahoo_news is not none
        if self.yahoo_news is None:
            raise ValueError('"yahoo news is currently none"')
        # get a list of links from yahoo
        links_list = self.yahoo_news['link'].tolist()
        # run concurrent processes to speed up news
        with mp.Pool(processes=os.cpu_count()) as pool:
            self.yahoo_news['newsBody'] = (
                pool.map(ExtractYahooNewsText().extract_text, links_list))

        return self

    def news_api_output(self) -> pd.DataFrame:
        'run the news gathering pipeline'
        self._query_yahoo_api()
        self._query_news_text()

        return self.yahoo_news

    def upload_to_bigquery(self):
        'upload news to bigquery'
        client = bigquery.Client(credentials=gcp_credentials())
        table_id = 'all_data.gatherNews'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('uuid', 'STRING'),
                bigquery.SchemaField('title', 'STRING'),
                bigquery.SchemaField('publisher', 'STRING'),
                bigquery.SchemaField('link', 'STRING'),
                bigquery.SchemaField('providerPublishTime', 'DATE'),
                bigquery.SchemaField('newsBody', 'STRING'),
                bigquery.SchemaField('ticker', 'STRING'),
            ],
            autodetect=False,
            source_format=bigquery.SourceFormat.CSV
        )
        df_to_upload = self.news_api_output()
        client.load_table_from_dataframe(
            df_to_upload,
            table_id,
            job_config=job_configs
        )

        return f'upload {self.ticker} to news table'


class GatherFinvizNews:
    '''get news from finviz api
    https://github.com/lit26/finvizfinance
    it is getting other sources from yahoo news
    so text content is limited
    '''
    finviz_news: pd.DataFrame = None

    def __init__(self,
                 ticker: str) -> None:
        self.ticker = str(ticker).upper()

    def __repr__(self) -> str:
        return repr(self.finviz_news)

    def _query_finviz_api(self,
                          back_days: int = 7) -> 'GatherFinvizNews':
        '''query custom finviz api'''

        df_news = finvizfinance(ticker=self.ticker).ticker_news()
        # select days of news to capture
        current_date = datetime.now().date()
        n_days_ago = current_date - timedelta(days=back_days)
        df_news['Date'] = df_news['Date'].dt.date
        df_news = df_news[df_news['Date'] >= n_days_ago]
        # remove unusable links
        df_news = df_news[
            df_news['Link'].str.startswith('https://finance.yahoo.com/news/')
            ]
        df_news.reset_index(inplace=True, drop=True)
        # ticker
        df_news['ticker'] = self.ticker
        # date
        df_news.rename(columns={'Date': 'providerPublishTime'}, inplace=True)
        # drop duplicates
        df_news.drop_duplicates(subset=['Link'], inplace=True)
        # add uuid
        df_news['uuid'] = [uuid.uuid4() for _ in range(len(df_news.index))]

        # populate the class variable
        self.finviz_news = df_news
        return self

    def _query_news_text(self) -> 'GatherFinvizNews':

        '''use links to extract yahoo news texts body'''
        # check finviz_news is not none
        if self.finviz_news is None:
            raise ValueError('"yahoo news is currently none"')

        links_list = self.finviz_news['Link'].tolist()
        # run concurrent processes to speed up news
        with mp.Pool(processes=os.cpu_count()) as pool:
            self.finviz_news['newsBody'] = (
                pool.map(ExtractYahooNewsText().extract_text, links_list))

        return self

    def news_api_output(self) -> pd.DataFrame:
        'run the news gathering pipeline'
        self._query_finviz_api()
        self._query_news_text()

        return self.finviz_news

    def upload_to_bigquery(self):
        'upload news to bigquery'
        client = bigquery.Client(credentials=gcp_credentials())
        table_id = 'all_data.gatherNews'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('uuid', 'STRING'),
                bigquery.SchemaField('Title', 'STRING'),
                #bigquery.SchemaField('publisher', 'STRING'),
                bigquery.SchemaField('Link', 'STRING'),
                bigquery.SchemaField('providerPublishTime', 'DATE'),
                bigquery.SchemaField('newsBody', 'STRING'),
                bigquery.SchemaField('ticker', 'STRING'),
            ],
            autodetect=False,
            source_format=bigquery.SourceFormat.CSV

        )
        df_to_upload = self.news_api_output()
        client.load_table_from_dataframe(
            df_to_upload,
            table_id,
            job_config=job_configs
        )

        return f'upload {self.ticker} to news table'


if __name__ == '__main__':
    x = GatherFinvizNews(ticker='tsla')
    print(x.news_api_output())
