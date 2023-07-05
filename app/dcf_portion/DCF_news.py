'''Query news tables'''
#import sys
#sys.path += ['../app/dcf_portion/']
import functools
import pandas as pd
from typing import Type, List
from google.cloud import bigquery
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from goog_auth import gcp_credentials


class GetNewsAndTitleSentiment:
    '''
    get news from bigquery and analyze sentiment based on
    news title
    '''

    def __init__(self,
                 ticker: str,
                 analyzer=SentimentIntensityAnalyzer(),
                 news_age: int = 1,
                 ) -> None:

        self.ticker = str(ticker).upper()
        self.news_age = int(news_age)
        self.analyzer = analyzer
        self.client = bigquery.Client(credentials=gcp_credentials())

    @functools.cached_property
    def _query_news(self) -> List[dict[str]]:
        query_string = '''
            SELECT *
            FROM all_data.gatherNews
            WHERE
                ticker = @_ticker
                AND providerPublishTime >= DATE_SUB(CURRENT_DATE(), INTERVAL @_week WEEK)
            ORDER BY providerPublishTime DESC
            ;
        '''
        job_configs = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('_ticker', 'STRING', self.ticker),
                bigquery.ScalarQueryParameter('_week', 'INTEGER', self.news_age),
            ]
        )
        # run query
        query_job = self.client.query(query=query_string,
                                      job_config=job_configs)
        query_job.result()
        print('ran news query')
        query_results = []
        for row in query_job:
            row_dict = {
                'ticker': row['ticker'],
                'title': row['title'],
                'link': row['link'],
                'publishTime': row['providerPublishTime'],
            }
            query_results.append(row_dict)

        return query_results

    def sentiment_analysis(self) -> pd.DataFrame:
        df_news = pd.DataFrame(self._query_news, index=None)

        df_news['sentiment'] = (df_news['title']
                                .apply(self.analyzer.polarity_scores)
                                )

        # extract sentiment scores
        df_news['sentiment_comp'] = df_news['sentiment'].str.get('compound')
        df_news['sentiment_pos'] = df_news['sentiment'].str.get('pos')
        df_news['sentiment_neu'] = df_news['sentiment'].str.get('neu')
        df_news['sentiment_neg'] = df_news['sentiment'].str.get('neg')

        return df_news


if __name__ == '__main__':
    print(GetNewsAndTitleSentiment(
            ticker='tsla'
            )._sentiment_analysis())
