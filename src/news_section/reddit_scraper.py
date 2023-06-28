'''reddit scraper'''
import os
import praw
import pandas as pd
from datetime import datetime
from typing import List
from dotenv import load_dotenv
from google.cloud import bigquery
from goog_auth import gcp_credentials


load_dotenv()


class GatherRedditPosts:
    '''search ticker mentions in subreddits'''

    reddit = praw.Reddit(client_id=os.getenv('reddit_client_id'),
                         client_secret=os.getenv('reddit_client_secret'),
                         user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0'
                         )
    reddit.read_only = True

    df_reddit_post: pd.DataFrame = None

    def __init__(self,
                 ticker: str,
                 subs: List[str] = None) -> None:
        self.ticker = str(ticker).upper()
        self.subs = subs

    def search_subreddits(self) -> 'GatherRedditPosts':
        '''seaching ticker mentioned in list of subs'''
        if self.subs is None:
            self.subs = ['stockmarket', 'finance', 'stocks', 'valueinvesting']

        sub_reddits = '+'.join(self.subs)

        reddit_posts = []
        for post in (self.reddit.subreddit(sub_reddits).
                     search(query=self.ticker, time_filter='week')):

            reddit_posts.append({
                'ticker': self.ticker,
                'title': post.title,
                'upvotes': post.score,
                'link': post.url,
                'textBody': post.selftext,
                })

        if len(reddit_posts) > 0:
            df_posts = pd.DataFrame.from_dict(reddit_posts)
            df_posts.sort_values(by='upvotes', ascending=False, inplace=True)
            # remove picture posts
            df_posts.dropna(subset=['textBody'], inplace=True)

            self.df_reddit_post = df_posts

            return self

        else:
            # create an empty dataframe
            self.df_reddit_post = pd.DataFrame(
                columns=['ticker', 'title', 'upvotes', 'link', 'textBody']
            )

    def reddit_output(self) -> pd.DataFrame:
        'running searches'
        self.search_subreddits()

        return self.df_reddit_post

    def upload_to_bigquery(self):
        'upload news to bigquery'
        # populate search dataframe

        client = bigquery.Client(credentials=gcp_credentials())
        table_id = 'all_data.gather_reddit'
        job_configs = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('ticker', 'STRING'),
                bigquery.SchemaField('title', 'STRING'),
                bigquery.SchemaField('upvotes', 'INTEGER'),
                bigquery.SchemaField('link', 'STRING'),
                bigquery.SchemaField('textBody', 'STRING')
            ],
            autodetect=False,

        )
        df_to_upload = self.reddit_output()
        client.load_table_from_dataframe(
            df_to_upload,
            table_id,
            job_config=job_configs
        )

        return f'upload {self.ticker} to reddit table'


if __name__ == '__main__':
    print(GatherRedditPosts(ticker='nvda').upload_to_bigquery())
