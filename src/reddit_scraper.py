'''reddit scraper'''
import os
import praw
import pandas as pd
from dotenv import load_dotenv


load_dotenv()


class GatherRedditPosts:
    '''search ticker mentions in subreddits'''

    reddit = praw.Reddit(client_id=os.getenv('reddit_client_id'),
                         client_secret=os.getenv('reddit_client_secret'),
                         user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0'
                         )
    reddit.read_only = True

    def __init__(self, ticker) -> None:
        self.ticker = str(ticker).upper()

    def search_subreddits(self,
                          subs: list = None,
                          ) -> pd.DataFrame:
        '''seaching ticker mentioned in list of subs'''
        if subs is None:
            subs = ['stockmarket', 'finance', 'stocks', 'valueinvesting']

        sub_reddits = '+'.join(subs)

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

        if len(reddit_posts) != 0:
            df_posts = pd.DataFrame.from_dict(reddit_posts)
            df_posts.sort_values(by='upvotes', ascending=False, inplace=True)
            # remove picture posts
            df_posts.dropna(subset=['textBody'], inplace=True)

            return df_posts

        return None

if __name__ == '__main__':
    print(GatherRedditPosts(ticker='amd').search_subreddits())
