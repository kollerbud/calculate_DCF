import praw
from DCF_data.api_keys import REDDIT_API_KEYS
import pandas as pd


class GatherReddit:

    reddit = praw.Reddit(client_id=REDDIT_API_KEYS.client_id,
                         client_secret=REDDIT_API_KEYS.client_secret,
                         user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
                         )
    reddit.read_only = True

    def __init__(self, ticker) -> None:
        self.ticker = str(ticker).upper()

    def search_subreddits(self, section='normal') -> pd.DataFrame:

        if section not in ['normal', 'meme']:
            raise ValueError('use "normal" or "meme"')

        if section == 'normal':
            sub_reddits = ('stockmarket+finance+stocks')
        if section == 'meme':
            sub_reddits = ('wallstreetbets')

        reddit_posts = []
        for post in (self.reddit.subreddit(sub_reddits).
                     search(query=self.ticker, time_filter='week')):

            reddit_posts.append({
                                 'ticker': self.ticker,
                                 'title': post.title,
                                 'upvotes': post.score,
                                 'link': post.url
                                 })

        if len(reddit_posts) != 0:
            df_posts = pd.DataFrame.from_dict(reddit_posts)
            df_posts.sort_values(by='upvotes', ascending=False, inplace=True)

            return df_posts

        else:
            return None

if __name__ == '__main__':
    print(GatherReddit('nvda').search_subreddits())