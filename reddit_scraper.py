import praw
from api_keys import REDDIT_API_KEYS
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd


class GatherReddit:

    def __init__(self, ticker) -> None:
        self.ticker = str(ticker).upper()
        self.reddit = praw.Reddit(client_id=REDDIT_API_KEYS.client_id,
                                  client_secret=REDDIT_API_KEYS.client_secret,
                                  user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
                                 )
        self.reddit.read_only = True
        self.analyzer = SentimentIntensityAnalyzer()

    def text_cleanup(self, *func):
        # clean up /n and links
        def wrapper():
            pass
        pass

    def market_subreddits(self) -> pd.DataFrame:
        sub_reddits = ('stockmarket+finance+stocks')
        reddit_posts = []
        for post in (self.reddit.subreddit(sub_reddits).
                     search(query=self.ticker, time_filter='month')):
            reddit_posts.append({'title': post.title,
                                 'upvotes': post.score
                                 #'post_content': post.selftext
                                 })
        return pd.DataFrame.from_dict(reddit_posts)


if __name__ == '__main__':
    print(GatherReddit('amd').market_subreddits().head())
'''

reddit = praw.Reddit(client_id = REDDIT_API_KEYS.client_id,
                          client_secret = REDDIT_API_KEYS.client_secret,
                          user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
                           )
reddit.read_only=True

# have to make multi-subreddit public, not sure how to use private ones

sub_reddits = ('stockmarket+finance+stocks')


for post in reddit.subreddit(sub_reddits).search(query='NVDA', time_filter='week'):
    print(post.title, post.score, post.selftext)

'''

