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

    def search_subreddits(self, section='normal') -> pd.DataFrame:

        if section not in ['normal', 'meme']:
            raise ValueError('use "normal" or "meme"')

        if section == 'normal':
            sub_reddits = ('stockmarket+finance+stocks')
        if section == 'meme':
            sub_reddits = ('wallstreetbets')

        reddit_posts = []
        for post in (self.reddit.subreddit(sub_reddits).
                     search(query=self.ticker, time_filter='month')):
                    
            reddit_posts.append({'title': post.title,
                                 'upvotes': post.score,
                                 'link': post.url
                                 #'post_content': post.selftext
                                 })
        if len(reddit_posts) != 0:
            df_posts = pd.DataFrame.from_dict(reddit_posts)
            df_posts.sort_values(by='upvotes', ascending=False, inplace=True)
            
            return df_posts
        
        else:
            return None

if __name__ == '__main__':
    print(GatherReddit('amd').search_subreddits())